use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wxcd_codex::{CodexClient, CodexEvent, CodexThreadSummary};
use wxcd_eventlog::{EventLog, ReplayState};
use wxcd_proto::{
    AppConfig, ApprovalDecision, ApprovalKind, BridgeEvent, LocalSessionMirror, PendingApproval,
    SessionAuthority, SessionFailure, SessionFailureKind, SessionRecord, SessionState,
    WebexAttachmentActionEvent, WebexIngressAck, WebexIngressEnvelope, WebexMessageEvent,
    generate_session_id,
};
use wxcd_render::{
    ImportedHistoryTurn, LocalThreadListItem, build_approval_attachment, build_overview_attachment,
    render_cleanup_failed_preview, render_control_list, render_failed_session_diagnostics,
    render_final_summary, render_help, render_history_page, render_imported_history,
    render_local_thread_list, render_purge_archived_warning, render_status_summary,
};
use wxcd_webex::{CreateMessageRequest, EnsureMembership, UpdateMessageRequest, WebexClient};

const LOCAL_THREAD_PAGE_SIZE: usize = 20;
const HISTORY_PAGE_SIZE: usize = 10;
const IMPORTED_HISTORY_TURN_LIMIT: usize = HISTORY_PAGE_SIZE;
const RECENT_EVENT_ID_LIMIT: usize = 1024;
const INSTALLATION_IDENTITY_FILE: &str = "installation-identity.json";

#[derive(Parser)]
struct Args {}

#[derive(Default)]
struct WorkerState {
    sessions: HashMap<String, SessionRecord>,
    room_to_session: HashMap<String, String>,
    thread_to_session: HashMap<String, String>,
    pending_approvals: HashMap<String, PendingApproval>,
    recent_event_ids: HashSet<String>,
    recent_event_queue: VecDeque<String>,
    events_since_snapshot: usize,
    executable_installation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListMode {
    Bridge,
    Local,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ListCommand {
    mode: ListMode,
    page: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiagnoseCommand {
    Sessions,
    Session(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CleanupFailedCommand {
    Preview,
    Session(String),
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PurgeArchivedCommand {
    session_id: String,
    confirmed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThreadProbeKind {
    Readable,
    MissingThread,
    UnreadableThread,
    ProbeUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThreadProbe {
    kind: ThreadProbeKind,
    message: String,
}

struct LocalOnlyThreads {
    items: Vec<LocalThreadListItem>,
    total_count: usize,
    has_more: bool,
    page: usize,
    page_size: usize,
}

struct ImportedThreadHistory {
    turns: Vec<ImportedHistoryTurn>,
    total_turns: usize,
}

struct CreateBridgeSessionInput<'a> {
    owner_email: &'a str,
    repo_name: &'a str,
    repo_path: &'a str,
    thread_id: &'a str,
    checkpoint: &'a str,
    installation_id: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InstallationIdentity {
    installation_id: String,
    created_at: chrono::DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    run().await
}

async fn run() -> Result<()> {
    let mut config = AppConfig::load()?;
    tokio::fs::create_dir_all(&config.bridge.state_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create state dir {}",
                config.bridge.state_dir.display()
            )
        })?;
    let installation = load_or_create_installation_identity(&config.bridge.state_dir).await?;

    remove_stale_socket(&config.bridge.socket_path).await?;

    let webex = WebexClient::new(&config.webex.bot_token)?;
    if config.webex.bot_display_name.is_none() {
        match webex.get_me().await {
            Ok(person) => {
                config.webex.bot_display_name = person.display_name;
            }
            Err(error) => {
                warn!("failed to resolve Webex bot display name: {error:#}");
            }
        }
    }
    let control_room = webex
        .resolve_room_reference(&config.webex.control_room_ref)
        .await
        .context("failed to resolve control room reference")?;
    let data_room = webex
        .resolve_room_reference(&config.webex.data_room_ref)
        .await
        .context("failed to resolve data room reference")?;

    let mut codex = CodexClient::spawn().await?;
    codex
        .initialize("wxcd-worker", true)
        .await
        .context("failed to initialize codex app-server")?;

    let event_log = EventLog::new(&webex, &data_room.id);
    let local_snapshot_path = config.bridge.state_dir.join("bridge-state.json");
    let remote_replay = match event_log.replay().await {
        Ok(replay) => Some(replay),
        Err(error) => {
            warn!("failed to replay Data Space, falling back to local snapshot: {error:#}");
            None
        }
    };
    let replayed_data_space = remote_replay.is_some();
    let replay = match remote_replay {
        Some(replay) => replay,
        None => load_local_snapshot(&local_snapshot_path).await?,
    };
    let mut state = WorkerState::from_replay(replay);
    state.set_executable_installation(&installation.installation_id);
    if replayed_data_space {
        match load_local_snapshot(&local_snapshot_path).await {
            Ok(local_replay) => {
                let changed = state.merge_local_mirror(
                    local_replay,
                    &installation.installation_id,
                    Utc::now(),
                );
                if changed {
                    persist_local_snapshot(
                        &local_snapshot_path,
                        &state.to_replay_state().to_snapshot(),
                    )
                    .await?;
                }
            }
            Err(error) => {
                warn!(
                    "failed to read local mirror snapshot after Data Space replay succeeded: {error:#}"
                );
            }
        }
    } else {
        state.rebuild_session_indexes();
    }
    reconcile_sessions(
        &config,
        &webex,
        &event_log,
        &mut state,
        &mut codex,
        &installation,
    )
    .await?;

    let healthy = Arc::new(AtomicBool::new(false));
    let (webex_tx, mut webex_rx) = mpsc::channel(256);
    let listener = UnixListener::bind(&config.bridge.socket_path).with_context(|| {
        format!(
            "failed to bind unix socket {}",
            config.bridge.socket_path.display()
        )
    })?;
    tokio::spawn(run_ingress_server(listener, webex_tx, Arc::clone(&healthy)));
    healthy.store(true, Ordering::Relaxed);
    info!("wxcd worker is healthy");

    loop {
        tokio::select! {
            maybe_ingress = webex_rx.recv() => {
                let Some(event) = maybe_ingress else {
                    break;
                };
                if let Err(error) = handle_webex_ingress(
                    &config,
                    &webex,
                    &event_log,
                    &mut state,
                    &mut codex,
                    &control_room.id,
                    &installation,
                    event,
                ).await {
                    error!("failed to handle Webex ingress: {error:#}");
                }
            }
            maybe_codex = codex.events().recv() => {
                let Some(event) = maybe_codex else {
                    break;
                };
                if let Err(error) = handle_codex_event(
                    &config,
                    &webex,
                    &event_log,
                    &mut state,
                    &mut codex,
                    event,
                ).await {
                    error!("failed to handle Codex event: {error:#}");
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                info!("received ctrl-c, shutting down worker");
                break;
            }
        }
    }

    healthy.store(false, Ordering::Relaxed);
    codex.shutdown().await?;
    remove_stale_socket(&config.bridge.socket_path).await?;
    Ok(())
}

async fn handle_webex_ingress(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    control_room_id: &str,
    installation: &InstallationIdentity,
    event: WebexIngressEnvelope,
) -> Result<()> {
    match event {
        WebexIngressEnvelope::MessageCreated(message) => {
            if is_ignored_sender(config, &message.person_email)
                || !state.remember_event(&message.event_id)
            {
                return Ok(());
            }
            if message.room_id == control_room_id {
                handle_control_message(
                    config,
                    webex,
                    event_log,
                    state,
                    codex,
                    installation,
                    &message,
                )
                .await?;
            } else if let Some(session_id) = state.room_to_session.get(&message.room_id).cloned() {
                handle_session_message(
                    config,
                    webex,
                    event_log,
                    state,
                    codex,
                    &session_id,
                    &message.text,
                )
                .await?;
            }
        }
        WebexIngressEnvelope::AttachmentActionCreated(action) => {
            if is_ignored_sender(config, &action.person_email)
                || !state.remember_event(&action.event_id)
            {
                return Ok(());
            }
            handle_attachment_action(config, webex, event_log, state, codex, installation, action)
                .await?;
        }
        WebexIngressEnvelope::HealthCheck => {}
    }

    Ok(())
}

async fn handle_control_message(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    installation: &InstallationIdentity,
    message: &WebexMessageEvent,
) -> Result<()> {
    let text = normalize_control_command_text(
        message.text.trim(),
        &config.webex.bot_email,
        config.webex.bot_display_name.as_deref(),
    );
    let owner_email = message.person_email.to_ascii_lowercase();
    if matches_help_command(text) {
        send_plain_message(webex, &message.room_id, render_help()).await?;
        return Ok(());
    }
    if let Some(list_command) = parse_list_command(text) {
        let mut sessions = sessions_for_control_list(state, &installation.installation_id);
        sessions.sort_by_key(|session| session.updated_at);
        sessions.reverse();
        let rendered = match list_command.mode {
            ListMode::Bridge => render_control_list(&sessions),
            ListMode::Local => {
                let local_threads =
                    collect_local_only_threads(codex, state, list_command.page).await?;
                render_local_thread_list(
                    &local_threads.items,
                    local_threads.total_count,
                    local_threads.has_more,
                    local_threads.page,
                    local_threads.page_size,
                )
            }
            ListMode::All => {
                let local_threads =
                    collect_local_only_threads(codex, state, list_command.page).await?;
                format!(
                    "{}\n\n{}",
                    render_control_list(&sessions),
                    render_local_thread_list(
                        &local_threads.items,
                        local_threads.total_count,
                        local_threads.has_more,
                        local_threads.page,
                        local_threads.page_size,
                    )
                )
            }
        };
        send_plain_message(webex, &message.room_id, &rendered).await?;
        return Ok(());
    }
    if let Some(thread_id) = parse_resume_local_thread_id(text) {
        let local_thread = find_local_only_thread(codex, state, thread_id).await?;
        let resumed = codex
            .thread_resume(thread_id)
            .await
            .with_context(|| format!("failed to resume local Codex thread `{thread_id}`"))?;
        let imported_history = match read_thread_history(codex, thread_id).await {
            Ok(turns) => latest_thread_history_summary(&turns, IMPORTED_HISTORY_TURN_LIMIT),
            Err(error) => {
                warn!(
                    "failed to read local Codex history for thread {}: {error:#}",
                    thread_id
                );
                ImportedThreadHistory {
                    turns: Vec::new(),
                    total_turns: 0,
                }
            }
        };
        let cwd = local_thread
            .cwd
            .clone()
            .or_else(|| {
                resumed
                    .pointer("/thread/cwd")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .ok_or_else(|| anyhow!("local Codex thread `{thread_id}` is missing cwd"))?;
        let repo_name = repo_name_for_cwd(config, &cwd);
        let checkpoint = format!("Attached local Codex thread `{thread_id}`.");
        let session = match create_bridge_session(
            config,
            webex,
            event_log,
            state,
            CreateBridgeSessionInput {
                owner_email: &owner_email,
                repo_name: &repo_name,
                repo_path: &cwd,
                thread_id,
                checkpoint: &checkpoint,
                installation_id: &installation.installation_id,
            },
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                if let Err(archive_error) = codex.thread_archive(thread_id).await {
                    warn!(
                        "failed to archive unbound Codex thread {} after session creation failed: {archive_error:#}",
                        thread_id
                    );
                }
                send_plain_message(
                    webex,
                    &message.room_id,
                    &format!("Failed to create session room or add `{owner_email}`: {error:#}"),
                )
                .await
                .ok();
                return Err(error);
            }
        };
        if let Err(error) = import_local_thread_history(
            webex,
            &session.session_room_id,
            thread_id,
            &imported_history,
        )
        .await
        {
            warn!(
                "failed to import local Codex history for thread {} into session {}: {error:#}",
                thread_id, session.session_id
            );
        }

        send_plain_message(
            webex,
            &message.room_id,
            &format!(
                "Attached local thread `{thread_id}` as session `{}` in room `{}`.",
                session.session_id, session.title
            ),
        )
        .await?;
        return Ok(());
    }
    if let Some(session_id) = parse_attach_session_id(text) {
        let session = state
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
        ensure_session_belongs_to_installation(&session, &installation.installation_id)?;
        let attached = webex
            .ensure_membership(&session.session_room_id, &owner_email)
            .await?;
        let status_line = match attached {
            EnsureMembership::Created => {
                format!(
                    "Attached `{owner_email}` to session `{session_id}` in room `{}`.",
                    session.title
                )
            }
            EnsureMembership::AlreadyPresent => {
                format!(
                    "`{owner_email}` is already in session `{session_id}` room `{}`.",
                    session.title
                )
            }
        };
        let response = if let Some(web_link) = session.session_room_web_link.as_deref() {
            format!("{status_line}\nOpen: {web_link}")
        } else {
            status_line
        };
        send_plain_message(webex, &message.room_id, &response).await?;
        return Ok(());
    }
    if let Some(command) = parse_diagnose_command(text) {
        let response = handle_diagnose_command(
            config,
            webex,
            event_log,
            state,
            codex,
            installation,
            command,
        )
        .await?;
        send_plain_message(webex, &message.room_id, &response).await?;
        return Ok(());
    }
    if let Some(command) = parse_cleanup_failed_command(text) {
        let response = handle_cleanup_failed_command(
            config,
            webex,
            event_log,
            state,
            codex,
            installation,
            command,
        )
        .await?;
        send_plain_message(webex, &message.room_id, &response).await?;
        return Ok(());
    }
    if let Some(command) = parse_purge_archived_command(text) {
        let response =
            handle_purge_archived_command(config, webex, event_log, state, installation, command)
                .await?;
        send_plain_message(webex, &message.room_id, &response).await?;
        return Ok(());
    }
    if let Some(session_id) = text
        .strip_prefix("archive ")
        .or_else(|| text.strip_prefix("/archive "))
        .map(str::trim)
    {
        let session = state
            .sessions
            .get(session_id)
            .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
        ensure_session_belongs_to_installation(session, &installation.installation_id)?;
        archive_session(config, webex, event_log, state, codex, session_id).await?;
        send_plain_message(
            webex,
            &message.room_id,
            &format!("Archived session `{session_id}`."),
        )
        .await?;
        return Ok(());
    }

    let Some(rest) = text
        .strip_prefix("new ")
        .or_else(|| text.strip_prefix("/new "))
    else {
        send_plain_message(webex, &message.room_id, render_help()).await?;
        return Ok(());
    };
    let Some((repo_name, task)) = rest.split_once("::") else {
        bail!("expected `new <repo> :: <task>`");
    };
    let repo_name = repo_name.trim();
    let task = task.trim();
    let repo = config
        .repo_by_name(repo_name)
        .ok_or_else(|| anyhow!("unknown repo `{repo_name}`"))?;

    let cwd = repo
        .path
        .to_str()
        .ok_or_else(|| anyhow!("repo path is not valid UTF-8"))?;
    let thread = codex
        .thread_start(
            cwd,
            &config.bridge.approval_policy,
            &config.bridge.sandbox_mode,
            &config.bridge.developer_instructions,
        )
        .await?;
    let thread_id = json_str(&thread, "/thread/id")?;
    let session = match create_bridge_session(
        config,
        webex,
        event_log,
        state,
        CreateBridgeSessionInput {
            owner_email: &owner_email,
            repo_name: &repo.name,
            repo_path: cwd,
            thread_id,
            checkpoint: "Session created.",
            installation_id: &installation.installation_id,
        },
    )
    .await
    {
        Ok(session) => session,
        Err(error) => {
            send_plain_message(
                webex,
                &message.room_id,
                &format!("Failed to create session room or add `{owner_email}`: {error:#}"),
            )
            .await
            .ok();
            return Err(error);
        }
    };

    if let Err(error) = send_plain_message(
        webex,
        &message.room_id,
        &format!(
            "Created session `{}` in room `{}`.",
            session.session_id, session.title
        ),
    )
    .await
    {
        warn!(
            "failed to send control-room confirmation for session {}: {error:#}",
            session.session_id
        );
    }

    if !task.is_empty() {
        handle_session_message(
            config,
            webex,
            event_log,
            state,
            codex,
            &session.session_id,
            task,
        )
        .await?;
    }

    Ok(())
}

async fn create_bridge_session(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    input: CreateBridgeSessionInput<'_>,
) -> Result<SessionRecord> {
    let session_id = generate_session_id(Utc::now());
    let title = format!(
        "{} {} {}",
        config.bridge.session_title_prefix, session_id, input.repo_name
    );
    let room = webex.create_room(&title).await?;
    if !input
        .owner_email
        .eq_ignore_ascii_case(&config.webex.bot_email)
        && let Err(error) = webex
            .ensure_membership(&room.id, input.owner_email)
            .await
            .with_context(|| {
                format!(
                    "failed to add `{}` to Webex session room `{}`",
                    input.owner_email, room.id
                )
            })
    {
        if let Err(cleanup_error) = webex.delete_room(&room.id).await {
            warn!(
                "failed to delete Webex room {} after membership failure: {cleanup_error:#}",
                room.id
            );
        }
        return Err(error);
    }

    let mut session = SessionRecord {
        session_id,
        title,
        repo_name: input.repo_name.to_string(),
        repo_path: input.repo_path.to_string(),
        owner_email: input.owner_email.to_string(),
        session_room_id: room.id.clone(),
        session_room_web_link: room.web_link.clone(),
        thread_id: input.thread_id.to_string(),
        overview_message_id: None,
        state: SessionState::Creating,
        last_checkpoint: Some(input.checkpoint.to_string()),
        last_final: None,
        active_turn_id: None,
        active_turn_buffer: String::new(),
        updated_at: Utc::now(),
        archived: false,
        failure: None,
        authority: Some(SessionAuthority {
            installation_id: input.installation_id.to_string(),
        }),
        local_mirror: Some(LocalSessionMirror {
            installation_id: input.installation_id.to_string(),
            mirrored_at: Utc::now(),
        }),
    };
    state.upsert_session(session.clone());
    persist_event(
        event_log,
        state,
        BridgeEvent::SessionCreated {
            session: session.clone(),
        },
        config,
    )
    .await?;

    let overview = webex
        .create_message(&CreateMessageRequest {
            room_id: session.session_room_id.clone(),
            text: Some(render_status_summary(&session)),
            markdown: None,
            attachments: Some(vec![build_overview_attachment(&session)]),
        })
        .await?;
    session.overview_message_id = Some(overview.id.clone());
    session.state = SessionState::Idle;
    session.updated_at = Utc::now();
    state.upsert_session(session.clone());
    persist_event(
        event_log,
        state,
        BridgeEvent::SessionUpdated {
            session: session.clone(),
        },
        config,
    )
    .await?;
    refresh_overview(webex, &session).await?;
    Ok(session)
}

async fn handle_session_message(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    session_id: &str,
    text: &str,
) -> Result<()> {
    let Some(mut session) = state.sessions.get(session_id).cloned() else {
        bail!("unknown session `{session_id}`");
    };
    let trimmed = text.trim();
    let command_text = normalize_session_command_text(
        trimmed,
        &config.webex.bot_email,
        config.webex.bot_display_name.as_deref(),
    );

    if let Some(page) = parse_session_history_page(command_text) {
        match read_thread_history(codex, &session.thread_id).await {
            Ok(turns) => {
                let history = slice_thread_history_page(&turns, page, HISTORY_PAGE_SIZE);
                for message in render_history_page(
                    &session.thread_id,
                    &history.turns,
                    page,
                    HISTORY_PAGE_SIZE,
                    history.total_turns,
                ) {
                    send_plain_message(webex, &session.session_room_id, &message).await?;
                }
            }
            Err(error) => {
                send_plain_message(
                    webex,
                    &session.session_room_id,
                    &format!(
                        "Failed to read Codex history for thread `{}`: {error:#}",
                        session.thread_id
                    ),
                )
                .await?;
            }
        }
        return Ok(());
    }

    match command_text {
        "help" | "/help" => {
            send_plain_message(webex, &session.session_room_id, render_help()).await?;
            return Ok(());
        }
        "/status" => {
            send_plain_message(
                webex,
                &session.session_room_id,
                &render_status_summary(&session),
            )
            .await?;
            return Ok(());
        }
        "/resume" => {
            codex.thread_resume(&session.thread_id).await?;
            session.failure = None;
            session.state = SessionState::Idle;
            session.updated_at = Utc::now();
            state.upsert_session(session.clone());
            persist_event(
                event_log,
                state,
                BridgeEvent::SessionUpdated {
                    session: session.clone(),
                },
                config,
            )
            .await?;
            refresh_overview(webex, &session).await?;
            return Ok(());
        }
        "/pause" | "/stop" => {
            if let Some(turn_id) = session.active_turn_id.clone() {
                codex.turn_interrupt(&session.thread_id, &turn_id).await?;
            }
            session.state = SessionState::Paused;
            session.updated_at = Utc::now();
            state.upsert_session(session.clone());
            persist_event(
                event_log,
                state,
                BridgeEvent::SessionUpdated {
                    session: session.clone(),
                },
                config,
            )
            .await?;
            refresh_overview(webex, &session).await?;
            return Ok(());
        }
        _ => {}
    }

    codex.thread_resume(&session.thread_id).await.ok();
    let response = codex
        .turn_start(&session.thread_id, &session.repo_path, text)
        .await?;
    session.active_turn_id = response
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    session.failure = None;
    session.active_turn_buffer.clear();
    session.last_checkpoint = Some(format!("Submitted turn: {}", abbreviate(text, 80)));
    session.state = SessionState::Running;
    session.updated_at = Utc::now();
    state.upsert_session(session.clone());
    persist_event(
        event_log,
        state,
        BridgeEvent::SessionUpdated {
            session: session.clone(),
        },
        config,
    )
    .await?;
    refresh_overview(webex, &session).await?;
    Ok(())
}

async fn handle_attachment_action(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    installation: &InstallationIdentity,
    action: WebexAttachmentActionEvent,
) -> Result<()> {
    let action_name = find_string(&action.inputs, "wxcd_action");
    match action_name.as_deref() {
        Some("approval") => {
            let approval_id = find_string(&action.inputs, "approval_id")
                .ok_or_else(|| anyhow!("approval card payload missing approval_id"))?;
            let decision = find_string(&action.inputs, "decision")
                .ok_or_else(|| anyhow!("approval card payload missing decision"))?;
            ensure_approval_belongs_to_installation(
                state,
                &approval_id,
                &installation.installation_id,
            )?;
            resolve_approval(
                config,
                webex,
                event_log,
                state,
                codex,
                &approval_id,
                &decision,
            )
            .await?;
        }
        Some("status") | Some("resume") | Some("pause") | Some("archive") => {
            let session_id = find_string(&action.inputs, "session_id")
                .ok_or_else(|| anyhow!("card payload missing session_id"))?;
            ensure_session_id_belongs_to_installation(
                state,
                &session_id,
                &installation.installation_id,
            )?;
            let command = match action_name.as_deref().unwrap() {
                "status" => "/status",
                "resume" => "/resume",
                "pause" => "/pause",
                "archive" => {
                    archive_session(config, webex, event_log, state, codex, &session_id).await?;
                    return Ok(());
                }
                _ => unreachable!(),
            };
            handle_session_message(config, webex, event_log, state, codex, &session_id, command)
                .await?;
        }
        _ => {
            let attachment = webex
                .get_attachment_action(&action.attachment_action_id)
                .await?;
            if let Some(decision) = find_string(&attachment.inputs, "decision") {
                let approval_id = find_string(&attachment.inputs, "approval_id")
                    .ok_or_else(|| anyhow!("attachment action missing approval_id"))?;
                ensure_approval_belongs_to_installation(
                    state,
                    &approval_id,
                    &installation.installation_id,
                )?;
                resolve_approval(
                    config,
                    webex,
                    event_log,
                    state,
                    codex,
                    &approval_id,
                    &decision,
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn handle_codex_event(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    event: CodexEvent,
) -> Result<()> {
    match event {
        CodexEvent::Notification { method, params } => match method.as_str() {
            "thread/status/changed" => {
                let thread_id = json_str(&params, "/threadId")?;
                if let Some(session_id) = state.thread_to_session.get(thread_id).cloned()
                    && let Some(session) = state.sessions.get_mut(&session_id)
                {
                    let status = params
                        .pointer("/status/type")
                        .and_then(Value::as_str)
                        .unwrap_or("idle");
                    session.state = match status {
                        "active" => SessionState::Running,
                        "idle" if session.archived => SessionState::Archived,
                        "idle" if session.last_final.is_some() => SessionState::Completed,
                        _ => SessionState::Idle,
                    };
                    session.failure = None;
                    session.updated_at = Utc::now();
                    let updated = session.clone();
                    let _ = session;
                    state.upsert_session(updated.clone());
                    persist_event(
                        event_log,
                        state,
                        BridgeEvent::SessionUpdated {
                            session: updated.clone(),
                        },
                        config,
                    )
                    .await?;
                    refresh_overview(webex, &updated).await?;
                }
            }
            "turn/started" => {
                let thread_id = json_str(&params, "/threadId")?;
                let turn_id = json_str(&params, "/turn/id")?;
                if let Some(session_id) = state.thread_to_session.get(thread_id).cloned()
                    && let Some(session) = state.sessions.get_mut(&session_id)
                {
                    session.active_turn_id = Some(turn_id.to_string());
                    session.failure = None;
                    session.active_turn_buffer.clear();
                    session.state = SessionState::Running;
                    session.updated_at = Utc::now();
                    let updated = session.clone();
                    let _ = session;
                    state.upsert_session(updated.clone());
                    persist_event(
                        event_log,
                        state,
                        BridgeEvent::SessionUpdated {
                            session: updated.clone(),
                        },
                        config,
                    )
                    .await?;
                    refresh_overview(webex, &updated).await?;
                }
            }
            "item/agentMessage/delta" => {
                let thread_id = json_str(&params, "/threadId")?;
                let delta = params
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if let Some(session_id) = state.thread_to_session.get(thread_id).cloned()
                    && let Some(session) = state.sessions.get_mut(&session_id)
                {
                    session.active_turn_buffer.push_str(delta);
                }
            }
            "turn/completed" => {
                let thread_id = json_str(&params, "/threadId")?;
                if let Some(session_id) = state.thread_to_session.get(thread_id).cloned()
                    && let Some(session) = state.sessions.get_mut(&session_id)
                {
                    session.state = SessionState::Completed;
                    session.failure = None;
                    session.last_final = Some(session.active_turn_buffer.trim().to_string());
                    session.active_turn_id = None;
                    session.updated_at = Utc::now();
                    let updated = session.clone();
                    let summary = render_final_summary(&updated);
                    let _ = session;
                    state.upsert_session(updated.clone());
                    send_plain_message(webex, &updated.session_room_id, &summary).await?;
                    persist_event(
                        event_log,
                        state,
                        BridgeEvent::SessionUpdated {
                            session: updated.clone(),
                        },
                        config,
                    )
                    .await?;
                    refresh_overview(webex, &updated).await?;
                }
            }
            _ => {}
        },
        CodexEvent::ServerRequest { id, method, params } => match method.as_str() {
            "item/commandExecution/requestApproval" => {
                let approval =
                    build_pending_approval(state, id, ApprovalKind::CommandExecution, &params)?;
                request_approval(webex, event_log, state, config, approval).await?;
            }
            "item/fileChange/requestApproval" => {
                let approval =
                    build_pending_approval(state, id, ApprovalKind::FileChange, &params)?;
                request_approval(webex, event_log, state, config, approval).await?;
            }
            "item/permissions/requestApproval" => {
                let mut approval =
                    build_pending_approval(state, id, ApprovalKind::Permissions, &params)?;
                approval.requested_permissions = params.get("permissions").cloned();
                request_approval(webex, event_log, state, config, approval).await?;
            }
            other => {
                warn!("unhandled codex server request: {other}");
                codex
                    .respond_error(id, -32001, "wxcd does not handle this server request yet")
                    .await?;
            }
        },
    }

    Ok(())
}

async fn request_approval(
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    config: &AppConfig,
    mut approval: PendingApproval,
) -> Result<()> {
    let session = state
        .sessions
        .get(&approval.session_id)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "approval referenced unknown session {}",
                approval.session_id
            )
        })?;
    let card = webex
        .create_message(&CreateMessageRequest {
            room_id: session.session_room_id.clone(),
            text: Some(format!(
                "Approval required for session `{}`.",
                approval.session_id
            )),
            markdown: None,
            attachments: Some(vec![build_approval_attachment(&approval)]),
        })
        .await?;
    approval.card_message_id = Some(card.id.clone());
    state
        .pending_approvals
        .insert(approval.approval_id.clone(), approval.clone());
    if let Some(mut session_mut) = state.sessions.get(&approval.session_id).cloned() {
        session_mut.state = SessionState::WaitingApproval;
        session_mut.updated_at = Utc::now();
        state.upsert_session(session_mut.clone());
        persist_event(
            event_log,
            state,
            BridgeEvent::SessionUpdated {
                session: session_mut.clone(),
            },
            config,
        )
        .await?;
        refresh_overview(webex, &session_mut).await?;
    }
    persist_event(
        event_log,
        state,
        BridgeEvent::ApprovalRequested { approval },
        config,
    )
    .await?;
    Ok(())
}

async fn resolve_approval(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    approval_id: &str,
    decision: &str,
) -> Result<()> {
    let approval = state
        .pending_approvals
        .get(approval_id)
        .cloned()
        .ok_or_else(|| anyhow!("unknown approval `{approval_id}`"))?;
    let decision_enum = parse_decision(decision)?;
    match approval.kind {
        ApprovalKind::CommandExecution => {
            codex
                .respond(
                    approval.codex_request_id.clone(),
                    json!({
                        "decision": match decision_enum {
                            ApprovalDecision::Accept => "accept",
                            ApprovalDecision::AcceptForSession => "acceptForSession",
                            ApprovalDecision::Decline => "decline",
                            ApprovalDecision::Cancel => "cancel",
                        }
                    }),
                )
                .await?;
        }
        ApprovalKind::FileChange => {
            codex
                .respond(
                    approval.codex_request_id.clone(),
                    json!({
                        "decision": match decision_enum {
                            ApprovalDecision::Accept => "accept",
                            ApprovalDecision::AcceptForSession => "acceptForSession",
                            ApprovalDecision::Decline => "decline",
                            ApprovalDecision::Cancel => "cancel",
                        }
                    }),
                )
                .await?;
        }
        ApprovalKind::Permissions => match decision_enum {
            ApprovalDecision::Accept | ApprovalDecision::AcceptForSession => {
                codex
                    .respond(
                        approval.codex_request_id.clone(),
                        json!({
                            "permissions": approval.requested_permissions.clone().unwrap_or_else(|| json!({})),
                            "scope": if matches!(decision_enum, ApprovalDecision::AcceptForSession) {
                                "session"
                            } else {
                                "turn"
                            }
                        }),
                    )
                    .await?;
            }
            ApprovalDecision::Decline => {
                codex
                    .respond_error(
                        approval.codex_request_id.clone(),
                        -32010,
                        "permission request declined",
                    )
                    .await?;
            }
            ApprovalDecision::Cancel => {
                codex
                    .respond_error(
                        approval.codex_request_id.clone(),
                        -32011,
                        "permission request cancelled",
                    )
                    .await?;
            }
        },
    }

    state.pending_approvals.remove(approval_id);
    persist_event(
        event_log,
        state,
        BridgeEvent::ApprovalResolved {
            approval_id: approval_id.to_string(),
            session_id: approval.session_id.clone(),
            decision: decision_enum.clone(),
            resolved_at: Utc::now(),
        },
        config,
    )
    .await?;

    if let Some(mut session) = state.sessions.get(&approval.session_id).cloned() {
        session.state = SessionState::Running;
        session.failure = None;
        session.updated_at = Utc::now();
        state.upsert_session(session.clone());
        persist_event(
            event_log,
            state,
            BridgeEvent::SessionUpdated {
                session: session.clone(),
            },
            config,
        )
        .await?;
        refresh_overview(webex, &session).await?;
        send_plain_message(
            webex,
            &session.session_room_id,
            &format!("Resolved approval `{approval_id}` with `{decision}`."),
        )
        .await?;
    }

    Ok(())
}

async fn handle_diagnose_command(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    installation: &InstallationIdentity,
    command: DiagnoseCommand,
) -> Result<String> {
    match command {
        DiagnoseCommand::Sessions => {
            let mut sessions = sessions_for_diagnostics(state, &installation.installation_id);
            sessions.sort_by_key(|session| session.updated_at);
            sessions.reverse();
            Ok(render_failed_session_diagnostics(&sessions))
        }
        DiagnoseCommand::Session(session_id) => {
            let session = state
                .sessions
                .get(&session_id)
                .cloned()
                .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
            if !session_belongs_to_installation(&session, &installation.installation_id) {
                return Ok(format!(
                    "Session `{}` is indexed in Data Space but is not managed by this installation; diagnosis will not mutate it.",
                    session.session_id
                ));
            }
            if session.archived || session.state == SessionState::Archived {
                return Ok(format!(
                    "Session `{}` is archived; diagnosis will not mutate it. Use `purge archived {}` to preview Webex room deletion.",
                    session.session_id, session.session_id
                ));
            }
            let probe = probe_session_thread(codex, &session).await;
            if let Some(updated) = apply_thread_probe(&session, &probe, Utc::now()) {
                state.upsert_session(updated.clone());
                persist_event(
                    event_log,
                    state,
                    BridgeEvent::SessionUpdated {
                        session: updated.clone(),
                    },
                    config,
                )
                .await?;
                refresh_overview(webex, &updated).await.ok();
                return Ok(format!(
                    "Diagnosed session `{}`: {:?}. {}",
                    updated.session_id, probe.kind, probe.message
                ));
            }

            Ok(format!(
                "Diagnosed session `{}`: {:?}. {}",
                session.session_id, probe.kind, probe.message
            ))
        }
    }
}

async fn handle_cleanup_failed_command(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    installation: &InstallationIdentity,
    command: CleanupFailedCommand,
) -> Result<String> {
    match command {
        CleanupFailedCommand::Preview => {
            let mut sessions = cleanup_failed_sessions(state, &installation.installation_id);
            sessions.sort_by_key(|session| session.updated_at);
            sessions.reverse();
            Ok(render_cleanup_failed_preview(&sessions))
        }
        CleanupFailedCommand::Session(session_id) => {
            ensure_failed_cleanup_target(state, &session_id, &installation.installation_id)?;
            archive_session(config, webex, event_log, state, codex, &session_id).await?;
            Ok(format!(
                "Soft-archived failed session `{session_id}`. Use `purge archived {session_id}` to preview Webex room deletion."
            ))
        }
        CleanupFailedCommand::All => {
            let mut session_ids = state
                .sessions
                .values()
                .filter(|session| is_cleanup_failed_session(session, &installation.installation_id))
                .map(|session| session.session_id.clone())
                .collect::<Vec<_>>();
            session_ids.sort();
            if session_ids.is_empty() {
                return Ok("No failed active sessions found.".to_string());
            }
            let mut archived = Vec::new();
            for session_id in session_ids {
                archive_session(config, webex, event_log, state, codex, &session_id).await?;
                archived.push(session_id);
            }
            Ok(format!(
                "Soft-archived {} failed sessions: {}.",
                archived.len(),
                archived
                    .iter()
                    .map(|session_id| format!("`{session_id}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }
}

async fn handle_purge_archived_command(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    installation: &InstallationIdentity,
    command: PurgeArchivedCommand,
) -> Result<String> {
    let session =
        validate_purge_archived_session(state, &command.session_id, &installation.installation_id)?;
    if !command.confirmed {
        return Ok(render_purge_archived_warning(&session));
    }

    webex.delete_room(&session.session_room_id).await?;
    state.remove_session(&session.session_id);
    persist_event(
        event_log,
        state,
        BridgeEvent::SessionPurged {
            session_id: session.session_id.clone(),
            purged_at: Utc::now(),
        },
        config,
    )
    .await?;
    persist_snapshot(event_log, state, config).await?;
    Ok(format!(
        "Purged archived session `{}` and deleted Webex room `{}`.",
        session.session_id, session.title
    ))
}

fn ensure_failed_cleanup_target(
    state: &WorkerState,
    session_id: &str,
    installation_id: &str,
) -> Result<()> {
    let session = state
        .sessions
        .get(session_id)
        .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
    ensure_session_belongs_to_installation(session, installation_id)?;
    if session.archived || session.state == SessionState::Archived {
        bail!("session `{session_id}` is already archived");
    }
    if session.state != SessionState::Failed {
        bail!("session `{session_id}` is not failed");
    }
    Ok(())
}

fn validate_purge_archived_session(
    state: &WorkerState,
    session_id: &str,
    installation_id: &str,
) -> Result<SessionRecord> {
    let session = state
        .sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
    ensure_session_belongs_to_installation(&session, installation_id)?;
    if !session.archived && session.state != SessionState::Archived {
        bail!("session `{session_id}` is not archived; run `archive {session_id}` first");
    }
    Ok(session)
}

async fn archive_session(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    session_id: &str,
) -> Result<()> {
    let mut session = state
        .sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
    if session_requires_codex_archive(&session) {
        codex.thread_archive(&session.thread_id).await?;
    } else if let Err(error) = codex.thread_archive(&session.thread_id).await {
        warn!(
            "failed to archive missing local Codex thread {} for failed session {}: {error:#}",
            session.thread_id, session.session_id
        );
    }
    session.archived = true;
    session.state = SessionState::Archived;
    session.updated_at = Utc::now();
    webex
        .update_room_title(
            &session.session_room_id,
            &format!("[ARCHIVED] {}", session.title),
        )
        .await
        .ok();
    state.upsert_session(session.clone());
    state.remove_pending_approvals_for_session(session_id);
    persist_event(
        event_log,
        state,
        BridgeEvent::SessionArchived {
            session_id: session_id.to_string(),
            archived_at: Utc::now(),
        },
        config,
    )
    .await?;
    refresh_overview(webex, &session).await?;
    Ok(())
}

fn session_requires_codex_archive(session: &SessionRecord) -> bool {
    session.state != SessionState::Failed
}

fn sessions_for_control_list(state: &WorkerState, installation_id: &str) -> Vec<SessionRecord> {
    state
        .sessions
        .values()
        .filter(|session| is_default_list_session(session, installation_id))
        .cloned()
        .collect()
}

fn is_default_list_session(session: &SessionRecord, installation_id: &str) -> bool {
    !session.archived
        && session.state != SessionState::Archived
        && session.state != SessionState::Failed
        && session_belongs_to_installation(session, installation_id)
}

fn cleanup_failed_sessions(state: &WorkerState, installation_id: &str) -> Vec<SessionRecord> {
    state
        .sessions
        .values()
        .filter(|session| is_cleanup_failed_session(session, installation_id))
        .cloned()
        .collect()
}

fn sessions_for_diagnostics(state: &WorkerState, installation_id: &str) -> Vec<SessionRecord> {
    state
        .sessions
        .values()
        .filter(|session| session_belongs_to_installation(session, installation_id))
        .cloned()
        .collect()
}

fn is_cleanup_failed_session(session: &SessionRecord, installation_id: &str) -> bool {
    session.state == SessionState::Failed
        && !session.archived
        && session_belongs_to_installation(session, installation_id)
}

fn session_belongs_to_installation(session: &SessionRecord, installation_id: &str) -> bool {
    if let Some(authority) = &session.authority {
        return authority.installation_id == installation_id;
    }

    session
        .local_mirror
        .as_ref()
        .is_some_and(|mirror| mirror.installation_id == installation_id)
}

fn ensure_session_belongs_to_installation(
    session: &SessionRecord,
    installation_id: &str,
) -> Result<()> {
    if session_belongs_to_installation(session, installation_id) {
        return Ok(());
    }
    bail!(
        "session `{}` is not managed by this installation",
        session.session_id
    )
}

fn ensure_session_id_belongs_to_installation(
    state: &WorkerState,
    session_id: &str,
    installation_id: &str,
) -> Result<()> {
    let session = state
        .sessions
        .get(session_id)
        .ok_or_else(|| anyhow!("unknown session `{session_id}`"))?;
    ensure_session_belongs_to_installation(session, installation_id)
}

fn ensure_approval_belongs_to_installation(
    state: &WorkerState,
    approval_id: &str,
    installation_id: &str,
) -> Result<()> {
    let approval = state
        .pending_approvals
        .get(approval_id)
        .ok_or_else(|| anyhow!("unknown approval `{approval_id}`"))?;
    ensure_session_id_belongs_to_installation(state, &approval.session_id, installation_id)
}

async fn refresh_overview(webex: &WebexClient, session: &SessionRecord) -> Result<()> {
    let Some(message_id) = session.overview_message_id.as_deref() else {
        return Ok(());
    };
    if let Err(error) = webex
        .update_message(
            message_id,
            &UpdateMessageRequest {
                text: Some(render_status_summary(session)),
                markdown: None,
                attachments: Some(vec![build_overview_attachment(session)]),
            },
        )
        .await
    {
        warn!(
            "failed to refresh overview message {} for session {}: {error:#}",
            message_id, session.session_id
        );
    }
    Ok(())
}

async fn reconcile_sessions(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    installation: &InstallationIdentity,
) -> Result<()> {
    let session_ids = state.sessions.keys().cloned().collect::<Vec<_>>();
    for session_id in session_ids {
        let Some(session) = state.sessions.get(&session_id).cloned() else {
            continue;
        };
        if session.archived {
            continue;
        }
        if !session_belongs_to_installation(&session, &installation.installation_id) {
            continue;
        }
        let probe = probe_session_thread(codex, &session).await;
        if probe.kind == ThreadProbeKind::Readable {
            codex.thread_resume(&session.thread_id).await.ok();
        }
        if let Some(updated) = apply_thread_probe(&session, &probe, Utc::now()) {
            let room_id = updated.session_room_id.clone();
            let send_failure_message =
                updated.state == SessionState::Failed && session.state != SessionState::Failed;
            state.upsert_session(updated.clone());
            persist_event(
                event_log,
                state,
                BridgeEvent::SessionUpdated {
                    session: updated.clone(),
                },
                config,
            )
            .await?;
            if send_failure_message {
                send_plain_message(
                    webex,
                    &room_id,
                    &format!(
                        "Remote metadata exists, but the local Codex thread could not be recovered: {}",
                        probe.message
                    ),
                )
                .await
                .ok();
            }
        }
    }
    Ok(())
}

async fn probe_session_thread(codex: &mut CodexClient, session: &SessionRecord) -> ThreadProbe {
    match codex.thread_read(&session.thread_id, false).await {
        Ok(_) => {
            return ThreadProbe {
                kind: ThreadProbeKind::Readable,
                message: "Local Codex thread is readable.".to_string(),
            };
        }
        Err(read_error) => {
            warn!(
                "failed to read thread {} for session {}: {read_error:#}",
                session.thread_id, session.session_id
            );
        }
    }

    match codex.thread_resume(&session.thread_id).await {
        Ok(_) => match codex.thread_read(&session.thread_id, false).await {
            Ok(_) => {
                return ThreadProbe {
                    kind: ThreadProbeKind::Readable,
                    message: "Local Codex thread became readable after resume.".to_string(),
                };
            }
            Err(read_error) => {
                warn!(
                    "failed to read thread {} after resume for session {}: {read_error:#}",
                    session.thread_id, session.session_id
                );
            }
        },
        Err(resume_error) => {
            warn!(
                "failed to resume thread {} for session {}: {resume_error:#}",
                session.thread_id, session.session_id
            );
        }
    }

    match codex_thread_exists(codex, &session.thread_id).await {
        Ok(true) => ThreadProbe {
            kind: ThreadProbeKind::UnreadableThread,
            message: format!(
                "Local Codex thread `{}` is listed but `thread/read` still fails.",
                session.thread_id
            ),
        },
        Ok(false) => ThreadProbe {
            kind: ThreadProbeKind::MissingThread,
            message: format!(
                "Local Codex thread `{}` is missing from `thread/list`.",
                session.thread_id
            ),
        },
        Err(error) => ThreadProbe {
            kind: ThreadProbeKind::ProbeUnavailable,
            message: format!("Could not list local Codex threads: {error:#}"),
        },
    }
}

async fn codex_thread_exists(codex: &mut CodexClient, thread_id: &str) -> Result<bool> {
    let mut cursor = None;
    loop {
        let page = codex.thread_list_page(false, cursor.as_deref()).await?;
        if page.data.iter().any(|thread| thread.id == thread_id) {
            return Ok(true);
        }
        let Some(next_cursor) = page.next_cursor else {
            return Ok(false);
        };
        cursor = Some(next_cursor);
    }
}

fn apply_thread_probe(
    session: &SessionRecord,
    probe: &ThreadProbe,
    detected_at: chrono::DateTime<Utc>,
) -> Option<SessionRecord> {
    if session.archived || session.state == SessionState::Archived {
        return None;
    }
    match probe.kind {
        ThreadProbeKind::Readable => {
            if session.failure.is_none() && session.state != SessionState::Failed {
                return None;
            }
            let mut updated = session.clone();
            updated.failure = None;
            if updated.state == SessionState::Failed {
                updated.state = SessionState::Idle;
            }
            updated.last_checkpoint = Some(probe.message.clone());
            updated.updated_at = detected_at;
            Some(updated)
        }
        ThreadProbeKind::MissingThread
        | ThreadProbeKind::UnreadableThread
        | ThreadProbeKind::ProbeUnavailable => {
            let failure_kind = match probe.kind {
                ThreadProbeKind::MissingThread => SessionFailureKind::MissingThread,
                ThreadProbeKind::UnreadableThread => SessionFailureKind::UnreadableThread,
                ThreadProbeKind::ProbeUnavailable => SessionFailureKind::ProbeUnavailable,
                ThreadProbeKind::Readable => unreachable!(),
            };
            if session.state == SessionState::Failed
                && session.failure.as_ref().is_some_and(|failure| {
                    failure.kind == failure_kind && failure.message == probe.message
                })
            {
                return None;
            }
            let mut updated = session.clone();
            updated.state = SessionState::Failed;
            updated.failure = Some(SessionFailure {
                kind: failure_kind,
                message: probe.message.clone(),
                detected_at,
            });
            updated.last_checkpoint = Some(probe.message.clone());
            updated.updated_at = detected_at;
            Some(updated)
        }
    }
}

async fn persist_event(
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    event: BridgeEvent,
    config: &AppConfig,
) -> Result<()> {
    if let Err(error) = event_log.append_event(event).await {
        warn!("failed to append Data Space event, relying on local snapshot fallback: {error:#}");
    }
    state.events_since_snapshot += 1;
    if state.events_since_snapshot >= config.bridge.snapshot_interval {
        persist_snapshot(event_log, state, config).await?;
        return Ok(());
    }
    persist_local_snapshot(
        &config.bridge.state_dir.join("bridge-state.json"),
        &state.to_replay_state().to_snapshot(),
    )
    .await?;
    Ok(())
}

async fn persist_snapshot(
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    config: &AppConfig,
) -> Result<()> {
    let snapshot = state.to_replay_state().to_snapshot();
    if let Err(error) = event_log.append_snapshot(&snapshot).await {
        warn!("failed to append Data Space snapshot: {error:#}");
    }
    state.events_since_snapshot = 0;
    persist_local_snapshot(
        &config.bridge.state_dir.join("bridge-state.json"),
        &snapshot,
    )
    .await?;
    Ok(())
}

async fn run_ingress_server(
    listener: UnixListener,
    events_tx: mpsc::Sender<WebexIngressEnvelope>,
    healthy: Arc<AtomicBool>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let tx = events_tx.clone();
                let healthy = Arc::clone(&healthy);
                tokio::spawn(async move {
                    if let Err(error) = handle_ingress_connection(stream, tx, healthy).await {
                        warn!("ingress connection failed: {error:#}");
                    }
                });
            }
            Err(error) => {
                warn!("failed to accept unix socket connection: {error:#}");
            }
        }
    }
}

async fn handle_ingress_connection(
    stream: UnixStream,
    events_tx: mpsc::Sender<WebexIngressEnvelope>,
    healthy: Arc<AtomicBool>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let line = lines
        .next_line()
        .await
        .context("failed to read ingress payload")?
        .ok_or_else(|| anyhow!("ingress payload was empty"))?;
    let event: WebexIngressEnvelope =
        serde_json::from_str(&line).context("failed to decode ingress payload")?;
    if !matches!(event, WebexIngressEnvelope::HealthCheck) {
        events_tx
            .send(event)
            .await
            .context("failed to enqueue ingress event")?;
    }
    let ack = WebexIngressAck {
        ok: true,
        healthy: healthy.load(Ordering::Relaxed),
        detail: None,
    };
    let encoded = serde_json::to_vec(&ack)?;
    writer.write_all(&encoded).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn send_plain_message(webex: &WebexClient, room_id: &str, text: &str) -> Result<()> {
    webex
        .create_message(&CreateMessageRequest {
            room_id: room_id.to_string(),
            text: Some(text.to_string()),
            markdown: None,
            attachments: None,
        })
        .await?;
    Ok(())
}

fn matches_help_command(text: &str) -> bool {
    text.eq_ignore_ascii_case("help") || text.eq_ignore_ascii_case("/help")
}

fn parse_list_command(text: &str) -> Option<ListCommand> {
    if text.eq_ignore_ascii_case("list") || text.eq_ignore_ascii_case("/list") {
        return Some(ListCommand {
            mode: ListMode::Bridge,
            page: 1,
        });
    }
    let rest = text
        .strip_prefix("list ")
        .or_else(|| text.strip_prefix("/list "))?
        .trim();
    let (scope, page) = if let Some((scope, page_str)) = rest.rsplit_once(" page ") {
        let page = page_str.trim().parse::<usize>().ok()?;
        if page == 0 {
            return None;
        }
        (scope.trim(), page)
    } else {
        (rest, 1)
    };
    if scope.eq_ignore_ascii_case("local") {
        return Some(ListCommand {
            mode: ListMode::Local,
            page,
        });
    }
    if scope.eq_ignore_ascii_case("all") {
        return Some(ListCommand {
            mode: ListMode::All,
            page,
        });
    }
    None
}

fn parse_resume_local_thread_id(text: &str) -> Option<&str> {
    text.strip_prefix("resume local ")
        .or_else(|| text.strip_prefix("/resume local "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_attach_session_id(text: &str) -> Option<&str> {
    text.strip_prefix("attach ")
        .or_else(|| text.strip_prefix("/attach "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_diagnose_command(text: &str) -> Option<DiagnoseCommand> {
    let rest = text
        .strip_prefix("diagnose ")
        .or_else(|| text.strip_prefix("/diagnose "))?
        .trim();
    if rest.eq_ignore_ascii_case("sessions") {
        return Some(DiagnoseCommand::Sessions);
    }
    (!rest.is_empty()).then(|| DiagnoseCommand::Session(rest.to_string()))
}

fn parse_cleanup_failed_command(text: &str) -> Option<CleanupFailedCommand> {
    if text.eq_ignore_ascii_case("cleanup failed") || text.eq_ignore_ascii_case("/cleanup failed") {
        return Some(CleanupFailedCommand::Preview);
    }
    let rest = text
        .strip_prefix("cleanup failed ")
        .or_else(|| text.strip_prefix("/cleanup failed "))?;
    let rest = rest.trim();
    if rest.eq_ignore_ascii_case("all") {
        return Some(CleanupFailedCommand::All);
    }
    (!rest.is_empty()).then(|| CleanupFailedCommand::Session(rest.to_string()))
}

fn parse_purge_archived_command(text: &str) -> Option<PurgeArchivedCommand> {
    let rest = text
        .strip_prefix("purge archived ")
        .or_else(|| text.strip_prefix("/purge archived "))?
        .trim();
    if rest.is_empty() {
        return None;
    }
    let (session_id, confirmed) = match rest.strip_suffix(" confirm") {
        Some(session_id) => (session_id.trim(), true),
        None => (rest, false),
    };
    (!session_id.is_empty()).then(|| PurgeArchivedCommand {
        session_id: session_id.to_string(),
        confirmed,
    })
}

fn parse_session_history_page(text: &str) -> Option<usize> {
    if text.eq_ignore_ascii_case("/history") {
        return Some(1);
    }

    let page = text.strip_prefix("/history page ")?.trim().parse().ok()?;
    (page > 0).then_some(page)
}

fn normalize_session_command_text<'a>(
    text: &'a str,
    bot_email: &str,
    bot_display_name: Option<&str>,
) -> &'a str {
    normalize_prefixed_command_text(text, bot_email, bot_display_name, is_session_command)
}

fn is_session_command(text: &str) -> bool {
    parse_session_history_page(text).is_some()
        || matches!(
            text,
            "help" | "/help" | "/status" | "/resume" | "/pause" | "/stop"
        )
}

fn normalize_control_command_text<'a>(
    text: &'a str,
    bot_email: &str,
    bot_display_name: Option<&str>,
) -> &'a str {
    normalize_prefixed_command_text(text, bot_email, bot_display_name, is_control_command)
}

fn normalize_prefixed_command_text<'a>(
    text: &'a str,
    bot_email: &str,
    bot_display_name: Option<&str>,
    is_command: fn(&str) -> bool,
) -> &'a str {
    let trimmed = text.trim();
    if is_command(trimmed) {
        return trimmed;
    }

    for (index, ch) in trimmed.char_indices() {
        if !ch.is_whitespace() {
            continue;
        }
        let candidate = trimmed[index..].trim_start();
        let prefix = trimmed[..index].trim();
        if is_bot_mention_prefix(prefix, bot_email, bot_display_name) && is_command(candidate) {
            return candidate;
        }
    }

    trimmed
}

fn is_bot_mention_prefix(prefix: &str, bot_email: &str, bot_display_name: Option<&str>) -> bool {
    let Some(local_part) = bot_email.split('@').next() else {
        return false;
    };
    let normalized_prefix = normalize_mention_name(prefix);
    if normalized_prefix.is_empty() {
        return false;
    }
    normalized_prefix == normalize_mention_name(local_part)
        || bot_display_name.is_some_and(|name| normalized_prefix == normalize_mention_name(name))
}

fn normalize_mention_name(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_control_command(text: &str) -> bool {
    matches_help_command(text)
        || parse_list_command(text).is_some()
        || parse_resume_local_thread_id(text).is_some()
        || parse_attach_session_id(text).is_some()
        || parse_diagnose_command(text).is_some()
        || parse_cleanup_failed_command(text).is_some()
        || parse_purge_archived_command(text).is_some()
        || text.starts_with("new ")
        || text.starts_with("/new ")
        || text.starts_with("archive ")
        || text.starts_with("/archive ")
}

async fn collect_local_only_threads(
    codex: &mut CodexClient,
    state: &WorkerState,
    page: usize,
) -> Result<LocalOnlyThreads> {
    let managed_thread_ids = state
        .sessions
        .values()
        .map(|session| session.thread_id.as_str())
        .collect::<HashSet<_>>();

    let mut cursor = None;
    let mut local_only = Vec::new();
    let mut has_more = false;
    let target_count = page
        .checked_mul(LOCAL_THREAD_PAGE_SIZE)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| anyhow!("requested page is too large"))?;

    loop {
        let page = codex.thread_list_page(false, cursor.as_deref()).await?;
        for thread in page.data {
            if managed_thread_ids.contains(thread.id.as_str()) {
                continue;
            }
            local_only.push(summarize_local_thread(thread));
            if local_only.len() >= target_count {
                has_more = true;
                break;
            }
        }
        if has_more {
            break;
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    local_only.sort_by_key(|thread| std::cmp::Reverse(thread.updated_at));
    let total_count = local_only.len();
    let offset = (page - 1) * LOCAL_THREAD_PAGE_SIZE;
    let items = if offset < local_only.len() {
        local_only
            .into_iter()
            .skip(offset)
            .take(LOCAL_THREAD_PAGE_SIZE)
            .collect()
    } else {
        Vec::new()
    };
    Ok(LocalOnlyThreads {
        items,
        total_count,
        has_more,
        page,
        page_size: LOCAL_THREAD_PAGE_SIZE,
    })
}

fn summarize_local_thread(thread: CodexThreadSummary) -> LocalThreadListItem {
    let title = thread
        .name
        .filter(|value| !value.trim().is_empty())
        .or_else(|| thread.preview.filter(|value| !value.trim().is_empty()))
        .unwrap_or_else(|| "(untitled thread)".to_string());
    let status = thread
        .status
        .map(|status| status.kind)
        .unwrap_or_else(|| "unknown".to_string());
    LocalThreadListItem {
        thread_id: thread.id,
        title,
        cwd: thread.cwd,
        status,
        updated_at: thread.updated_at,
    }
}

async fn find_local_only_thread(
    codex: &mut CodexClient,
    state: &WorkerState,
    thread_id: &str,
) -> Result<CodexThreadSummary> {
    if let Some(session_id) = attached_session_for_thread(state, thread_id) {
        bail!("local Codex thread `{thread_id}` is already attached as session `{session_id}`");
    }

    let mut cursor = None;
    loop {
        let page = codex.thread_list_page(false, cursor.as_deref()).await?;
        if let Some(thread) = page.data.into_iter().find(|thread| thread.id == thread_id) {
            return Ok(thread);
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    bail!("unknown local Codex thread `{thread_id}`")
}

fn attached_session_for_thread<'a>(state: &'a WorkerState, thread_id: &str) -> Option<&'a str> {
    state
        .sessions
        .values()
        .find(|session| session.thread_id == thread_id)
        .map(|session| session.session_id.as_str())
}

fn repo_name_for_cwd(config: &AppConfig, cwd: &str) -> String {
    let cwd_path = Path::new(cwd);
    if let Some(repo) = config
        .repos
        .iter()
        .find(|repo| cwd_path.starts_with(&repo.path))
    {
        return repo.name.clone();
    }

    cwd_path
        .file_name()
        .and_then(|part| part.to_str())
        .unwrap_or("local")
        .to_string()
}

async fn import_local_thread_history(
    webex: &WebexClient,
    room_id: &str,
    thread_id: &str,
    history: &ImportedThreadHistory,
) -> Result<()> {
    if history.turns.is_empty() {
        return Ok(());
    }

    for message in render_imported_history(thread_id, &history.turns, history.total_turns) {
        send_plain_message(webex, room_id, &message).await?;
    }
    Ok(())
}

async fn read_thread_history(
    codex: &mut CodexClient,
    thread_id: &str,
) -> Result<Vec<ImportedHistoryTurn>> {
    let thread = match codex.thread_read(thread_id, true).await {
        Ok(thread) => thread,
        Err(read_error) => {
            warn!(
                "failed to read Codex thread {} directly, attempting resume before retry: {read_error:#}",
                thread_id
            );
            codex
                .thread_resume(thread_id)
                .await
                .with_context(|| format!("failed to resume Codex thread `{thread_id}`"))?;
            codex.thread_read(thread_id, true).await.with_context(|| {
                format!("failed to read Codex thread `{thread_id}` after resume")
            })?
        }
    };

    Ok(extract_thread_history_turns(&thread))
}

fn extract_thread_history_turns(thread: &Value) -> Vec<ImportedHistoryTurn> {
    let Some(turns) = thread.pointer("/thread/turns").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut imported = Vec::new();
    for turn in turns {
        let Some(items) = turn.get("items").and_then(Value::as_array) else {
            continue;
        };

        let mut user_texts = Vec::new();
        let mut assistant_texts = Vec::new();
        for item in items {
            match item.get("type").and_then(Value::as_str) {
                Some("userMessage") => {
                    let Some(contents) = item.get("content").and_then(Value::as_array) else {
                        continue;
                    };
                    for content in contents {
                        if content.get("type").and_then(Value::as_str) != Some("text") {
                            continue;
                        }
                        if let Some(text) = content.get("text").and_then(Value::as_str) {
                            let trimmed = text.trim();
                            if !trimmed.is_empty() {
                                user_texts.push(trimmed.to_string());
                            }
                        }
                    }
                }
                Some("agentMessage")
                    if item.get("phase").and_then(Value::as_str) == Some("final_answer") =>
                {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            assistant_texts.push(trimmed.to_string());
                        }
                    }
                }
                _ => {}
            }
        }

        if user_texts.is_empty() && assistant_texts.is_empty() {
            continue;
        }
        imported.push(ImportedHistoryTurn {
            user_text: user_texts.join("\n\n"),
            assistant_text: (!assistant_texts.is_empty()).then(|| assistant_texts.join("\n\n")),
        });
    }

    imported
}

fn latest_thread_history_summary(
    turns: &[ImportedHistoryTurn],
    limit: usize,
) -> ImportedThreadHistory {
    let total_turns = turns.len();
    let shown_turns = if total_turns > limit {
        turns[total_turns - limit..].to_vec()
    } else {
        turns.to_vec()
    };

    ImportedThreadHistory {
        turns: shown_turns,
        total_turns,
    }
}

fn slice_thread_history_page(
    turns: &[ImportedHistoryTurn],
    page: usize,
    page_size: usize,
) -> ImportedThreadHistory {
    let total_turns = turns.len();
    if total_turns == 0 {
        return ImportedThreadHistory {
            turns: Vec::new(),
            total_turns: 0,
        };
    }

    let newer_turns = page.saturating_sub(1).saturating_mul(page_size);
    if newer_turns >= total_turns {
        return ImportedThreadHistory {
            turns: Vec::new(),
            total_turns,
        };
    }

    let end = total_turns - newer_turns;
    let start = end.saturating_sub(page_size);
    ImportedThreadHistory {
        turns: turns[start..end].to_vec(),
        total_turns,
    }
}

fn build_pending_approval(
    state: &WorkerState,
    codex_request_id: Value,
    kind: ApprovalKind,
    params: &Value,
) -> Result<PendingApproval> {
    let thread_id = json_str(params, "/threadId")?;
    let session_id = state
        .thread_to_session
        .get(thread_id)
        .cloned()
        .ok_or_else(|| anyhow!("approval referenced unknown thread `{thread_id}`"))?;
    Ok(PendingApproval {
        approval_id: format!("apr_{}", uuid_suffix()),
        session_id,
        thread_id: thread_id.to_string(),
        turn_id: json_str(params, "/turnId")?.to_string(),
        codex_request_id,
        item_id: json_str(params, "/itemId")?.to_string(),
        kind,
        reason: params
            .get("reason")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        command: params
            .get("command")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        cwd: params
            .get("cwd")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        requested_permissions: None,
        card_message_id: None,
        requested_at: Utc::now(),
    })
}

fn uuid_suffix() -> String {
    use rand::Rng as _;

    let mut rng = rand::rng();
    format!("{:08x}", rng.random::<u32>())
}

fn parse_decision(decision: &str) -> Result<ApprovalDecision> {
    match decision {
        "accept" => Ok(ApprovalDecision::Accept),
        "accept_for_session" | "acceptForSession" => Ok(ApprovalDecision::AcceptForSession),
        "decline" => Ok(ApprovalDecision::Decline),
        "cancel" => Ok(ApprovalDecision::Cancel),
        _ => bail!("unsupported approval decision `{decision}`"),
    }
}

fn find_string(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(value) = map.get(key).and_then(Value::as_str) {
                return Some(value.to_string());
            }
            map.values().find_map(|child| find_string(child, key))
        }
        Value::Array(items) => items.iter().find_map(|item| find_string(item, key)),
        _ => None,
    }
}

fn json_str<'a>(value: &'a Value, pointer: &str) -> Result<&'a str> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing JSON pointer {pointer}"))
}

fn abbreviate(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let abbreviated = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        format!("{abbreviated}...")
    } else {
        abbreviated
    }
}

fn is_ignored_sender(config: &AppConfig, person_email: &str) -> bool {
    let normalized = person_email.to_ascii_lowercase();
    normalized == config.webex.bot_email
        || !config
            .webex
            .allowed_user_emails
            .iter()
            .any(|email| email == &normalized)
}

async fn remove_stale_socket(socket_path: &Path) -> Result<()> {
    if tokio::fs::try_exists(socket_path).await.unwrap_or(false) {
        tokio::fs::remove_file(socket_path)
            .await
            .with_context(|| format!("failed to remove stale socket {}", socket_path.display()))?;
    }
    Ok(())
}

async fn load_local_snapshot(path: &Path) -> Result<ReplayState> {
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Ok(ReplayState::default());
    }
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read local snapshot {}", path.display()))?;
    let snapshot = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse local snapshot {}", path.display()))?;
    Ok(ReplayState::from_snapshot(snapshot))
}

async fn persist_local_snapshot(path: &Path, snapshot: &wxcd_proto::BridgeSnapshot) -> Result<()> {
    let encoded = serde_json::to_string_pretty(snapshot)?;
    tokio::fs::write(path, encoded)
        .await
        .with_context(|| format!("failed to write local snapshot {}", path.display()))?;
    Ok(())
}

async fn load_or_create_installation_identity(state_dir: &Path) -> Result<InstallationIdentity> {
    let path = state_dir.join(INSTALLATION_IDENTITY_FILE);
    if tokio::fs::try_exists(&path).await.unwrap_or(false) {
        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read installation identity {}", path.display()))?;
        let identity = serde_json::from_str::<InstallationIdentity>(&content)
            .with_context(|| format!("failed to parse installation identity {}", path.display()))?;
        if identity.installation_id.trim().is_empty() {
            bail!(
                "installation identity {} has an empty installation_id",
                path.display()
            );
        }
        return Ok(identity);
    }

    let identity = InstallationIdentity {
        installation_id: generate_installation_id(Utc::now()),
        created_at: Utc::now(),
    };
    let encoded = serde_json::to_string_pretty(&identity)?;
    tokio::fs::write(&path, encoded)
        .await
        .with_context(|| format!("failed to write installation identity {}", path.display()))?;
    Ok(identity)
}

fn generate_installation_id(now: chrono::DateTime<Utc>) -> String {
    format!("ins_{}_{}", now.format("%Y%m%d"), uuid_suffix())
}

impl WorkerState {
    fn from_replay(replay: ReplayState) -> Self {
        let mut state = Self {
            events_since_snapshot: replay.events_since_snapshot,
            ..Self::default()
        };
        for session in replay.sessions.into_values() {
            state.upsert_session(session);
        }
        for approval in replay.pending_approvals.into_values() {
            state
                .pending_approvals
                .insert(approval.approval_id.clone(), approval);
        }
        state
    }

    fn upsert_session(&mut self, session: SessionRecord) {
        if let Some(previous) = self.sessions.get(&session.session_id) {
            self.room_to_session.remove(&previous.session_room_id);
            self.thread_to_session.remove(&previous.thread_id);
        }
        if self.should_index_session(&session) {
            self.room_to_session
                .insert(session.session_room_id.clone(), session.session_id.clone());
            self.thread_to_session
                .insert(session.thread_id.clone(), session.session_id.clone());
        }
        self.sessions.insert(session.session_id.clone(), session);
    }

    fn remove_session(&mut self, session_id: &str) -> Option<SessionRecord> {
        let session = self.sessions.remove(session_id)?;
        self.room_to_session.remove(&session.session_room_id);
        self.thread_to_session.remove(&session.thread_id);
        self.remove_pending_approvals_for_session(session_id);
        Some(session)
    }

    fn remove_pending_approvals_for_session(&mut self, session_id: &str) {
        self.pending_approvals
            .retain(|_, approval| approval.session_id != session_id);
    }

    fn set_executable_installation(&mut self, installation_id: &str) {
        self.executable_installation_id = Some(installation_id.to_string());
        self.rebuild_session_indexes();
    }

    fn merge_local_mirror(
        &mut self,
        local_replay: ReplayState,
        installation_id: &str,
        mirrored_at: chrono::DateTime<Utc>,
    ) -> bool {
        let mut changed = false;
        for local_session in local_replay.sessions.into_values() {
            if !session_is_claimable_local_mirror(&local_session, installation_id) {
                continue;
            }
            let Some(session) = self.sessions.get_mut(&local_session.session_id) else {
                continue;
            };
            if session.authority.is_none() {
                session.authority = Some(SessionAuthority {
                    installation_id: installation_id.to_string(),
                });
                changed = true;
            }
            let desired_mirror = local_session
                .local_mirror
                .filter(|mirror| mirror.installation_id == installation_id)
                .unwrap_or_else(|| LocalSessionMirror {
                    installation_id: installation_id.to_string(),
                    mirrored_at,
                });
            if session.local_mirror.as_ref() != Some(&desired_mirror) {
                session.local_mirror = Some(desired_mirror);
                changed = true;
            }
        }
        if changed {
            self.rebuild_session_indexes();
        }
        changed
    }

    fn rebuild_session_indexes(&mut self) {
        self.room_to_session.clear();
        self.thread_to_session.clear();
        for session in self.sessions.values() {
            if self.should_index_session(session) {
                self.room_to_session
                    .insert(session.session_room_id.clone(), session.session_id.clone());
                self.thread_to_session
                    .insert(session.thread_id.clone(), session.session_id.clone());
            }
        }
    }

    fn should_index_session(&self, session: &SessionRecord) -> bool {
        if session.archived
            || session.state == SessionState::Archived
            || session.state == SessionState::Failed
        {
            return false;
        }
        self.executable_installation_id
            .as_deref()
            .is_none_or(|installation_id| session_belongs_to_installation(session, installation_id))
    }

    fn remember_event(&mut self, event_id: &str) -> bool {
        if self.recent_event_ids.contains(event_id) {
            return false;
        }

        let event_id = event_id.to_string();
        self.recent_event_ids.insert(event_id.clone());
        self.recent_event_queue.push_back(event_id);
        while self.recent_event_queue.len() > RECENT_EVENT_ID_LIMIT {
            if let Some(evicted) = self.recent_event_queue.pop_front() {
                self.recent_event_ids.remove(&evicted);
            }
        }
        true
    }

    fn to_replay_state(&self) -> ReplayState {
        ReplayState {
            sessions: self.sessions.clone(),
            pending_approvals: self.pending_approvals.clone(),
            events_since_snapshot: self.events_since_snapshot,
        }
    }
}

fn session_is_claimable_local_mirror(session: &SessionRecord, installation_id: &str) -> bool {
    session.authority.as_ref().is_none_or(|authority| {
        authority.installation_id == installation_id
            || session
                .local_mirror
                .as_ref()
                .is_some_and(|mirror| mirror.installation_id == installation_id)
    })
}

#[cfg(test)]
mod tests;
