use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::{Duration, Instant, timeout};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wxcd_cbth_rpc::{
    PLUGIN_RPC_MAX_FRAME_BYTES, PLUGIN_RPC_PLUGIN_DRAIN_METHOD,
    PLUGIN_RPC_PLUGIN_HANDOFF_EXPORT_METHOD, PLUGIN_RPC_PLUGIN_HANDOFF_IMPORT_METHOD,
    PLUGIN_RPC_PLUGIN_HEALTH_CHECK_METHOD, PLUGIN_RPC_PLUGIN_LIFECYCLE_CAPABILITY,
    PLUGIN_RPC_PLUGIN_QUIESCE_METHOD, PLUGIN_RPC_PLUGIN_SHUTDOWN_METHOD,
    PLUGIN_RPC_PLUGIN_UNQUIESCE_METHOD, PLUGIN_RPC_PROTOCOL_VERSION_V1, PluginCapability,
    PluginDeliveryEnqueueRequest, PluginDeliveryTarget, PluginDrainRequest, PluginDrainResponse,
    PluginHealthCheckRequest, PluginHealthCheckResponse, PluginHelloRequest,
    PluginLifecycleAckResponse, PluginLifecycleMode, PluginQuiesceRequest, PluginRpcClient,
    PluginRpcError, PluginRpcErrorKind, PluginRpcRequestFrame, PluginRpcResponseFrame,
    PluginShutdownRequest, PluginUnquiesceRequest, read_plugin_rpc_frame, write_plugin_rpc_frame,
};
use wxcd_codex::{CodexClient, CodexEvent, CodexThreadSummary};
use wxcd_eventlog::{EventLog, ReplayState};
use wxcd_proto::{
    AppConfig, ApprovalDecision, ApprovalKind, BridgeEvent, CbthPluginConfig, DiagnosticsConfig,
    LocalSessionMirror, PendingApproval, SessionAuthority, SessionFailure, SessionFailureKind,
    SessionRecord, SessionState, WebexAsyncNotificationEvent, WebexAttachmentActionEvent,
    WebexIngressAck, WebexIngressEnvelope, WebexMessageEvent, generate_session_id,
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
const LOCAL_SNAPSHOT_FILE: &str = "bridge-state.json";
const PLUGIN_RPC_DOCTOR_TIMEOUT: Duration = Duration::from_secs(3);
const C5_DELIVERY_ACCEPTANCE_WINDOW_SECONDS: u64 = 60;
const PLUGIN_DELIVERY_BROKER_REQUEST_TIMEOUT: Duration =
    Duration::from_secs(C5_DELIVERY_ACCEPTANCE_WINDOW_SECONDS + 30);
const ASYNC_NOTIFICATION_INGRESS_ACK_TIMEOUT: Duration =
    Duration::from_secs(C5_DELIVERY_ACCEPTANCE_WINDOW_SECONDS + 45);
const PLUGIN_LIFECYCLE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const PLUGIN_LIFECYCLE_RESPONSE_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const PLUGIN_LIFECYCLE_CODEX_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const WXCD_CODEX_APP_SERVER_URL_ENV: &str = "WXCD_CODEX_APP_SERVER_URL";
const WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV: &str = "WXCD_CBTH_DELIVERY_BROKER_SOCKET";
const WXCD_CBTH_LIFECYCLE_SOCKET_ENV: &str = "WXCD_CBTH_LIFECYCLE_SOCKET";
const WXCD_SUPERVISOR_SHUTDOWN_MARKER_ENV: &str = "WXCD_SUPERVISOR_SHUTDOWN_MARKER";
const WEBEX_DELIVERY_MAX_ATTEMPTS: i64 = 3;
const WEBEX_DELIVERY_REDELIVERY_WINDOW_SECONDS: i64 = 3600;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Option<WorkerCommand>,
}

#[derive(Subcommand)]
enum WorkerCommand {
    Run,
    Doctor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ManifestStatus {
    Valid,
    Missing(String),
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RpcStatus {
    Disabled,
    MissingSocketPath,
    HelloOk { protocol_version: u32 },
    HelloFailed(String),
}

#[derive(Default)]
struct WorkerState {
    sessions: HashMap<String, SessionRecord>,
    room_to_session: HashMap<String, String>,
    thread_to_session: HashMap<String, String>,
    pending_approvals: HashMap<String, PendingApproval>,
    remote_snapshot_created_at: Option<chrono::DateTime<Utc>>,
    remote_archived_session_ids: HashSet<String>,
    remote_purged_session_ids: HashSet<String>,
    remote_resolved_approval_ids: HashSet<String>,
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

struct QueuedIngress {
    event: WebexIngressEnvelope,
    completion: Option<oneshot::Sender<std::result::Result<(), String>>>,
    work_permit: Option<LifecycleWorkPermit>,
}

enum WorkerQueueItem {
    Ingress(QueuedIngress),
    Lifecycle(QueuedLifecycleCommand),
}

struct QueuedLifecycleCommand {
    command: LifecycleCommand,
    completion: oneshot::Sender<std::result::Result<LifecycleCommandResponse, String>>,
    response_flushed: Option<oneshot::Receiver<()>>,
}

enum LifecycleCommand {
    Drain(PluginDrainRequest),
    Shutdown(PluginShutdownRequest),
    Unquiesce(PluginUnquiesceRequest),
}

enum LifecycleCommandResponse {
    Drain(PluginDrainResponse),
    Ack(PluginLifecycleAckResponse),
}

struct LifecycleCommandContext<'a, 'event_log> {
    config: &'a AppConfig,
    webex: &'a WebexClient,
    event_log: &'a EventLog<'event_log>,
    state: &'a mut WorkerState,
    codex: &'a mut CodexClient,
}

struct LifecycleUnquiesceContext<'a, 'event_log> {
    config: &'a AppConfig,
    webex: &'a WebexClient,
    event_log: &'a EventLog<'event_log>,
    state: &'a mut WorkerState,
    codex: &'a mut CodexClient,
    installation: &'a InstallationIdentity,
    lifecycle: &'a LifecycleControl,
    startup_reconcile_pending: &'a mut bool,
}

struct LifecycleRpcResponse {
    frame: PluginRpcResponseFrame,
    response_flushed: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleAdmissionPhase {
    Active,
    Quiescing,
    ShuttingDown,
}

#[derive(Debug)]
struct LifecycleControl {
    phase: Mutex<LifecycleAdmissionPhase>,
    in_flight: AtomicU64,
    drained: Notify,
}

struct LifecycleWorkPermit {
    lifecycle: Arc<LifecycleControl>,
    finished: bool,
}

impl LifecycleControl {
    fn new(initial_phase: LifecycleAdmissionPhase) -> Self {
        Self {
            phase: Mutex::new(initial_phase),
            in_flight: AtomicU64::new(0),
            drained: Notify::new(),
        }
    }

    fn phase(&self) -> LifecycleAdmissionPhase {
        *self.phase.lock().expect("lifecycle phase poisoned")
    }

    fn quiesce(&self) -> bool {
        let mut phase = self.phase.lock().expect("lifecycle phase poisoned");
        match *phase {
            LifecycleAdmissionPhase::Active | LifecycleAdmissionPhase::Quiescing => {
                *phase = LifecycleAdmissionPhase::Quiescing;
                true
            }
            LifecycleAdmissionPhase::ShuttingDown => false,
        }
    }

    fn unquiesce(&self) -> bool {
        let mut phase = self.phase.lock().expect("lifecycle phase poisoned");
        match *phase {
            LifecycleAdmissionPhase::Active | LifecycleAdmissionPhase::Quiescing => {
                *phase = LifecycleAdmissionPhase::Active;
                true
            }
            LifecycleAdmissionPhase::ShuttingDown => false,
        }
    }

    fn begin_shutdown(&self) -> Option<LifecycleAdmissionPhase> {
        let mut phase = self.phase.lock().expect("lifecycle phase poisoned");
        match *phase {
            LifecycleAdmissionPhase::ShuttingDown => None,
            LifecycleAdmissionPhase::Active | LifecycleAdmissionPhase::Quiescing => {
                let previous = *phase;
                *phase = LifecycleAdmissionPhase::ShuttingDown;
                Some(previous)
            }
        }
    }

    fn restore_shutdown_phase(&self, previous: LifecycleAdmissionPhase) {
        let mut phase = self.phase.lock().expect("lifecycle phase poisoned");
        if *phase == LifecycleAdmissionPhase::ShuttingDown {
            *phase = previous;
        }
    }

    fn try_begin_external_work(
        self: &Arc<Self>,
    ) -> std::result::Result<LifecycleWorkPermit, String> {
        let phase = self.phase.lock().expect("lifecycle phase poisoned");
        match *phase {
            LifecycleAdmissionPhase::Active => {
                self.in_flight.fetch_add(1, Ordering::SeqCst);
                Ok(LifecycleWorkPermit {
                    lifecycle: Arc::clone(self),
                    finished: false,
                })
            }
            LifecycleAdmissionPhase::Quiescing => {
                Err("plugin is quiescing and is not accepting new Webex work".to_string())
            }
            LifecycleAdmissionPhase::ShuttingDown => {
                Err("plugin is shutting down and is not accepting new Webex work".to_string())
            }
        }
    }

    fn in_flight_count(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }

    async fn wait_until_drained(&self, timeout_after: Duration) -> bool {
        timeout(timeout_after, async {
            loop {
                let notified = self.drained.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.in_flight_count() == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }

    fn health_check(
        &self,
        request: &PluginHealthCheckRequest,
        process_healthy: bool,
    ) -> PluginHealthCheckResponse {
        if !process_healthy {
            return PluginHealthCheckResponse {
                healthy: false,
                message: Some("worker has not completed startup health checks".to_string()),
            };
        }

        if matches!(request.mode, PluginLifecycleMode::PreActive)
            && (request.allow_external_side_effects
                || request.allow_cursor_advance
                || request.allow_delivery_commit)
        {
            return PluginHealthCheckResponse {
                healthy: false,
                message: Some(
                    "pre-active health check must fence side effects, cursor advance, and delivery commit"
                        .to_string(),
                ),
            };
        }

        let phase = self.phase();
        if phase == LifecycleAdmissionPhase::ShuttingDown {
            return PluginHealthCheckResponse {
                healthy: false,
                message: Some("worker is shutting down".to_string()),
            };
        }

        if matches!(request.mode, PluginLifecycleMode::PreActive)
            && phase == LifecycleAdmissionPhase::Active
        {
            return PluginHealthCheckResponse {
                healthy: false,
                message: Some(
                    "pre-active health check requires quiesced Webex work admission".to_string(),
                ),
            };
        }

        if matches!(request.mode, PluginLifecycleMode::Active)
            && phase != LifecycleAdmissionPhase::Active
        {
            return PluginHealthCheckResponse {
                healthy: false,
                message: Some("worker is quiesced and not accepting Webex work".to_string()),
            };
        }

        PluginHealthCheckResponse {
            healthy: true,
            message: None,
        }
    }
}

impl LifecycleWorkPermit {
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if self.lifecycle.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.lifecycle.drained.notify_waiters();
        }
    }
}

impl Drop for LifecycleWorkPermit {
    fn drop(&mut self) {
        self.finish();
    }
}

struct CreateBridgeSessionInput<'a> {
    owner_email: &'a str,
    repo_name: &'a str,
    repo_path: &'a str,
    thread_id: &'a str,
    checkpoint: &'a str,
    installation_id: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CodexConnectionConfig {
    Standalone,
    ManagedAppServer { url: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryBrokerConfig {
    socket_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InstallationIdentity {
    installation_id: String,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct LocalSnapshotIdentityMetadata {
    #[serde(default)]
    writer_installation_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match args.command.unwrap_or(WorkerCommand::Run) {
        WorkerCommand::Run => run().await,
        WorkerCommand::Doctor => run_doctor().await,
    }
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
    if config.bridge.cbth_plugin.enabled {
        tokio::fs::create_dir_all(&config.bridge.cbth_plugin.plugin_home)
            .await
            .with_context(|| {
                format!(
                    "failed to create plugin home {}",
                    config.bridge.cbth_plugin.plugin_home.display()
                )
            })?;
    }
    let installation = load_or_create_installation_identity(&config).await?;

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

    let mut codex = open_codex_client(&config).await?;
    codex
        .initialize("wxcd-worker", true)
        .await
        .context("failed to initialize codex app-server")?;
    let delivery_broker = resolve_delivery_broker_connection(
        &config,
        std::env::var(WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV).ok(),
    )?;

    let event_log = EventLog::new(&webex, &data_room.id);
    let local_snapshot_path = durable_local_snapshot_path(&config);
    let remote_replay = match event_log.replay().await {
        Ok(replay) => Some(replay),
        Err(error) => {
            warn!("failed to replay Data Space, falling back to local snapshot: {error:#}");
            None
        }
    };
    let replayed_data_space = remote_replay.is_some();
    let (replay, local_snapshot_metadata) = match remote_replay {
        Some(replay) => (replay, None),
        None => {
            let local_snapshot = load_durable_local_snapshot_with_metadata(&config).await?;
            (local_snapshot.replay, Some(local_snapshot.metadata))
        }
    };
    let mut state = WorkerState::from_replay(replay);
    state.set_executable_installation(&installation.installation_id);
    if replayed_data_space {
        match load_durable_local_snapshot_with_metadata(&config).await {
            Ok(local_snapshot) => {
                let LocalSnapshotReplay {
                    replay: local_replay,
                    metadata,
                } = local_snapshot;
                let changed = match metadata.writer_installation_id.as_deref() {
                    Some(writer_installation_id)
                        if writer_installation_id == installation.installation_id =>
                    {
                        match collect_local_thread_ids(&mut codex, true).await {
                            Ok(local_thread_ids) => state.merge_local_mirror(
                                local_replay,
                                &installation.installation_id,
                                Utc::now(),
                                LocalMirrorClaimScope::CurrentWriterSnapshotOrListedThreads(
                                    &local_thread_ids,
                                ),
                            ),
                            Err(error) => {
                                warn!(
                                    "failed to list local Codex threads before current-writer local mirror merge, only applying explicit current-installation evidence: {error:#}"
                                );
                                state.merge_local_mirror(
                                    local_replay,
                                    &installation.installation_id,
                                    Utc::now(),
                                    LocalMirrorClaimScope::CurrentWriterSnapshot,
                                )
                            }
                        }
                    }
                    Some(writer_installation_id) => {
                        warn!(
                            "local mirror snapshot belongs to installation {}, ignoring it for current installation {}",
                            writer_installation_id, installation.installation_id
                        );
                        false
                    }
                    None if metadata.existed => {
                        match collect_local_thread_ids(&mut codex, true).await {
                            Ok(local_thread_ids) => state.merge_local_mirror(
                                local_replay,
                                &installation.installation_id,
                                Utc::now(),
                                LocalMirrorClaimScope::ListedThreads(&local_thread_ids),
                            ),
                            Err(error) => {
                                warn!(
                                    "failed to list local Codex threads before legacy local mirror merge, leaving sessions unclaimed: {error:#}"
                                );
                                false
                            }
                        }
                    }
                    None => match collect_local_thread_ids(&mut codex, true).await {
                        Ok(local_thread_ids) => state.claim_legacy_local_sessions(
                            &installation.installation_id,
                            Utc::now(),
                            &local_thread_ids,
                        ),
                        Err(error) => {
                            warn!(
                                "failed to list local Codex threads before legacy Data Space claim, leaving sessions unclaimed: {error:#}"
                            );
                            false
                        }
                    },
                };
                if changed {
                    persist_snapshot(&event_log, &mut state, &config).await?;
                }
            }
            Err(error) => {
                warn!(
                    "failed to read local mirror snapshot after Data Space replay succeeded: {error:#}"
                );
            }
        }
    } else {
        if local_snapshot_metadata
            .is_some_and(|metadata| metadata.existed && metadata.writer_installation_id.is_none())
        {
            match collect_local_thread_ids(&mut codex, true).await {
                Ok(local_thread_ids) => {
                    let changed = state.claim_legacy_local_sessions(
                        &installation.installation_id,
                        Utc::now(),
                        &local_thread_ids,
                    );
                    if changed {
                        persist_local_snapshot(&local_snapshot_path, &state.to_snapshot()).await?;
                    }
                }
                Err(error) => {
                    warn!(
                        "failed to list local Codex threads before legacy snapshot claim, leaving sessions unclaimed: {error:#}"
                    );
                }
            }
        }
        state.rebuild_session_indexes();
    }
    let healthy = Arc::new(AtomicBool::new(false));
    let lifecycle = Arc::new(LifecycleControl::new(initial_lifecycle_phase(
        &config.bridge.cbth_plugin,
    )));
    let mut startup_reconcile_pending = lifecycle.phase() == LifecycleAdmissionPhase::Quiescing;
    if startup_reconcile_pending {
        info!("pre-active lifecycle mode enabled, deferring startup reconcile until unquiesce");
    } else {
        reconcile_sessions(
            &config,
            &webex,
            &event_log,
            &mut state,
            &mut codex,
            &installation,
        )
        .await?;
    }
    let (work_tx, mut work_rx) = mpsc::channel(256);
    let listener = UnixListener::bind(&config.bridge.socket_path).with_context(|| {
        format!(
            "failed to bind unix socket {}",
            config.bridge.socket_path.display()
        )
    })?;
    tokio::spawn(run_ingress_server(
        listener,
        work_tx.clone(),
        Arc::clone(&healthy),
        Arc::clone(&lifecycle),
    ));
    if let Some(socket_path) = lifecycle_control_socket_path(&config) {
        start_lifecycle_control_server(
            &socket_path,
            work_tx.clone(),
            Arc::clone(&healthy),
            Arc::clone(&lifecycle),
        )
        .await?;
    }
    healthy.store(true, Ordering::Relaxed);
    info!("wxcd worker is healthy");

    loop {
        tokio::select! {
            maybe_work = work_rx.recv() => {
                let Some(work) = maybe_work else {
                    break;
                };
                match work {
                    WorkerQueueItem::Ingress(mut event) => {
                        let result = handle_webex_ingress(
                            &config,
                            &webex,
                            &event_log,
                            &mut state,
                            &mut codex,
                            delivery_broker.as_ref(),
                            &control_room.id,
                            &installation,
                            event.event,
                        ).await.map_err(|error| format!("{error:#}"));
                        drop(event.work_permit.take());
                        if let Some(completion) = event.completion {
                            let _ = completion.send(result.clone());
                        }
                        if let Err(error) = result {
                            error!("failed to handle Webex ingress: {error}");
                        }
                    }
                    WorkerQueueItem::Lifecycle(command) => {
                        let QueuedLifecycleCommand {
                            command,
                            completion,
                            response_flushed,
                        } = command;
                        let should_shutdown = matches!(&command, LifecycleCommand::Shutdown(_));
                        let result = match command {
                            LifecycleCommand::Unquiesce(request) => {
                                handle_lifecycle_unquiesce(
                                    LifecycleUnquiesceContext {
                                        config: &config,
                                        webex: &webex,
                                        event_log: &event_log,
                                        state: &mut state,
                                        codex: &mut codex,
                                        installation: &installation,
                                        lifecycle: lifecycle.as_ref(),
                                        startup_reconcile_pending: &mut startup_reconcile_pending,
                                    },
                                    request,
                                )
                                .await
                            }
                            command => {
                                handle_lifecycle_command_with_runtime_drain(
                                    LifecycleCommandContext {
                                        config: &config,
                                        webex: &webex,
                                        event_log: &event_log,
                                        state: &mut state,
                                        codex: &mut codex,
                                    },
                                    command,
                                )
                                .await
                            }
                        }.map_err(|error| format!("{error:#}"));
                        let shutdown_accepted = should_shutdown
                            && matches!(
                                result.as_ref(),
                                Ok(LifecycleCommandResponse::Ack(response)) if response.accepted
                            );
                        let _ = completion.send(result);
                        if shutdown_accepted {
                            wait_for_lifecycle_response_flush(response_flushed).await;
                            info!("lifecycle shutdown requested, stopping worker");
                            break;
                        }
                    }
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
    if let Some(socket_path) = lifecycle_control_socket_path(&config) {
        remove_stale_socket(&socket_path).await.ok();
    }
    Ok(())
}

async fn open_codex_client(config: &AppConfig) -> Result<CodexClient> {
    match resolve_codex_connection(config, std::env::var(WXCD_CODEX_APP_SERVER_URL_ENV).ok())? {
        CodexConnectionConfig::Standalone => CodexClient::spawn().await,
        CodexConnectionConfig::ManagedAppServer { url } => {
            CodexClient::connect_websocket(&url).await
        }
    }
}

fn resolve_codex_connection(
    config: &AppConfig,
    managed_app_server_url: Option<String>,
) -> Result<CodexConnectionConfig> {
    if !config.bridge.cbth_plugin.enabled {
        return Ok(CodexConnectionConfig::Standalone);
    }

    match managed_app_server_url.filter(|value| !value.trim().is_empty()) {
        Some(url) => Ok(CodexConnectionConfig::ManagedAppServer { url }),
        None => {
            bail!("cbth plugin mode requires {WXCD_CODEX_APP_SERVER_URL_ENV} from wxcd-supervisor")
        }
    }
}

fn resolve_delivery_broker_connection(
    config: &AppConfig,
    broker_socket_path: Option<String>,
) -> Result<Option<DeliveryBrokerConfig>> {
    if !config.bridge.cbth_plugin.enabled {
        return Ok(None);
    }

    Ok(broker_socket_path
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .map(|socket_path| DeliveryBrokerConfig { socket_path }))
}

async fn run_doctor() -> Result<()> {
    let diagnostics = AppConfig::load_diagnostics()?;
    let manifest_status = validate_plugin_manifest(&diagnostics.bridge.cbth_plugin);
    let rpc_status = diagnose_plugin_rpc(&diagnostics.bridge.cbth_plugin).await;
    println!(
        "{}",
        render_doctor_report(&diagnostics, &manifest_status, &rpc_status)
    );
    Ok(())
}

fn validate_plugin_manifest(config: &CbthPluginConfig) -> ManifestStatus {
    let path = &config.manifest_path;
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ManifestStatus::Missing(path.display().to_string());
        }
        Err(error) => return ManifestStatus::Invalid(error.to_string()),
    };
    let value = match serde_json::from_str::<Value>(&content) {
        Ok(value) => value,
        Err(error) => return ManifestStatus::Invalid(format!("invalid JSON: {error}")),
    };

    let required_fields = [
        "/name",
        "/version",
        "/entrypoint/binary",
        "/capabilities",
        "/config_schema",
    ];
    let missing = required_fields
        .iter()
        .filter(|pointer| value.pointer(pointer).is_none())
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return ManifestStatus::Invalid(format!("missing fields: {}", missing.join(", ")));
    }
    if value.pointer("/name").and_then(Value::as_str) != Some("webex-connector") {
        return ManifestStatus::Invalid("manifest name must be webex-connector".to_string());
    }

    ManifestStatus::Valid
}

async fn diagnose_plugin_rpc(config: &CbthPluginConfig) -> RpcStatus {
    if !config.enabled {
        return RpcStatus::Disabled;
    }
    let Some(socket_path) = config.socket_path.as_ref() else {
        return RpcStatus::MissingSocketPath;
    };

    let request = PluginHelloRequest {
        plugin_name: "webex-connector".to_string(),
        plugin_instance_id: config.plugin_instance_id.clone(),
        plugin_release_id: config.plugin_release_id.clone(),
        protocol_versions: vec![PLUGIN_RPC_PROTOCOL_VERSION_V1],
        capabilities: vec![
            PluginCapability::new("diagnostics"),
            PluginCapability::new("standalone-compatible"),
            PluginCapability::new(PLUGIN_RPC_PLUGIN_LIFECYCLE_CAPABILITY),
        ],
        plugin_home: config.plugin_home.display().to_string(),
        pid: std::process::id(),
    };
    match timeout(PLUGIN_RPC_DOCTOR_TIMEOUT, async {
        let mut client = PluginRpcClient::connect(socket_path).await?;
        client.plugin_hello(request).await
    })
    .await
    {
        Ok(Ok(response)) => RpcStatus::HelloOk {
            protocol_version: response.protocol_version,
        },
        Ok(Err(error)) => RpcStatus::HelloFailed(format!("{error:#}")),
        Err(_) => RpcStatus::HelloFailed(format!(
            "plugin hello timed out after {}s",
            PLUGIN_RPC_DOCTOR_TIMEOUT.as_secs()
        )),
    }
}

fn render_doctor_report(
    diagnostics: &DiagnosticsConfig,
    manifest_status: &ManifestStatus,
    rpc_status: &RpcStatus,
) -> String {
    let plugin = &diagnostics.bridge.cbth_plugin;
    let mut lines = vec![
        "wxcd doctor".to_string(),
        format!("mode: {}", plugin.mode_name()),
        format!(
            "worker_socket: {}",
            diagnostics.bridge.socket_path.display()
        ),
        format!("state_dir: {}", diagnostics.bridge.state_dir.display()),
        format!("repos: {}", diagnostics.repos.len()),
        format!(
            "plugin_manifest: {}",
            render_manifest_status(manifest_status)
        ),
        format!("plugin_rpc: {}", render_rpc_status(rpc_status)),
    ];
    if !diagnostics.missing_webex_env.is_empty() {
        lines.push(format!(
            "webex_credentials: missing {}",
            diagnostics.missing_webex_env.join(", ")
        ));
    } else {
        lines.push("webex_credentials: present".to_string());
    }
    lines.join("\n")
}

fn render_manifest_status(status: &ManifestStatus) -> String {
    match status {
        ManifestStatus::Valid => "ok".to_string(),
        ManifestStatus::Missing(path) => format!("missing at {path}"),
        ManifestStatus::Invalid(message) => format!("invalid: {message}"),
    }
}

fn render_rpc_status(status: &RpcStatus) -> String {
    match status {
        RpcStatus::Disabled => "disabled".to_string(),
        RpcStatus::MissingSocketPath => "enabled but WXCD_CBTH_SOCKET_PATH is not set".to_string(),
        RpcStatus::HelloOk { protocol_version } => {
            format!("hello ok, protocol_version={protocol_version}")
        }
        RpcStatus::HelloFailed(message) => format!("hello failed: {message}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_webex_ingress(
    config: &AppConfig,
    webex: &WebexClient,
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    codex: &mut CodexClient,
    delivery_broker: Option<&DeliveryBrokerConfig>,
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
        WebexIngressEnvelope::AsyncNotification(notification) => {
            if !should_process_async_notification_event(state, &notification.event_id) {
                return Ok(());
            }
            enqueue_async_notification(delivery_broker, state, &notification).await?;
            state.remember_event(&notification.event_id);
        }
        WebexIngressEnvelope::HealthCheck | WebexIngressEnvelope::ActiveCheck => {}
    }

    Ok(())
}

async fn enqueue_async_notification(
    delivery_broker: Option<&DeliveryBrokerConfig>,
    state: &WorkerState,
    notification: &WebexAsyncNotificationEvent,
) -> Result<()> {
    let delivery_broker = delivery_broker
        .ok_or_else(|| anyhow!("async notification delivery requires cbth delivery broker"))?;
    let request = build_async_notification_delivery_request(state, notification)?;
    let result = timeout(PLUGIN_DELIVERY_BROKER_REQUEST_TIMEOUT, async {
        let mut client = PluginRpcClient::connect(&delivery_broker.socket_path)
            .await
            .with_context(|| {
                format!(
                    "failed to connect cbth delivery broker {}",
                    delivery_broker.socket_path.display()
                )
            })?;
        client.delivery_enqueue(request).await
    })
    .await
    .with_context(|| {
        format!(
            "timed out after {}s enqueueing async notification through cbth delivery broker",
            PLUGIN_DELIVERY_BROKER_REQUEST_TIMEOUT.as_secs()
        )
    })?
    .context("failed to enqueue async notification through cbth delivery broker")?;
    let driver_state = result
        .get("driver_state")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    info!(
        "enqueued Webex async notification {} through cbth delivery broker with driver_state={driver_state}",
        notification.event_id
    );
    Ok(())
}

fn build_async_notification_delivery_request(
    state: &WorkerState,
    notification: &WebexAsyncNotificationEvent,
) -> Result<PluginDeliveryEnqueueRequest> {
    let (session_id, thread_id) = resolve_async_notification_target(state, notification)?;
    let summary = notification.summary.trim();
    if summary.is_empty() {
        bail!("async notification summary must not be empty");
    }
    let event_id = notification.event_id.trim();
    if event_id.is_empty() {
        bail!("async notification event_id must not be empty");
    }
    let payload = json!({
        "kind": "webex_async_notification",
        "event_id": event_id,
        "session_id": session_id,
        "thread_id": thread_id,
        "summary": summary,
        "payload": notification.payload.clone(),
        "created": notification.created.to_rfc3339(),
    });
    Ok(PluginDeliveryEnqueueRequest {
        source_thread_id: thread_id.to_string(),
        summary: summary.to_string(),
        idempotency_key: webex_delivery_idempotency_key(event_id),
        inline_payload: Some(payload),
        artifact: None,
        delivery_policy: None,
        max_delivery_attempts: Some(WEBEX_DELIVERY_MAX_ATTEMPTS),
        redelivery_window_seconds: Some(WEBEX_DELIVERY_REDELIVERY_WINDOW_SECONDS),
        target: PluginDeliveryTarget {
            driver: "codex_app_server".to_string(),
            app_server_lease_id: None,
            managed_session_id: None,
            session_epoch: None,
            codex_binary: None,
        },
        plugin_metadata: Some(json!({
            "kind": "webex_async_notification",
            "webex_event_id": event_id,
            "session_id": session_id,
            "thread_id": thread_id,
        })),
    })
}

fn resolve_async_notification_target<'a>(
    state: &'a WorkerState,
    notification: &'a WebexAsyncNotificationEvent,
) -> Result<(&'a str, &'a str)> {
    match (
        notification.session_id.as_deref(),
        notification.thread_id.as_deref(),
    ) {
        (Some(session_id), Some(thread_id)) => {
            let session = state
                .sessions
                .get(session_id)
                .with_context(|| format!("unknown async notification session `{session_id}`"))?;
            ensure_async_delivery_session_is_executable(state, session)?;
            if session.thread_id != thread_id {
                bail!(
                    "async notification thread `{thread_id}` does not match session `{session_id}`"
                );
            }
            Ok((session_id, session.thread_id.as_str()))
        }
        (Some(session_id), None) => {
            let session = state
                .sessions
                .get(session_id)
                .with_context(|| format!("unknown async notification session `{session_id}`"))?;
            ensure_async_delivery_session_is_executable(state, session)?;
            Ok((session_id, session.thread_id.as_str()))
        }
        (None, Some(thread_id)) => {
            let session_id = state
                .thread_to_session
                .get(thread_id)
                .with_context(|| format!("unknown async notification thread `{thread_id}`"))?;
            Ok((session_id.as_str(), thread_id))
        }
        (None, None) => bail!("async notification must include session_id or thread_id"),
    }
}

fn ensure_async_delivery_session_is_executable(
    state: &WorkerState,
    session: &SessionRecord,
) -> Result<()> {
    if state.should_index_session_thread(session) {
        return Ok(());
    }
    bail!(
        "async notification session `{}` is not executable by this worker",
        session.session_id
    )
}

fn webex_delivery_idempotency_key(event_id: &str) -> String {
    format!("webex-delivery-{}", stable_fnv1a_hex(event_id))
}

fn stable_fnv1a_hex(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn should_process_async_notification_event(state: &WorkerState, event_id: &str) -> bool {
    !state.recent_event_ids.contains(event_id)
}

fn ingress_requires_processing_ack(event: &WebexIngressEnvelope) -> bool {
    matches!(event, WebexIngressEnvelope::AsyncNotification(_))
}

#[cfg(test)]
fn ingress_uses_delivery_enqueue(event: &WebexIngressEnvelope) -> bool {
    matches!(event, WebexIngressEnvelope::AsyncNotification(_))
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
        let mut sessions = sessions_for_control_list(
            state,
            &installation.installation_id,
            matches!(list_command.mode, ListMode::All),
        );
        sessions.sort_by_key(|session| session.updated_at);
        sessions.reverse();
        let rendered = match list_command.mode {
            ListMode::Bridge => render_control_list(&sessions),
            ListMode::Local => {
                let local_threads = collect_local_only_threads(
                    codex,
                    state,
                    &installation.installation_id,
                    list_command.page,
                )
                .await?;
                render_local_thread_list(
                    &local_threads.items,
                    local_threads.total_count,
                    local_threads.has_more,
                    local_threads.page,
                    local_threads.page_size,
                )
            }
            ListMode::All => {
                let local_threads = collect_local_only_threads(
                    codex,
                    state,
                    &installation.installation_id,
                    list_command.page,
                )
                .await?;
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
        let local_thread =
            find_local_only_thread(codex, state, &installation.installation_id, thread_id).await?;
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
    if session.state == SessionState::Failed && !is_failed_session_room_command(command_text) {
        send_plain_message(
            webex,
            &session.session_room_id,
            &render_failed_session_room_guard(&session),
        )
        .await?;
        return Ok(());
    }

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

fn is_failed_session_room_command(text: &str) -> bool {
    parse_session_history_page(text).is_some()
        || matches!(text, "help" | "/help" | "/status" | "/resume")
}

fn render_failed_session_room_guard(session: &SessionRecord) -> String {
    format!(
        "Session `{}` is failed and will not accept new turns until recovery. Use `/status`, `/history`, or `/resume` here; use `diagnose {}` or `cleanup failed {}` from the control room for operator cleanup.",
        session.session_id, session.session_id, session.session_id
    )
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

fn sessions_for_control_list(
    state: &WorkerState,
    installation_id: &str,
    include_archived: bool,
) -> Vec<SessionRecord> {
    state
        .sessions
        .values()
        .filter(|session| is_control_list_session(session, installation_id, include_archived))
        .cloned()
        .collect()
}

#[cfg(test)]
fn is_default_list_session(session: &SessionRecord, installation_id: &str) -> bool {
    is_control_list_session(session, installation_id, false)
}

fn is_control_list_session(
    session: &SessionRecord,
    installation_id: &str,
    include_archived: bool,
) -> bool {
    (include_archived || (!session.archived && session.state != SessionState::Archived))
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

async fn collect_local_thread_ids(
    codex: &mut CodexClient,
    include_archived: bool,
) -> Result<HashSet<String>> {
    let mut cursor = None;
    let mut thread_ids = HashSet::new();
    loop {
        let page = codex
            .thread_list_page(include_archived, cursor.as_deref())
            .await?;
        thread_ids.extend(page.data.into_iter().map(|thread| thread.id));
        let Some(next_cursor) = page.next_cursor else {
            return Ok(thread_ids);
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
    persist_local_snapshot(&durable_local_snapshot_path(config), &state.to_snapshot()).await?;
    Ok(())
}

async fn persist_snapshot(
    event_log: &EventLog<'_>,
    state: &mut WorkerState,
    config: &AppConfig,
) -> Result<()> {
    let snapshot = state.to_snapshot();
    if let Err(error) = event_log.append_snapshot(&snapshot).await {
        warn!("failed to append Data Space snapshot: {error:#}");
    }
    state.events_since_snapshot = 0;
    persist_local_snapshot(&durable_local_snapshot_path(config), &snapshot).await?;
    Ok(())
}

#[cfg(test)]
async fn handle_lifecycle_command(
    config: &AppConfig,
    state: &mut WorkerState,
    command: LifecycleCommand,
) -> Result<LifecycleCommandResponse> {
    persist_durable_lifecycle_mirror(config, state).await?;
    match command {
        LifecycleCommand::Drain(request) => {
            info!("lifecycle drain completed: {}", request.reason);
            Ok(LifecycleCommandResponse::Drain(PluginDrainResponse {
                drained: true,
                in_flight_count: Some(0),
            }))
        }
        LifecycleCommand::Shutdown(request) => {
            write_supervisor_shutdown_marker(config, &request).await?;
            info!("lifecycle shutdown accepted: {}", request.reason);
            Ok(LifecycleCommandResponse::Ack(PluginLifecycleAckResponse {
                accepted: true,
            }))
        }
        LifecycleCommand::Unquiesce(_) => bail!("unquiesce must be handled by the worker loop"),
    }
}

async fn handle_lifecycle_command_with_runtime_drain(
    ctx: LifecycleCommandContext<'_, '_>,
    command: LifecycleCommand,
) -> Result<LifecycleCommandResponse> {
    let mut ctx = ctx;
    match command {
        LifecycleCommand::Drain(request) => {
            let in_flight_count = drain_codex_runtime(&mut ctx).await?;
            persist_durable_lifecycle_mirror(ctx.config, ctx.state).await?;
            if in_flight_count > 0 {
                warn!(
                    "lifecycle drain incomplete: {in_flight_count} Codex events or sessions remain in flight"
                );
                return Ok(LifecycleCommandResponse::Drain(PluginDrainResponse {
                    drained: false,
                    in_flight_count: Some(in_flight_count),
                }));
            }
            info!("lifecycle drain completed: {}", request.reason);
            Ok(LifecycleCommandResponse::Drain(PluginDrainResponse {
                drained: true,
                in_flight_count: Some(0),
            }))
        }
        LifecycleCommand::Shutdown(request) => {
            let in_flight_count = drain_codex_runtime(&mut ctx).await?;
            persist_durable_lifecycle_mirror(ctx.config, ctx.state).await?;
            if in_flight_count > 0 {
                warn!(
                    "lifecycle shutdown rejected: {in_flight_count} Codex events or sessions remain in flight"
                );
                return Ok(LifecycleCommandResponse::Ack(PluginLifecycleAckResponse {
                    accepted: false,
                }));
            }
            write_supervisor_shutdown_marker(ctx.config, &request).await?;
            info!("lifecycle shutdown accepted: {}", request.reason);
            Ok(LifecycleCommandResponse::Ack(PluginLifecycleAckResponse {
                accepted: true,
            }))
        }
        LifecycleCommand::Unquiesce(_) => bail!("unquiesce must be handled by the worker loop"),
    }
}

async fn drain_codex_runtime(ctx: &mut LifecycleCommandContext<'_, '_>) -> Result<u64> {
    let deadline = Instant::now() + PLUGIN_LIFECYCLE_DRAIN_TIMEOUT;
    loop {
        process_pending_codex_events(ctx).await?;
        let in_flight_count = lifecycle_runtime_in_flight_count(ctx);
        if in_flight_count == 0 {
            return Ok(0);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(in_flight_count);
        }
        let wait_interval = lifecycle_runtime_wait_interval(remaining);
        match timeout(wait_interval, ctx.codex.events().recv()).await {
            Ok(Some(event)) => {
                handle_codex_event(
                    ctx.config,
                    ctx.webex,
                    ctx.event_log,
                    ctx.state,
                    ctx.codex,
                    event,
                )
                .await?;
            }
            Ok(None) => return Ok(lifecycle_runtime_in_flight_count(ctx)),
            Err(_) => {}
        }
    }
}

async fn process_pending_codex_events(ctx: &mut LifecycleCommandContext<'_, '_>) -> Result<()> {
    loop {
        let event = match ctx.codex.events().try_recv() {
            Ok(event) => event,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(()),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(()),
        };
        handle_codex_event(
            ctx.config,
            ctx.webex,
            ctx.event_log,
            ctx.state,
            ctx.codex,
            event,
        )
        .await?;
    }
}

fn lifecycle_runtime_in_flight_count(ctx: &mut LifecycleCommandContext<'_, '_>) -> u64 {
    ctx.state.lifecycle_codex_in_flight_count() + ctx.codex.events().len() as u64
}

fn lifecycle_runtime_wait_interval(remaining: Duration) -> Duration {
    if remaining < PLUGIN_LIFECYCLE_CODEX_DRAIN_POLL_INTERVAL {
        remaining
    } else {
        PLUGIN_LIFECYCLE_CODEX_DRAIN_POLL_INTERVAL
    }
}

async fn handle_lifecycle_unquiesce(
    ctx: LifecycleUnquiesceContext<'_, '_>,
    request: PluginUnquiesceRequest,
) -> Result<LifecycleCommandResponse> {
    if ctx.lifecycle.phase() == LifecycleAdmissionPhase::ShuttingDown {
        return Ok(LifecycleCommandResponse::Ack(PluginLifecycleAckResponse {
            accepted: false,
        }));
    }
    if *ctx.startup_reconcile_pending {
        reconcile_sessions(
            ctx.config,
            ctx.webex,
            ctx.event_log,
            ctx.state,
            ctx.codex,
            ctx.installation,
        )
        .await?;
        *ctx.startup_reconcile_pending = false;
    }
    let accepted = ctx.lifecycle.unquiesce();
    info!("lifecycle unquiesce completed: {}", request.reason);
    Ok(LifecycleCommandResponse::Ack(PluginLifecycleAckResponse {
        accepted,
    }))
}

async fn wait_for_lifecycle_response_flush(response_flushed: Option<oneshot::Receiver<()>>) {
    let Some(response_flushed) = response_flushed else {
        return;
    };
    match timeout(PLUGIN_LIFECYCLE_RESPONSE_FLUSH_TIMEOUT, response_flushed).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => warn!("lifecycle shutdown response writer dropped before flush confirmation"),
        Err(_) => warn!(
            "timed out after {}s waiting for lifecycle shutdown response flush",
            PLUGIN_LIFECYCLE_RESPONSE_FLUSH_TIMEOUT.as_secs()
        ),
    }
}

async fn persist_durable_lifecycle_mirror(config: &AppConfig, state: &WorkerState) -> Result<()> {
    persist_local_snapshot(&durable_local_snapshot_path(config), &state.to_snapshot()).await
}

fn durable_local_snapshot_path(config: &AppConfig) -> PathBuf {
    if config.bridge.cbth_plugin.enabled {
        config
            .bridge
            .cbth_plugin
            .plugin_home
            .join(LOCAL_SNAPSHOT_FILE)
    } else {
        config.bridge.state_dir.join(LOCAL_SNAPSHOT_FILE)
    }
}

fn lifecycle_control_socket_path(config: &AppConfig) -> Option<PathBuf> {
    lifecycle_control_socket_path_from_env(
        config,
        std::env::var_os(WXCD_CBTH_LIFECYCLE_SOCKET_ENV).as_deref(),
    )
}

fn lifecycle_control_socket_path_from_env(
    config: &AppConfig,
    socket_env: Option<&OsStr>,
) -> Option<PathBuf> {
    if !config.bridge.cbth_plugin.enabled {
        return None;
    }
    Some(
        socket_env
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| config.bridge.cbth_plugin.plugin_home.join("lifecycle.sock")),
    )
}

async fn write_supervisor_shutdown_marker(
    config: &AppConfig,
    request: &PluginShutdownRequest,
) -> Result<()> {
    let Some(path) = supervisor_shutdown_marker_path() else {
        return Ok(());
    };
    write_supervisor_shutdown_marker_at(&path, config, request).await
}

async fn write_supervisor_shutdown_marker_at(
    path: &Path,
    config: &AppConfig,
    request: &PluginShutdownRequest,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create shutdown marker directory {}",
                parent.display()
            )
        })?;
    }
    let marker = SupervisorShutdownMarker {
        plugin_instance_id: config.bridge.cbth_plugin.plugin_instance_id.clone(),
        plugin_release_id: config.bridge.cbth_plugin.plugin_release_id.clone(),
        reason: request.reason.clone(),
        created_at: Utc::now().to_rfc3339(),
    };
    tokio::fs::write(&path, serde_json::to_vec_pretty(&marker)?)
        .await
        .with_context(|| {
            format!(
                "failed to write supervisor shutdown marker {}",
                path.display()
            )
        })
}

fn supervisor_shutdown_marker_path() -> Option<PathBuf> {
    let path = std::env::var_os(WXCD_SUPERVISOR_SHUTDOWN_MARKER_ENV)?;
    if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(path))
    }
}

#[derive(Debug, Serialize)]
struct SupervisorShutdownMarker {
    plugin_instance_id: String,
    plugin_release_id: String,
    reason: String,
    created_at: String,
}

fn initial_lifecycle_phase(config: &CbthPluginConfig) -> LifecycleAdmissionPhase {
    initial_lifecycle_phase_from_env(
        config.enabled,
        std::env::var("WXCD_CBTH_PRE_ACTIVE").ok().as_deref(),
    )
}

fn initial_lifecycle_phase_from_env(
    plugin_enabled: bool,
    pre_active_env: Option<&str>,
) -> LifecycleAdmissionPhase {
    let normalized = pre_active_env.map(|value| value.trim().to_ascii_lowercase());
    match (plugin_enabled, normalized.as_deref()) {
        (true, Some("1" | "true" | "yes" | "on")) => LifecycleAdmissionPhase::Quiescing,
        _ => LifecycleAdmissionPhase::Active,
    }
}

async fn start_lifecycle_control_server(
    socket_path: &Path,
    events_tx: mpsc::Sender<WorkerQueueItem>,
    healthy: Arc<AtomicBool>,
    lifecycle: Arc<LifecycleControl>,
) -> Result<()> {
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!("failed to create lifecycle socket dir {}", parent.display())
        })?;
    }
    remove_stale_socket(socket_path).await?;
    let listener = UnixListener::bind(socket_path).with_context(|| {
        format!(
            "failed to bind cbth lifecycle control socket {}",
            socket_path.display()
        )
    })?;
    set_owner_only_socket_permissions(socket_path).await?;
    info!(
        "cbth lifecycle control socket ready at {}",
        socket_path.display()
    );
    tokio::spawn(run_lifecycle_control_server(
        listener, events_tx, healthy, lifecycle,
    ));
    Ok(())
}

async fn run_lifecycle_control_server(
    listener: UnixListener,
    events_tx: mpsc::Sender<WorkerQueueItem>,
    healthy: Arc<AtomicBool>,
    lifecycle: Arc<LifecycleControl>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let tx = events_tx.clone();
                let healthy = Arc::clone(&healthy);
                let lifecycle = Arc::clone(&lifecycle);
                tokio::spawn(async move {
                    if let Err(error) =
                        handle_lifecycle_control_connection(stream, tx, healthy, lifecycle).await
                    {
                        warn!("lifecycle control connection failed: {error:#}");
                    }
                });
            }
            Err(error) => warn!("failed to accept lifecycle control connection: {error:#}"),
        }
    }
}

async fn handle_lifecycle_control_connection(
    mut stream: UnixStream,
    events_tx: mpsc::Sender<WorkerQueueItem>,
    healthy: Arc<AtomicBool>,
    lifecycle: Arc<LifecycleControl>,
) -> Result<()> {
    loop {
        let frame: PluginRpcRequestFrame =
            match read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES).await {
                Ok(frame) => frame,
                Err(error)
                    if error.kind == PluginRpcErrorKind::Io
                        && error.message.contains("early eof") =>
                {
                    return Ok(());
                }
                Err(error) => return Err(anyhow!(error)),
            };
        let response = handle_lifecycle_rpc_frame(frame, &events_tx, &healthy, &lifecycle).await;
        write_plugin_rpc_frame(&mut stream, &response.frame, PLUGIN_RPC_MAX_FRAME_BYTES)
            .await
            .map_err(anyhow::Error::from)?;
        if let Some(response_flushed) = response.response_flushed {
            let _ = response_flushed.send(());
        }
    }
}

async fn handle_lifecycle_rpc_frame(
    frame: PluginRpcRequestFrame,
    events_tx: &mpsc::Sender<WorkerQueueItem>,
    healthy: &Arc<AtomicBool>,
    lifecycle: &Arc<LifecycleControl>,
) -> LifecycleRpcResponse {
    let mut response_flushed = None;
    let result = match frame.method.as_str() {
        PLUGIN_RPC_PLUGIN_HEALTH_CHECK_METHOD => {
            decode_lifecycle_params::<PluginHealthCheckRequest>(&frame).map(|request| {
                serde_json::to_value(
                    lifecycle.health_check(&request, healthy.load(Ordering::Relaxed)),
                )
                .expect("health response serializes")
            })
        }
        PLUGIN_RPC_PLUGIN_QUIESCE_METHOD => decode_lifecycle_params::<PluginQuiesceRequest>(&frame)
            .map(|_| {
                serde_json::to_value(PluginLifecycleAckResponse {
                    accepted: lifecycle.quiesce(),
                })
                .expect("quiesce response serializes")
            }),
        PLUGIN_RPC_PLUGIN_DRAIN_METHOD => {
            match decode_lifecycle_params::<PluginDrainRequest>(&frame) {
                Ok(request) => {
                    lifecycle.quiesce();
                    if !lifecycle
                        .wait_until_drained(PLUGIN_LIFECYCLE_DRAIN_TIMEOUT)
                        .await
                    {
                        Ok(serde_json::to_value(PluginDrainResponse {
                            drained: false,
                            in_flight_count: Some(lifecycle.in_flight_count()),
                        })
                        .expect("drain response serializes"))
                    } else {
                        lifecycle_command_response(
                            events_tx,
                            LifecycleCommand::Drain(request),
                            None,
                        )
                        .await
                        .and_then(|response| match response {
                            LifecycleCommandResponse::Drain(response) => {
                                serde_json::to_value(response).map_err(|error| {
                                    PluginRpcError::new(
                                        PluginRpcErrorKind::Internal,
                                        error.to_string(),
                                    )
                                })
                            }
                            LifecycleCommandResponse::Ack(_) => Err(PluginRpcError::new(
                                PluginRpcErrorKind::Internal,
                                "drain command returned ack response",
                            )),
                        })
                    }
                }
                Err(error) => Err(error),
            }
        }
        PLUGIN_RPC_PLUGIN_SHUTDOWN_METHOD => {
            match decode_lifecycle_params::<PluginShutdownRequest>(&frame) {
                Ok(request) => {
                    let previous_phase = match lifecycle.begin_shutdown() {
                        Some(previous_phase) => previous_phase,
                        None => {
                            return LifecycleRpcResponse {
                                frame: PluginRpcResponseFrame::success(
                                    frame.id,
                                    serde_json::to_value(PluginLifecycleAckResponse {
                                        accepted: false,
                                    })
                                    .expect("shutdown response serializes"),
                                ),
                                response_flushed: None,
                            };
                        }
                    };
                    if !lifecycle
                        .wait_until_drained(PLUGIN_LIFECYCLE_DRAIN_TIMEOUT)
                        .await
                    {
                        lifecycle.restore_shutdown_phase(previous_phase);
                        Ok(
                            serde_json::to_value(PluginLifecycleAckResponse { accepted: false })
                                .expect("shutdown response serializes"),
                        )
                    } else {
                        let (flushed_tx, flushed_rx) = oneshot::channel();
                        response_flushed = Some(flushed_tx);
                        match lifecycle_command_response(
                            events_tx,
                            LifecycleCommand::Shutdown(request),
                            Some(flushed_rx),
                        )
                        .await
                        {
                            Ok(response) => match response {
                                LifecycleCommandResponse::Ack(response) => {
                                    if !response.accepted {
                                        lifecycle.restore_shutdown_phase(previous_phase);
                                    }
                                    serde_json::to_value(response).map_err(|error| {
                                        PluginRpcError::new(
                                            PluginRpcErrorKind::Internal,
                                            error.to_string(),
                                        )
                                    })
                                }
                                LifecycleCommandResponse::Drain(_) => {
                                    lifecycle.restore_shutdown_phase(previous_phase);
                                    Err(PluginRpcError::new(
                                        PluginRpcErrorKind::Internal,
                                        "shutdown command returned drain response",
                                    ))
                                }
                            },
                            Err(error) => {
                                lifecycle.restore_shutdown_phase(previous_phase);
                                Err(error)
                            }
                        }
                    }
                }
                Err(error) => Err(error),
            }
        }
        PLUGIN_RPC_PLUGIN_UNQUIESCE_METHOD => {
            match decode_lifecycle_params::<PluginUnquiesceRequest>(&frame) {
                Ok(request) => lifecycle_command_response(
                    events_tx,
                    LifecycleCommand::Unquiesce(request),
                    None,
                )
                .await
                .and_then(|response| match response {
                    LifecycleCommandResponse::Ack(response) => serde_json::to_value(response)
                        .map_err(|error| {
                            PluginRpcError::new(PluginRpcErrorKind::Internal, error.to_string())
                        }),
                    LifecycleCommandResponse::Drain(_) => Err(PluginRpcError::new(
                        PluginRpcErrorKind::Internal,
                        "unquiesce command returned drain response",
                    )),
                }),
                Err(error) => Err(error),
            }
        }
        PLUGIN_RPC_PLUGIN_HANDOFF_EXPORT_METHOD | PLUGIN_RPC_PLUGIN_HANDOFF_IMPORT_METHOD => {
            Err(PluginRpcError::new(
                PluginRpcErrorKind::MethodNotFound,
                "webex-connector W5 does not implement optional plugin handoff",
            ))
        }
        _ => Err(PluginRpcError::new(
            PluginRpcErrorKind::MethodNotFound,
            format!("unsupported lifecycle method `{}`", frame.method),
        )),
    };

    match result {
        Ok(value) => LifecycleRpcResponse {
            frame: PluginRpcResponseFrame::success(frame.id, value),
            response_flushed,
        },
        Err(error) => LifecycleRpcResponse {
            frame: PluginRpcResponseFrame::failure(frame.id, error),
            response_flushed,
        },
    }
}

async fn lifecycle_command_response(
    events_tx: &mpsc::Sender<WorkerQueueItem>,
    command: LifecycleCommand,
    response_flushed: Option<oneshot::Receiver<()>>,
) -> std::result::Result<LifecycleCommandResponse, PluginRpcError> {
    let (completion, response) = oneshot::channel();
    events_tx
        .send(WorkerQueueItem::Lifecycle(QueuedLifecycleCommand {
            command,
            completion,
            response_flushed,
        }))
        .await
        .map_err(|_| {
            PluginRpcError::new(
                PluginRpcErrorKind::TransientDaemonUnavailable,
                "worker lifecycle command queue is closed",
            )
        })?;
    response
        .await
        .map_err(|_| {
            PluginRpcError::new(
                PluginRpcErrorKind::TransientDaemonUnavailable,
                "worker stopped before completing lifecycle command",
            )
        })?
        .map_err(|message| PluginRpcError::new(PluginRpcErrorKind::Internal, message))
}

fn decode_lifecycle_params<T>(
    frame: &PluginRpcRequestFrame,
) -> std::result::Result<T, PluginRpcError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(frame.params.clone()).map_err(|error| {
        PluginRpcError::new(
            PluginRpcErrorKind::InvalidRequest,
            format!("invalid {} params: {error}", frame.method),
        )
    })
}

async fn set_owner_only_socket_permissions(socket_path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(socket_path, permissions)
            .await
            .with_context(|| {
                format!(
                    "failed to set lifecycle control socket permissions on {}",
                    socket_path.display()
                )
            })?;
    }
    Ok(())
}

async fn run_ingress_server(
    listener: UnixListener,
    events_tx: mpsc::Sender<WorkerQueueItem>,
    healthy: Arc<AtomicBool>,
    lifecycle: Arc<LifecycleControl>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let tx = events_tx.clone();
                let healthy = Arc::clone(&healthy);
                let lifecycle = Arc::clone(&lifecycle);
                tokio::spawn(async move {
                    if let Err(error) =
                        handle_ingress_connection(stream, tx, healthy, lifecycle).await
                    {
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
    events_tx: mpsc::Sender<WorkerQueueItem>,
    healthy: Arc<AtomicBool>,
    lifecycle: Arc<LifecycleControl>,
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
    let ack = match event {
        WebexIngressEnvelope::HealthCheck => WebexIngressAck {
            ok: true,
            healthy: healthy.load(Ordering::Relaxed),
            detail: None,
        },
        WebexIngressEnvelope::ActiveCheck => {
            worker_active_check_ack(healthy.load(Ordering::Relaxed), lifecycle.phase())
        }
        event if ingress_requires_processing_ack(&event) => {
            let work_permit = match lifecycle.try_begin_external_work() {
                Ok(work_permit) => work_permit,
                Err(detail) => {
                    return write_ingress_ack(
                        &mut writer,
                        WebexIngressAck {
                            ok: false,
                            healthy: healthy.load(Ordering::Relaxed),
                            detail: Some(detail),
                        },
                    )
                    .await;
                }
            };
            let (completion_tx, completion_rx) = oneshot::channel();
            match timeout(ASYNC_NOTIFICATION_INGRESS_ACK_TIMEOUT, async {
                events_tx
                    .send(WorkerQueueItem::Ingress(QueuedIngress {
                        event,
                        completion: Some(completion_tx),
                        work_permit: Some(work_permit),
                    }))
                    .await
                    .context("failed to enqueue ingress event")?;
                completion_rx
                    .await
                    .map_err(|_| anyhow!("ingress handler stopped before processing event"))
            })
            .await
            {
                Ok(Ok(Ok(()))) => WebexIngressAck {
                    ok: true,
                    healthy: healthy.load(Ordering::Relaxed),
                    detail: None,
                },
                Ok(Ok(Err(detail))) => WebexIngressAck {
                    ok: false,
                    healthy: healthy.load(Ordering::Relaxed),
                    detail: Some(detail),
                },
                Ok(Err(error)) => WebexIngressAck {
                    ok: false,
                    healthy: healthy.load(Ordering::Relaxed),
                    detail: Some(format!("{error:#}")),
                },
                Err(_) => WebexIngressAck {
                    ok: false,
                    healthy: healthy.load(Ordering::Relaxed),
                    detail: Some(format!(
                        "timed out after {}s processing async notification",
                        ASYNC_NOTIFICATION_INGRESS_ACK_TIMEOUT.as_secs()
                    )),
                },
            }
        }
        event => {
            let work_permit = match lifecycle.try_begin_external_work() {
                Ok(work_permit) => work_permit,
                Err(detail) => {
                    return write_ingress_ack(
                        &mut writer,
                        WebexIngressAck {
                            ok: false,
                            healthy: healthy.load(Ordering::Relaxed),
                            detail: Some(detail),
                        },
                    )
                    .await;
                }
            };
            events_tx
                .send(WorkerQueueItem::Ingress(QueuedIngress {
                    event,
                    completion: None,
                    work_permit: Some(work_permit),
                }))
                .await
                .context("failed to enqueue ingress event")?;
            WebexIngressAck {
                ok: true,
                healthy: healthy.load(Ordering::Relaxed),
                detail: None,
            }
        }
    };
    write_ingress_ack(&mut writer, ack).await
}

fn worker_active_check_ack(
    process_healthy: bool,
    phase: LifecycleAdmissionPhase,
) -> WebexIngressAck {
    if !process_healthy {
        return WebexIngressAck {
            ok: false,
            healthy: false,
            detail: Some("worker has not completed startup health checks".to_string()),
        };
    }
    if phase == LifecycleAdmissionPhase::Active {
        return WebexIngressAck {
            ok: true,
            healthy: true,
            detail: None,
        };
    }
    WebexIngressAck {
        ok: false,
        healthy: true,
        detail: Some(match phase {
            LifecycleAdmissionPhase::Quiescing => {
                "worker is quiescing and not accepting new Webex work".to_string()
            }
            LifecycleAdmissionPhase::ShuttingDown => "worker is shutting down".to_string(),
            LifecycleAdmissionPhase::Active => unreachable!("active phase handled above"),
        }),
    }
}

async fn write_ingress_ack(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    ack: WebexIngressAck,
) -> Result<()> {
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
    installation_id: &str,
    page: usize,
) -> Result<LocalOnlyThreads> {
    let managed_thread_ids = state
        .sessions
        .values()
        .filter(|session| session_belongs_to_installation(session, installation_id))
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
    installation_id: &str,
    thread_id: &str,
) -> Result<CodexThreadSummary> {
    if let Some(session_id) = attached_session_for_thread(state, thread_id, installation_id) {
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

fn attached_session_for_thread<'a>(
    state: &'a WorkerState,
    thread_id: &str,
    installation_id: &str,
) -> Option<&'a str> {
    state
        .sessions
        .values()
        .find(|session| {
            session.thread_id == thread_id
                && session_belongs_to_installation(session, installation_id)
        })
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

#[derive(Debug)]
struct LocalSnapshotMetadata {
    existed: bool,
    writer_installation_id: Option<String>,
}

#[derive(Debug)]
struct LocalSnapshotReplay {
    replay: ReplayState,
    metadata: LocalSnapshotMetadata,
}

async fn load_local_snapshot_with_metadata(path: &Path) -> Result<LocalSnapshotReplay> {
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Ok(LocalSnapshotReplay {
            replay: ReplayState::default(),
            metadata: LocalSnapshotMetadata {
                existed: false,
                writer_installation_id: None,
            },
        });
    }
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read local snapshot {}", path.display()))?;
    let snapshot: wxcd_proto::BridgeSnapshot = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse local snapshot {}", path.display()))?;
    let writer_installation_id = snapshot.writer_installation_id.clone();
    Ok(LocalSnapshotReplay {
        replay: ReplayState::from_snapshot(snapshot),
        metadata: LocalSnapshotMetadata {
            existed: true,
            writer_installation_id,
        },
    })
}

async fn load_durable_local_snapshot_with_metadata(
    config: &AppConfig,
) -> Result<LocalSnapshotReplay> {
    let primary_path = durable_local_snapshot_path(config);
    let primary = load_local_snapshot_with_metadata(&primary_path).await?;
    if primary.metadata.existed {
        return Ok(primary);
    }

    let legacy_path = config.bridge.state_dir.join(LOCAL_SNAPSHOT_FILE);
    if legacy_path == primary_path {
        return Ok(primary);
    }

    load_local_snapshot_with_metadata(&legacy_path).await
}

async fn persist_local_snapshot(path: &Path, snapshot: &wxcd_proto::BridgeSnapshot) -> Result<()> {
    let encoded = serde_json::to_string_pretty(snapshot)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create local snapshot dir {}", parent.display()))?;
    }
    tokio::fs::write(path, encoded)
        .await
        .with_context(|| format!("failed to write local snapshot {}", path.display()))?;
    Ok(())
}

async fn load_or_create_installation_identity(config: &AppConfig) -> Result<InstallationIdentity> {
    let state_dir = &config.bridge.state_dir;
    let path = state_dir.join(INSTALLATION_IDENTITY_FILE);
    if tokio::fs::try_exists(&path)
        .await
        .with_context(|| format!("failed to inspect installation identity {}", path.display()))?
    {
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

    let identity = match recover_installation_identity_from_snapshot(config).await? {
        Some(identity) => identity,
        None => InstallationIdentity {
            installation_id: generate_installation_id(Utc::now()),
            created_at: Utc::now(),
        },
    };
    persist_installation_identity(&path, &identity).await?;
    Ok(identity)
}

async fn recover_installation_identity_from_snapshot(
    config: &AppConfig,
) -> Result<Option<InstallationIdentity>> {
    let durable_path = durable_local_snapshot_path(config);
    let legacy_path = config.bridge.state_dir.join(LOCAL_SNAPSHOT_FILE);
    let paths = if durable_path == legacy_path {
        vec![durable_path]
    } else {
        vec![durable_path, legacy_path]
    };

    for snapshot_path in paths {
        if let Some(identity) =
            recover_installation_identity_from_snapshot_path(&snapshot_path).await?
        {
            return Ok(Some(identity));
        }
    }
    Ok(None)
}

async fn recover_installation_identity_from_snapshot_path(
    snapshot_path: &Path,
) -> Result<Option<InstallationIdentity>> {
    if !tokio::fs::try_exists(snapshot_path).await.unwrap_or(false) {
        return Ok(None);
    }

    let content = match tokio::fs::read_to_string(snapshot_path).await {
        Ok(content) => content,
        Err(error) => {
            warn!(
                "failed to read local snapshot {} while recovering missing installation identity, minting a new identity: {error:#}",
                snapshot_path.display()
            );
            return Ok(None);
        }
    };
    let metadata: LocalSnapshotIdentityMetadata = match serde_json::from_str(&content) {
        Ok(metadata) => metadata,
        Err(error) => {
            warn!(
                "failed to parse local snapshot {} while recovering missing installation identity, minting a new identity: {error:#}",
                snapshot_path.display()
            );
            return Ok(None);
        }
    };
    let Some(installation_id) = metadata.writer_installation_id else {
        return Ok(None);
    };
    if installation_id.trim().is_empty() {
        warn!(
            "local snapshot {} has an empty writer_installation_id while recovering missing installation identity, minting a new identity",
            snapshot_path.display()
        );
        return Ok(None);
    }
    Ok(Some(InstallationIdentity {
        installation_id,
        created_at: Utc::now(),
    }))
}

async fn persist_installation_identity(path: &Path, identity: &InstallationIdentity) -> Result<()> {
    let encoded = serde_json::to_string_pretty(&identity)?;
    tokio::fs::write(&path, encoded)
        .await
        .with_context(|| format!("failed to write installation identity {}", path.display()))?;
    Ok(())
}

fn generate_installation_id(now: chrono::DateTime<Utc>) -> String {
    format!("ins_{}_{}", now.format("%Y%m%d"), uuid_suffix())
}

impl WorkerState {
    fn from_replay(replay: ReplayState) -> Self {
        let mut state = Self {
            events_since_snapshot: replay.events_since_snapshot,
            remote_snapshot_created_at: replay.snapshot_created_at,
            remote_archived_session_ids: replay.archived_session_ids,
            remote_purged_session_ids: replay.purged_session_ids,
            remote_resolved_approval_ids: replay.resolved_approval_ids,
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
        if self.should_route_session_room(&session) {
            self.room_to_session
                .insert(session.session_room_id.clone(), session.session_id.clone());
        }
        if self.should_index_session_thread(&session) {
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
        claim_scope: LocalMirrorClaimScope<'_>,
    ) -> bool {
        let mut changed = false;
        let local_pending_approvals = local_replay.pending_approvals;
        for mut local_session in local_replay.sessions.into_values() {
            if !local_session_is_claimable_mirror_evidence(&local_session, installation_id) {
                continue;
            }
            if !claim_scope.allows(&local_session, installation_id) {
                continue;
            }
            let desired_mirror = local_session
                .local_mirror
                .clone()
                .filter(|mirror| mirror.installation_id == installation_id)
                .unwrap_or_else(|| LocalSessionMirror {
                    installation_id: installation_id.to_string(),
                    mirrored_at,
                });
            if local_session.authority.is_none() {
                local_session.authority = Some(SessionAuthority {
                    installation_id: installation_id.to_string(),
                });
            }
            if local_session.local_mirror.as_ref() != Some(&desired_mirror) {
                local_session.local_mirror = Some(desired_mirror.clone());
            }
            let session_id = local_session.session_id.clone();
            let Some(session) = self.sessions.get(&session_id) else {
                if self.local_session_is_stale_against_remote(&local_session) {
                    continue;
                }
                self.upsert_session(local_session);
                changed = true;
                continue;
            };
            if !remote_session_accepts_local_mirror_claim(session, installation_id) {
                continue;
            }
            let remote_blocks_local_replacement =
                remote_session_blocks_local_replacement(session, &local_session)
                    || (self.remote_archived_session_ids.contains(&session_id)
                        && !(local_session.archived
                            || local_session.state == SessionState::Archived));
            if local_session.updated_at > session.updated_at && !remote_blocks_local_replacement {
                self.upsert_session(local_session);
                changed = true;
                continue;
            }
            let session = self
                .sessions
                .get_mut(&session_id)
                .expect("session should exist after immutable lookup");
            if session.authority.is_none() || session.local_mirror.as_ref() != Some(&desired_mirror)
            {
                if session.authority.is_none() {
                    session.authority = Some(SessionAuthority {
                        installation_id: installation_id.to_string(),
                    });
                }
                if session.local_mirror.as_ref() != Some(&desired_mirror) {
                    session.local_mirror = Some(desired_mirror);
                }
                changed = true;
            }
        }
        if self.merge_local_pending_approvals(local_pending_approvals, installation_id) {
            changed = true;
        }
        if changed {
            self.rebuild_session_indexes();
        }
        changed
    }

    fn merge_local_pending_approvals(
        &mut self,
        pending_approvals: HashMap<String, PendingApproval>,
        installation_id: &str,
    ) -> bool {
        let mut changed = false;
        for approval in pending_approvals.into_values() {
            if self.local_approval_is_stale_against_remote(&approval) {
                continue;
            }
            let Some(session) = self.sessions.get(&approval.session_id) else {
                continue;
            };
            if !session_belongs_to_installation(session, installation_id) {
                continue;
            }
            if approval.thread_id != session.thread_id {
                continue;
            }
            match self.pending_approvals.get(&approval.approval_id) {
                Some(existing) if existing.requested_at >= approval.requested_at => {}
                _ => {
                    self.pending_approvals
                        .insert(approval.approval_id.clone(), approval);
                    changed = true;
                }
            }
        }
        changed
    }

    fn local_session_is_stale_against_remote(&self, session: &SessionRecord) -> bool {
        self.remote_purged_session_ids.contains(&session.session_id)
            || self
                .remote_snapshot_created_at
                .as_ref()
                .is_some_and(|snapshot_created_at| *snapshot_created_at >= session.updated_at)
    }

    fn local_approval_is_stale_against_remote(&self, approval: &PendingApproval) -> bool {
        self.remote_resolved_approval_ids
            .contains(&approval.approval_id)
            || self
                .remote_snapshot_created_at
                .as_ref()
                .is_some_and(|snapshot_created_at| *snapshot_created_at >= approval.requested_at)
    }

    fn claim_legacy_local_sessions(
        &mut self,
        installation_id: &str,
        mirrored_at: chrono::DateTime<Utc>,
        local_thread_ids: &HashSet<String>,
    ) -> bool {
        let mut changed = false;
        for session in self.sessions.values_mut() {
            if session.authority.is_some() || session.local_mirror.is_some() {
                continue;
            }
            if !local_thread_ids.contains(&session.thread_id) {
                continue;
            }
            session.authority = Some(SessionAuthority {
                installation_id: installation_id.to_string(),
            });
            session.local_mirror = Some(LocalSessionMirror {
                installation_id: installation_id.to_string(),
                mirrored_at,
            });
            changed = true;
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
            if self.should_route_session_room(session) {
                self.room_to_session
                    .insert(session.session_room_id.clone(), session.session_id.clone());
            }
            if self.should_index_session_thread(session) {
                self.thread_to_session
                    .insert(session.thread_id.clone(), session.session_id.clone());
            }
        }
    }

    fn should_route_session_room(&self, session: &SessionRecord) -> bool {
        if session.archived || session.state == SessionState::Archived {
            return false;
        }
        self.executable_installation_id
            .as_deref()
            .is_none_or(|installation_id| session_belongs_to_installation(session, installation_id))
    }

    fn should_index_session_thread(&self, session: &SessionRecord) -> bool {
        session.state != SessionState::Failed && self.should_route_session_room(session)
    }

    fn lifecycle_codex_in_flight_count(&self) -> u64 {
        self.sessions
            .values()
            .filter(|session| self.should_route_session_room(session))
            .filter(|session| {
                session.active_turn_id.is_some()
                    || matches!(
                        session.state,
                        SessionState::Creating
                            | SessionState::Running
                            | SessionState::WaitingApproval
                    )
            })
            .count() as u64
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
            snapshot_created_at: self.remote_snapshot_created_at,
            archived_session_ids: self.remote_archived_session_ids.clone(),
            purged_session_ids: self.remote_purged_session_ids.clone(),
            resolved_approval_ids: self.remote_resolved_approval_ids.clone(),
        }
    }

    fn to_snapshot(&self) -> wxcd_proto::BridgeSnapshot {
        let mut snapshot = self.to_replay_state().to_snapshot();
        snapshot.writer_installation_id = self.executable_installation_id.clone();
        snapshot
    }
}

#[derive(Debug, Clone, Copy)]
enum LocalMirrorClaimScope<'a> {
    CurrentWriterSnapshot,
    CurrentWriterSnapshotOrListedThreads(&'a HashSet<String>),
    ListedThreads(&'a HashSet<String>),
}

impl LocalMirrorClaimScope<'_> {
    fn allows(&self, session: &SessionRecord, installation_id: &str) -> bool {
        match self {
            Self::CurrentWriterSnapshot => {
                local_session_has_current_installation_evidence(session, installation_id)
            }
            Self::CurrentWriterSnapshotOrListedThreads(local_thread_ids) => {
                local_session_has_current_installation_evidence(session, installation_id)
                    || local_thread_ids.contains(&session.thread_id)
            }
            Self::ListedThreads(local_thread_ids) => local_thread_ids.contains(&session.thread_id),
        }
    }
}

fn local_session_is_claimable_mirror_evidence(
    session: &SessionRecord,
    installation_id: &str,
) -> bool {
    session
        .authority
        .as_ref()
        .is_none_or(|authority| authority.installation_id == installation_id)
}

fn local_session_has_current_installation_evidence(
    session: &SessionRecord,
    installation_id: &str,
) -> bool {
    session
        .authority
        .as_ref()
        .is_some_and(|authority| authority.installation_id == installation_id)
        || session
            .local_mirror
            .as_ref()
            .is_some_and(|mirror| mirror.installation_id == installation_id)
}

fn remote_session_accepts_local_mirror_claim(
    session: &SessionRecord,
    installation_id: &str,
) -> bool {
    session
        .authority
        .as_ref()
        .is_none_or(|authority| authority.installation_id == installation_id)
}

fn remote_session_blocks_local_replacement(
    remote_session: &SessionRecord,
    local_session: &SessionRecord,
) -> bool {
    (remote_session.archived || remote_session.state == SessionState::Archived)
        && !(local_session.archived || local_session.state == SessionState::Archived)
}

#[cfg(test)]
mod tests;
