use super::{
    CleanupFailedCommand, DiagnoseCommand, ListCommand, ListMode, PurgeArchivedCommand,
    RECENT_EVENT_ID_LIMIT, ThreadProbe, ThreadProbeKind, WorkerState, abbreviate,
    apply_thread_probe, ensure_failed_cleanup_target, extract_thread_history_turns,
    generate_installation_id, is_default_list_session, load_or_create_installation_identity,
    normalize_control_command_text, normalize_session_command_text, parse_attach_session_id,
    parse_cleanup_failed_command, parse_diagnose_command, parse_list_command,
    parse_purge_archived_command, parse_resume_local_thread_id, parse_session_history_page,
    repo_name_for_cwd, session_belongs_to_installation, session_requires_codex_archive,
    slice_thread_history_page, validate_purge_archived_session,
};
use chrono::Utc;
use serde_json::json;
use wxcd_proto::{
    AppConfig, BridgeConfig, RepoConfig, SessionAuthority, SessionFailure, SessionFailureKind,
    SessionRecord, SessionState, WebexConfig,
};
use wxcd_render::ImportedHistoryTurn;

const BOT_EMAIL: &str = "codex-webex-connector@webex.bot";
const BOT_DISPLAY_NAME: &str = "Codex Webex Connector";

#[test]
fn strips_single_word_mention_prefix() {
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector list", BOT_EMAIL, None),
        "list"
    );
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector list local", BOT_EMAIL, None),
        "list local"
    );
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector list local page 2", BOT_EMAIL, None),
        "list local page 2"
    );
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector /help", BOT_EMAIL, None),
        "/help"
    );
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector new repo :: task", BOT_EMAIL, None),
        "new repo :: task"
    );
    assert_eq!(
        normalize_control_command_text(
            "Codex-Webex-Connector resume local 019d6eff",
            BOT_EMAIL,
            None
        ),
        "resume local 019d6eff"
    );
    assert_eq!(
        normalize_control_command_text(
            "Codex-Webex-Connector attach ses_20260417_abc123",
            BOT_EMAIL,
            None
        ),
        "attach ses_20260417_abc123"
    );
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector diagnose sessions", BOT_EMAIL, None),
        "diagnose sessions"
    );
    assert_eq!(
        normalize_control_command_text("Codex-Webex-Connector cleanup failed all", BOT_EMAIL, None),
        "cleanup failed all"
    );
    assert_eq!(
        normalize_control_command_text(
            "Codex-Webex-Connector purge archived ses_20260417_abc123 confirm",
            BOT_EMAIL,
            None
        ),
        "purge archived ses_20260417_abc123 confirm"
    );
}

#[test]
fn strips_multi_word_mention_prefix() {
    assert_eq!(
        normalize_control_command_text("Codex Webex Connector /list", BOT_EMAIL, None),
        "/list"
    );
    assert_eq!(
        normalize_control_command_text(
            "Bridge Controller /list",
            BOT_EMAIL,
            Some("Bridge Controller")
        ),
        "/list"
    );
}

#[test]
fn leaves_non_commands_untouched() {
    assert_eq!(
        normalize_control_command_text("hello there", BOT_EMAIL, None),
        "hello there"
    );
    assert_eq!(
        normalize_control_command_text("please list", BOT_EMAIL, None),
        "please list"
    );
}

#[test]
fn parses_list_commands() {
    assert_eq!(
        parse_list_command("list"),
        Some(ListCommand {
            mode: ListMode::Bridge,
            page: 1
        })
    );
    assert_eq!(
        parse_list_command("/list"),
        Some(ListCommand {
            mode: ListMode::Bridge,
            page: 1
        })
    );
    assert_eq!(
        parse_list_command("list local"),
        Some(ListCommand {
            mode: ListMode::Local,
            page: 1
        })
    );
    assert_eq!(
        parse_list_command("/list all page 3"),
        Some(ListCommand {
            mode: ListMode::All,
            page: 3
        })
    );
    assert_eq!(parse_list_command("list whatever"), None);
    assert_eq!(parse_list_command("list local page 0"), None);
}

#[test]
fn parses_resume_local_command() {
    assert_eq!(
        parse_resume_local_thread_id("resume local 019d6eff"),
        Some("019d6eff")
    );
    assert_eq!(
        parse_resume_local_thread_id("/resume local 019d6eff"),
        Some("019d6eff")
    );
    assert_eq!(parse_resume_local_thread_id("resume local "), None);
}

#[test]
fn parses_attach_session_command() {
    assert_eq!(
        parse_attach_session_id("attach ses_20260417_abc123"),
        Some("ses_20260417_abc123")
    );
    assert_eq!(
        parse_attach_session_id("/attach ses_20260417_abc123"),
        Some("ses_20260417_abc123")
    );
    assert_eq!(parse_attach_session_id("attach "), None);
}

#[test]
fn parses_diagnose_cleanup_and_purge_commands() {
    assert_eq!(
        parse_diagnose_command("diagnose sessions"),
        Some(DiagnoseCommand::Sessions)
    );
    assert_eq!(
        parse_diagnose_command("/diagnose ses_1"),
        Some(DiagnoseCommand::Session("ses_1".to_string()))
    );
    assert_eq!(
        parse_cleanup_failed_command("cleanup failed"),
        Some(CleanupFailedCommand::Preview)
    );
    assert_eq!(
        parse_cleanup_failed_command("/cleanup failed all"),
        Some(CleanupFailedCommand::All)
    );
    assert_eq!(
        parse_cleanup_failed_command("cleanup failed ses_1"),
        Some(CleanupFailedCommand::Session("ses_1".to_string()))
    );
    assert_eq!(
        parse_purge_archived_command("purge archived ses_1"),
        Some(PurgeArchivedCommand {
            session_id: "ses_1".to_string(),
            confirmed: false
        })
    );
    assert_eq!(
        parse_purge_archived_command("/purge archived ses_1 confirm"),
        Some(PurgeArchivedCommand {
            session_id: "ses_1".to_string(),
            confirmed: true
        })
    );
    assert_eq!(parse_cleanup_failed_command("cleanup failedness"), None);
    assert_eq!(parse_purge_archived_command("purge archived "), None);
}

#[test]
fn parses_session_history_command() {
    assert_eq!(parse_session_history_page("/history"), Some(1));
    assert_eq!(parse_session_history_page("/history page 3"), Some(3));
    assert_eq!(parse_session_history_page("/history page 0"), None);
    assert_eq!(parse_session_history_page("history"), None);
}

#[test]
fn normalizes_session_commands_with_bot_mention_prefix() {
    assert_eq!(
        normalize_session_command_text("Codex-Webex-Connector /history", BOT_EMAIL, None),
        "/history"
    );
    assert_eq!(
        normalize_session_command_text("Codex Webex Connector /history page 2", BOT_EMAIL, None),
        "/history page 2"
    );
    assert_eq!(
        normalize_session_command_text("Codex-Webex-Connector /status", BOT_EMAIL, None),
        "/status"
    );
    assert_eq!(
        normalize_session_command_text(
            "Bridge Controller /history",
            BOT_EMAIL,
            Some("Bridge Controller")
        ),
        "/history"
    );
    assert_eq!(
        normalize_session_command_text("hello there", BOT_EMAIL, Some(BOT_DISPLAY_NAME)),
        "hello there"
    );
    assert_eq!(
        normalize_session_command_text("please /status", BOT_EMAIL, Some(BOT_DISPLAY_NAME)),
        "please /status"
    );
}

#[test]
fn abbreviates_utf8_text_on_character_boundaries() {
    let input = "\u{4f60}\u{597d}".repeat(60);
    let abbreviated = abbreviate(&input, 80);
    assert!(abbreviated.ends_with("..."));
    assert_eq!(abbreviated.trim_end_matches("...").chars().count(), 80);
}

#[test]
fn recent_event_dedupe_is_bounded() {
    let mut state = WorkerState::default();
    assert!(state.remember_event("first"));
    assert!(!state.remember_event("first"));

    for index in 0..RECENT_EVENT_ID_LIMIT {
        assert!(state.remember_event(&format!("event-{index}")));
    }

    assert!(state.remember_event("first"));
    assert!(!state.remember_event(&format!("event-{}", RECENT_EVENT_ID_LIMIT - 1)));
}

#[test]
fn derives_repo_name_from_configured_path() {
    let config = AppConfig {
        webex: WebexConfig {
            bot_token: "token".to_string(),
            bot_email: "bot@example.com".to_string(),
            bot_display_name: Some("Test Bot".to_string()),
            control_room_ref: "control".to_string(),
            data_room_ref: "data".to_string(),
            allowed_user_emails: vec!["user@example.com".to_string()],
        },
        bridge: BridgeConfig {
            socket_path: "/tmp/wxcd.sock".into(),
            state_dir: "/tmp".into(),
            session_title_prefix: "WXCD".to_string(),
            approval_policy: "on-request".to_string(),
            sandbox_mode: "workspace-write".to_string(),
            snapshot_interval: 20,
            developer_instructions: "test".to_string(),
            config_path: None,
        },
        repos: vec![RepoConfig {
            name: "codex-webex-connector".to_string(),
            path: "/Users/hoteng/Program/GitHub/codex-webex-connector".into(),
        }],
    };

    assert_eq!(
        repo_name_for_cwd(
            &config,
            "/Users/hoteng/Program/GitHub/codex-webex-connector/subdir"
        ),
        "codex-webex-connector"
    );
    assert_eq!(
        repo_name_for_cwd(&config, "/tmp/random-repo"),
        "random-repo"
    );
}

#[test]
fn extracts_thread_history_turns_from_thread_read() {
    let thread = json!({
        "thread": {
            "turns": [
                {
                    "items": [
                        {
                            "type": "userMessage",
                            "content": [
                                { "type": "text", "text": "first prompt" }
                            ]
                        },
                        {
                            "type": "agentMessage",
                            "phase": "commentary",
                            "text": "working"
                        },
                        {
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "first answer"
                        }
                    ]
                },
                {
                    "items": [
                        {
                            "type": "userMessage",
                            "content": [
                                { "type": "text", "text": "second prompt" }
                            ]
                        },
                        {
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "second answer"
                        }
                    ]
                }
            ]
        }
    });

    let history = extract_thread_history_turns(&thread);
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].user_text, "first prompt");
    assert_eq!(history[0].assistant_text.as_deref(), Some("first answer"));
    assert_eq!(history[1].user_text, "second prompt");
    assert_eq!(history[1].assistant_text.as_deref(), Some("second answer"));
}

#[test]
fn slices_thread_history_pages_newest_first() {
    let turns = vec![
        ImportedHistoryTurn {
            user_text: "turn 1".to_string(),
            assistant_text: Some("answer 1".to_string()),
        },
        ImportedHistoryTurn {
            user_text: "turn 2".to_string(),
            assistant_text: Some("answer 2".to_string()),
        },
        ImportedHistoryTurn {
            user_text: "turn 3".to_string(),
            assistant_text: Some("answer 3".to_string()),
        },
        ImportedHistoryTurn {
            user_text: "turn 4".to_string(),
            assistant_text: Some("answer 4".to_string()),
        },
        ImportedHistoryTurn {
            user_text: "turn 5".to_string(),
            assistant_text: Some("answer 5".to_string()),
        },
    ];

    let page_one = slice_thread_history_page(&turns, 1, 2);
    assert_eq!(page_one.total_turns, 5);
    assert_eq!(page_one.turns.len(), 2);
    assert_eq!(page_one.turns[0].user_text, "turn 4");
    assert_eq!(page_one.turns[1].user_text, "turn 5");

    let page_two = slice_thread_history_page(&turns, 2, 2);
    assert_eq!(page_two.turns.len(), 2);
    assert_eq!(page_two.turns[0].user_text, "turn 2");
    assert_eq!(page_two.turns[1].user_text, "turn 3");

    let page_three = slice_thread_history_page(&turns, 3, 2);
    assert_eq!(page_three.turns.len(), 1);
    assert_eq!(page_three.turns[0].user_text, "turn 1");

    let page_four = slice_thread_history_page(&turns, 4, 2);
    assert!(page_four.turns.is_empty());
    assert_eq!(page_four.total_turns, 5);
}

#[test]
fn failed_sessions_do_not_require_codex_archive() {
    let failed = session_record("ses_failed", SessionState::Failed, false);
    let idle = session_record("ses_idle", SessionState::Idle, false);

    assert!(!session_requires_codex_archive(&failed));
    assert!(session_requires_codex_archive(&idle));
}

#[test]
fn validates_failed_cleanup_targets() {
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_failed", SessionState::Failed, false));
    state.upsert_session(session_record("ses_idle", SessionState::Idle, false));
    state.upsert_session(session_record("ses_archived", SessionState::Archived, true));

    assert!(ensure_failed_cleanup_target(&state, "ses_failed").is_ok());
    assert!(ensure_failed_cleanup_target(&state, "ses_idle").is_err());
    assert!(ensure_failed_cleanup_target(&state, "ses_archived").is_err());
    assert!(ensure_failed_cleanup_target(&state, "ses_missing").is_err());
}

#[test]
fn validates_archived_purge_targets() {
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_archived", SessionState::Archived, true));
    state.upsert_session(session_record("ses_idle", SessionState::Idle, false));

    assert!(validate_purge_archived_session(&state, "ses_archived").is_ok());
    assert!(validate_purge_archived_session(&state, "ses_idle").is_err());
    assert!(validate_purge_archived_session(&state, "ses_missing").is_err());
}

#[test]
fn applies_thread_probe_state_transitions() {
    let idle = session_record("ses_idle", SessionState::Idle, false);
    let missing = ThreadProbe {
        kind: ThreadProbeKind::MissingThread,
        message: "missing".to_string(),
    };
    let updated = apply_thread_probe(&idle, &missing, Utc::now()).unwrap();
    assert_eq!(updated.state, SessionState::Failed);
    assert_eq!(
        updated.failure.as_ref().map(|failure| &failure.kind),
        Some(&SessionFailureKind::MissingThread)
    );

    let readable = ThreadProbe {
        kind: ThreadProbeKind::Readable,
        message: "readable".to_string(),
    };
    let recovered = apply_thread_probe(&updated, &readable, Utc::now()).unwrap();
    assert_eq!(recovered.state, SessionState::Idle);
    assert!(recovered.failure.is_none());

    let unavailable = ThreadProbe {
        kind: ThreadProbeKind::ProbeUnavailable,
        message: "probe unavailable".to_string(),
    };
    let probe_unavailable = apply_thread_probe(&recovered, &unavailable, Utc::now()).unwrap();
    assert_eq!(probe_unavailable.state, SessionState::Failed);
    assert_eq!(
        probe_unavailable
            .failure
            .as_ref()
            .map(|failure| &failure.kind),
        Some(&SessionFailureKind::ProbeUnavailable)
    );

    let archived = session_record("ses_archived", SessionState::Archived, true);
    assert!(apply_thread_probe(&archived, &missing, Utc::now()).is_none());
}

#[test]
fn control_list_filters_non_executable_sessions() {
    let installation_id = "ins_current";
    let mut current = session_record("ses_current", SessionState::Idle, false);
    current.authority = Some(SessionAuthority {
        installation_id: installation_id.to_string(),
    });
    let mut missing = session_record("ses_missing", SessionState::Failed, false);
    missing.authority = Some(SessionAuthority {
        installation_id: installation_id.to_string(),
    });
    let mut foreign = session_record("ses_foreign", SessionState::Idle, false);
    foreign.authority = Some(SessionAuthority {
        installation_id: "ins_other".to_string(),
    });
    let archived = session_record("ses_archived", SessionState::Archived, true);

    assert!(is_default_list_session(&current, installation_id));
    assert!(!is_default_list_session(&missing, installation_id));
    assert!(!is_default_list_session(&foreign, installation_id));
    assert!(!is_default_list_session(&archived, installation_id));
}

#[test]
fn local_mirror_can_claim_legacy_remote_session() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));

    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert(
        "ses_1".to_string(),
        session_record("ses_1", SessionState::Idle, false),
    );

    assert!(state.merge_local_mirror(local_replay, installation_id, Utc::now()));
    let session = state.sessions.get("ses_1").unwrap();
    assert!(session_belongs_to_installation(session, installation_id));
    assert_eq!(
        session
            .authority
            .as_ref()
            .map(|authority| authority.installation_id.as_str()),
        Some(installation_id)
    );
    assert_eq!(
        session
            .local_mirror
            .as_ref()
            .map(|mirror| mirror.installation_id.as_str()),
        Some(installation_id)
    );
}

#[test]
fn local_mirror_does_not_claim_foreign_authority() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    let mut remote = session_record("ses_1", SessionState::Idle, false);
    remote.authority = Some(SessionAuthority {
        installation_id: "ins_other".to_string(),
    });
    state.upsert_session(remote);

    let mut local = session_record("ses_1", SessionState::Idle, false);
    local.authority = Some(SessionAuthority {
        installation_id: "ins_other".to_string(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(!state.merge_local_mirror(local_replay, installation_id, Utc::now()));
    let session = state.sessions.get("ses_1").unwrap();
    assert!(!session_belongs_to_installation(session, installation_id));
}

#[tokio::test]
async fn installation_identity_persists_and_loads() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();

    let created = load_or_create_installation_identity(&state_dir)
        .await
        .unwrap();
    let loaded = load_or_create_installation_identity(&state_dir)
        .await
        .unwrap();

    assert_eq!(created, loaded);
    assert!(created.installation_id.starts_with("ins_"));

    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[test]
fn remove_session_cleans_indexes_and_approvals() {
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Archived, true));
    state.pending_approvals.insert(
        "apr_1".to_string(),
        wxcd_proto::PendingApproval {
            approval_id: "apr_1".to_string(),
            session_id: "ses_1".to_string(),
            thread_id: "thread-ses_1".to_string(),
            turn_id: "turn".to_string(),
            codex_request_id: json!(1),
            item_id: "item".to_string(),
            kind: wxcd_proto::ApprovalKind::CommandExecution,
            reason: None,
            command: None,
            cwd: None,
            requested_permissions: None,
            card_message_id: None,
            requested_at: Utc::now(),
        },
    );

    let removed = state.remove_session("ses_1").unwrap();
    assert_eq!(removed.session_id, "ses_1");
    assert!(state.sessions.is_empty());
    assert!(state.room_to_session.is_empty());
    assert!(state.thread_to_session.is_empty());
    assert!(state.pending_approvals.is_empty());
}

fn session_record(session_id: &str, state: SessionState, archived: bool) -> SessionRecord {
    let failure = (state == SessionState::Failed).then(|| SessionFailure {
        kind: SessionFailureKind::MissingThread,
        message: "missing".to_string(),
        detected_at: Utc::now(),
    });
    SessionRecord {
        session_id: session_id.to_string(),
        title: format!("WXCD {session_id} repo"),
        repo_name: "repo".to_string(),
        repo_path: "/tmp/repo".to_string(),
        owner_email: "user@example.com".to_string(),
        session_room_id: format!("room-{session_id}"),
        session_room_web_link: None,
        thread_id: format!("thread-{session_id}"),
        overview_message_id: None,
        state,
        last_checkpoint: None,
        last_final: None,
        active_turn_id: None,
        active_turn_buffer: String::new(),
        updated_at: Utc::now(),
        archived,
        failure,
        authority: None,
        local_mirror: None,
    }
}
