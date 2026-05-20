use super::{
    ASYNC_NOTIFICATION_INGRESS_ACK_TIMEOUT, C5_DELIVERY_ACCEPTANCE_WINDOW_SECONDS,
    CleanupFailedCommand, CodexConnectionConfig, DiagnoseCommand, LifecycleAdmissionPhase,
    LifecycleCommand, LifecycleCommandResponse, LifecycleControl, LifecycleTransitionToken,
    ListCommand, ListMode, LocalMirrorClaimScope, PLUGIN_DELIVERY_BROKER_REQUEST_TIMEOUT,
    PurgeArchivedCommand, QueuedLifecycleCommand, RECENT_EVENT_ID_LIMIT,
    SIDECAR_DEFERRED_INGRESS_DIR, SIDECAR_DRAIN_STATE_DIR, ThreadProbe, ThreadProbeKind,
    WorkerQueueItem, WorkerState, abbreviate, apply_thread_probe, attached_session_for_thread,
    build_async_notification_delivery_request, durable_local_snapshot_path,
    ensure_approval_belongs_to_installation, ensure_failed_cleanup_target,
    ensure_session_id_belongs_to_installation, extract_thread_history_turns,
    generate_installation_id, handle_lifecycle_command, ingress_requires_processing_ack,
    ingress_uses_delivery_enqueue, initial_lifecycle_phase_from_env, is_control_list_session,
    is_default_list_session, is_failed_session_room_command, lifecycle_command_response,
    lifecycle_control_socket_path_from_env, lifecycle_runtime_in_flight_total,
    lifecycle_sidecar_in_flight_count, load_durable_local_snapshot_with_metadata,
    load_local_snapshot_with_metadata, load_or_create_installation_identity,
    normalize_control_command_text, normalize_session_command_text, parse_attach_session_id,
    parse_cleanup_failed_command, parse_diagnose_command, parse_list_command,
    parse_purge_archived_command, parse_resume_local_thread_id, parse_session_history_page,
    remove_stale_lifecycle_socket, repo_name_for_cwd, resolve_codex_connection,
    resolve_delivery_broker_connection, session_belongs_to_installation,
    session_requires_codex_archive, sessions_for_diagnostics,
    should_process_async_notification_event, sidecar_drain_in_flight_count,
    sidecar_drain_in_flight_count_after, sidecar_drain_state_file_prefix,
    sidecar_received_before_cutoff, slice_thread_history_page, stable_fnv1a_hex,
    startup_snapshot_persist_now, validate_purge_archived_session,
    wait_for_lifecycle_response_flush, webex_delivery_idempotency_key, worker_active_check_ack,
    worker_ingress_socket_path_for, worker_ingress_socket_path_from_env,
    write_supervisor_shutdown_marker_at,
};
use chrono::{Duration, TimeZone, Utc};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wxcd_cbth_rpc::{
    PluginDrainRequest, PluginHealthCheckRequest, PluginRpcError, PluginRpcErrorKind,
    PluginShutdownRequest,
};
use wxcd_proto::{
    AppConfig, BridgeConfig, BridgeSnapshot, CbthPluginConfig, DiagnosticsConfig,
    LocalSessionMirror, RepoConfig, SessionAuthority, SessionFailure, SessionFailureKind,
    SessionRecord, SessionState, WebexAsyncNotificationEvent, WebexConfig, WebexIngressEnvelope,
    WebexMessageEvent,
};
use wxcd_render::ImportedHistoryTurn;

const BOT_EMAIL: &str = "codex-webex-connector@webex.bot";
const BOT_DISPLAY_NAME: &str = "Codex Webex Connector";

fn lifecycle_drain_command(reason: &str) -> LifecycleCommand {
    LifecycleCommand::Drain {
        request: PluginDrainRequest {
            reason: reason.to_string(),
        },
        token: LifecycleTransitionToken::for_generation(0),
    }
}

fn app_config_with_plugin(enabled: bool) -> AppConfig {
    AppConfig {
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
            cbth_plugin: CbthPluginConfig {
                enabled,
                socket_path: if enabled {
                    Some("/tmp/cbth.sock".into())
                } else {
                    None
                },
                plugin_home: "/tmp/plugin".into(),
                plugin_instance_id: if enabled {
                    "instance-1".to_string()
                } else {
                    "standalone".to_string()
                },
                plugin_release_id: "0.1.0".to_string(),
                manifest_path: "plugin/manifest.json".into(),
            },
        },
        repos: vec![RepoConfig {
            name: "codex-webex-connector".to_string(),
            path: "/Users/hoteng/Program/GitHub/codex-webex-connector".into(),
        }],
    }
}

fn app_config_with_state_dir(state_dir: &Path, plugin_enabled: bool) -> AppConfig {
    let mut config = app_config_with_plugin(plugin_enabled);
    config.bridge.state_dir = state_dir.to_path_buf();
    if plugin_enabled {
        config.bridge.cbth_plugin.plugin_home = state_dir.join("plugin-home");
    }
    config
}

#[test]
fn worker_ingress_socket_env_override_wins() {
    let config = app_config_with_plugin(true);

    assert_eq!(
        worker_ingress_socket_path_from_env(
            &config,
            Some(std::ffi::OsStr::new("/tmp/custom-wxcd.sock")),
        ),
        PathBuf::from("/tmp/custom-wxcd.sock")
    );
}

#[test]
fn plugin_worker_ingress_socket_is_release_scoped() {
    let mut config = app_config_with_plugin(true);
    config.bridge.state_dir = "/tmp/wxcd-state".into();

    let socket_path = worker_ingress_socket_path_from_env(&config, Some(std::ffi::OsStr::new("")));

    assert_eq!(
        socket_path,
        PathBuf::from("/tmp").join(format!(
            "wxcd-ingress-{}.sock",
            stable_fnv1a_hex("instance-1\n0.1.0\n/tmp/wxcd-state")
        ))
    );
    assert_ne!(socket_path, config.bridge.socket_path);
}

#[test]
fn standalone_worker_ingress_socket_uses_bridge_socket_path() {
    let config = app_config_with_plugin(false);

    assert_eq!(
        worker_ingress_socket_path_for(
            &config.bridge.socket_path,
            &config.bridge.state_dir,
            &config.bridge.cbth_plugin,
        ),
        config.bridge.socket_path
    );
}

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
fn failed_session_rooms_accept_only_recovery_commands() {
    assert!(is_failed_session_room_command("help"));
    assert!(is_failed_session_room_command("/help"));
    assert!(is_failed_session_room_command("/status"));
    assert!(is_failed_session_room_command("/history"));
    assert!(is_failed_session_room_command("/history page 2"));
    assert!(is_failed_session_room_command("/resume"));
    assert!(!is_failed_session_room_command("/pause"));
    assert!(!is_failed_session_room_command("continue the task"));
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
    state.forget_event("first");
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
    let config = app_config_with_plugin(false);

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
fn codex_connection_defaults_to_standalone() {
    let config = app_config_with_plugin(false);

    let connection = resolve_codex_connection(&config, None).expect("connection");

    assert_eq!(connection, CodexConnectionConfig::Standalone);
}

#[test]
fn standalone_connection_ignores_managed_app_server_url() {
    let config = app_config_with_plugin(false);

    let connection = resolve_codex_connection(&config, Some("ws://127.0.0.1:1234".to_string()))
        .expect("connection");

    assert_eq!(connection, CodexConnectionConfig::Standalone);
}

#[test]
fn codex_connection_uses_supervisor_managed_app_server_url() {
    let config = app_config_with_plugin(true);

    let connection = resolve_codex_connection(&config, Some("ws://127.0.0.1:1234".to_string()))
        .expect("connection");

    assert_eq!(
        connection,
        CodexConnectionConfig::ManagedAppServer {
            url: "ws://127.0.0.1:1234".to_string()
        }
    );
}

#[test]
fn cbth_plugin_mode_requires_supervisor_managed_app_server_url() {
    let config = app_config_with_plugin(true);

    let error = resolve_codex_connection(&config, None).expect_err("missing managed URL");

    assert!(
        format!("{error:#}").contains("WXCD_CODEX_APP_SERVER_URL"),
        "{error:#}"
    );
}

#[test]
fn delivery_broker_defaults_to_none_for_standalone_mode() {
    let config = app_config_with_plugin(false);

    let broker = resolve_delivery_broker_connection(
        &config,
        Some("/tmp/wxcd-delivery-broker.sock".to_string()),
    )
    .expect("broker");

    assert_eq!(broker, None);
}

#[test]
fn cbth_plugin_mode_allows_missing_delivery_broker_socket() {
    let config = app_config_with_plugin(true);

    let broker = resolve_delivery_broker_connection(&config, None).expect("broker");

    assert_eq!(broker, None);
}

#[test]
fn async_notification_builds_delivery_owned_enqueue_request() {
    let mut state = WorkerState::default();
    let session = session_record("ses_1", SessionState::Idle, false);
    state.upsert_session(session);
    let notification = WebexAsyncNotificationEvent {
        event_id: "event-1".to_string(),
        session_id: Some("ses_1".to_string()),
        thread_id: None,
        summary: "background job finished".to_string(),
        payload: Some(json!({"result": "done"})),
        created: Utc::now(),
    };

    let request =
        build_async_notification_delivery_request(&state, &notification).expect("request");

    assert_eq!(request.source_thread_id, "thread-ses_1");
    assert_eq!(request.summary, "background job finished");
    assert_eq!(
        request.idempotency_key,
        webex_delivery_idempotency_key("event-1")
    );
    assert!(is_c5_ascii_token(&request.idempotency_key));
    assert_eq!(request.target.driver, "codex_app_server");
    assert_eq!(request.target.app_server_lease_id, None);
    assert_eq!(request.target.codex_binary, None);
    assert_eq!(request.max_delivery_attempts, Some(3));
    assert_eq!(request.redelivery_window_seconds, Some(3600));
    assert_eq!(
        request
            .inline_payload
            .as_ref()
            .expect("payload")
            .pointer("/payload/result")
            .and_then(serde_json::Value::as_str),
        Some("done")
    );
}

#[test]
fn async_notification_rejects_non_executable_session_id() {
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_archived", SessionState::Archived, true));
    let notification = WebexAsyncNotificationEvent {
        event_id: "event-archived".to_string(),
        session_id: Some("ses_archived".to_string()),
        thread_id: None,
        summary: "background job finished".to_string(),
        payload: None,
        created: Utc::now(),
    };

    let error = build_async_notification_delivery_request(&state, &notification)
        .expect_err("archived session should not be executable");

    assert!(
        format!("{error:#}").contains("not executable by this worker"),
        "{error:#}"
    );
}

#[test]
fn async_notification_rejects_empty_event_id() {
    let mut state = WorkerState::default();
    let session = session_record("ses_1", SessionState::Idle, false);
    state.upsert_session(session);
    let notification = WebexAsyncNotificationEvent {
        event_id: "   ".to_string(),
        session_id: Some("ses_1".to_string()),
        thread_id: None,
        summary: "background job finished".to_string(),
        payload: None,
        created: Utc::now(),
    };

    let error = build_async_notification_delivery_request(&state, &notification)
        .expect_err("empty event id should fail");

    assert!(
        format!("{error:#}").contains("event_id must not be empty"),
        "{error:#}"
    );
}

#[test]
fn async_notification_idempotency_key_is_c5_token_safe() {
    let key = webex_delivery_idempotency_key("webex/event:1 with spaces");

    assert!(key.starts_with("webex-delivery-"));
    assert!(is_c5_ascii_token(&key));
    assert_eq!(
        key,
        webex_delivery_idempotency_key("webex/event:1 with spaces")
    );
}

#[test]
fn async_notification_event_is_remembered_only_after_enqueue_success() {
    let mut state = WorkerState::default();
    let event_id = "event-retry";

    assert!(should_process_async_notification_event(&state, event_id));
    assert!(should_process_async_notification_event(&state, event_id));

    state.remember_event(event_id);

    assert!(!should_process_async_notification_event(&state, event_id));
}

#[test]
fn async_notification_timeouts_cover_c5_acceptance_window() {
    assert!(
        PLUGIN_DELIVERY_BROKER_REQUEST_TIMEOUT
            >= std::time::Duration::from_secs(C5_DELIVERY_ACCEPTANCE_WINDOW_SECONDS + 20)
    );
    assert!(
        ASYNC_NOTIFICATION_INGRESS_ACK_TIMEOUT
            >= PLUGIN_DELIVERY_BROKER_REQUEST_TIMEOUT + std::time::Duration::from_secs(10)
    );
}

fn is_c5_ascii_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[test]
fn normal_user_message_ingress_does_not_use_delivery_enqueue() {
    let message = WebexIngressEnvelope::MessageCreated(WebexMessageEvent {
        event_id: "event-message".to_string(),
        room_id: "room-ses_1".to_string(),
        message_id: "message-1".to_string(),
        person_email: "user@example.com".to_string(),
        text: "normal user turn".to_string(),
        created: Utc::now(),
        sidecar_received_at: None,
        processing_ack: false,
    });
    let replayed_message = WebexIngressEnvelope::MessageCreated(WebexMessageEvent {
        event_id: "event-replayed-message".to_string(),
        room_id: "room-ses_1".to_string(),
        message_id: "message-2".to_string(),
        person_email: "user@example.com".to_string(),
        text: "replayed user turn".to_string(),
        created: Utc::now(),
        sidecar_received_at: None,
        processing_ack: true,
    });
    let async_notification = WebexIngressEnvelope::AsyncNotification(WebexAsyncNotificationEvent {
        event_id: "event-async".to_string(),
        session_id: Some("ses_1".to_string()),
        thread_id: None,
        summary: "background job finished".to_string(),
        payload: None,
        created: Utc::now(),
    });

    assert!(!ingress_uses_delivery_enqueue(&message));
    assert!(!ingress_uses_delivery_enqueue(&replayed_message));
    assert!(ingress_uses_delivery_enqueue(&async_notification));
    assert!(!ingress_requires_processing_ack(&message));
    assert!(ingress_requires_processing_ack(&replayed_message));
    assert!(ingress_requires_processing_ack(&async_notification));
}

#[test]
fn doctor_report_describes_standalone_mode_without_credentials() {
    let diagnostics = DiagnosticsConfig {
        bridge: BridgeConfig {
            socket_path: "/tmp/wxcd.sock".into(),
            state_dir: "/tmp/wxcd-state".into(),
            session_title_prefix: "WXCD".to_string(),
            approval_policy: "on-request".to_string(),
            sandbox_mode: "workspace-write".to_string(),
            snapshot_interval: 20,
            developer_instructions: "test".to_string(),
            config_path: None,
            cbth_plugin: CbthPluginConfig {
                enabled: false,
                socket_path: None,
                plugin_home: "/tmp/plugin".into(),
                plugin_instance_id: "standalone".to_string(),
                plugin_release_id: "0.1.0".to_string(),
                manifest_path: "plugin/manifest.json".into(),
            },
        },
        repos: vec![RepoConfig {
            name: "repo".to_string(),
            path: "/tmp/repo".into(),
        }],
        missing_webex_env: vec!["WEBEX_BOT_TOKEN"],
    };

    let report = super::render_doctor_report(
        &diagnostics,
        &super::ManifestStatus::Valid,
        &super::RpcStatus::Disabled,
    );

    assert!(report.contains("mode: standalone"));
    assert!(report.contains("plugin_rpc: disabled"));
    assert!(report.contains("webex_credentials: missing WEBEX_BOT_TOKEN"));
}

#[test]
fn plugin_manifest_validates_packaging_metadata() {
    let manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("plugin/manifest.json");
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&manifest_path).expect("read plugin manifest"),
    )
    .expect("decode plugin manifest");
    let config = CbthPluginConfig {
        enabled: false,
        socket_path: None,
        plugin_home: "/tmp/plugin".into(),
        plugin_instance_id: "standalone".to_string(),
        plugin_release_id: "0.1.0".to_string(),
        manifest_path,
    };

    assert_eq!(
        super::validate_plugin_manifest(&config),
        super::ManifestStatus::Valid
    );
    assert_eq!(
        manifest
            .pointer("/entrypoint/binary")
            .and_then(|value| value.as_str()),
        Some("../bin/wxcd-supervisor")
    );
    assert_eq!(
        manifest
            .pointer("/diagnostics/command/0")
            .and_then(|value| value.as_str()),
        Some("../bin/wxcd-worker")
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
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_failed",
        SessionState::Failed,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_idle",
        SessionState::Idle,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_archived",
        SessionState::Archived,
        true,
        installation_id,
    ));

    assert!(ensure_failed_cleanup_target(&state, "ses_failed", installation_id).is_ok());
    assert!(ensure_failed_cleanup_target(&state, "ses_idle", installation_id).is_err());
    assert!(ensure_failed_cleanup_target(&state, "ses_archived", installation_id).is_err());
    assert!(ensure_failed_cleanup_target(&state, "ses_missing", installation_id).is_err());
    assert!(ensure_failed_cleanup_target(&state, "ses_failed", "ins_other").is_err());
}

#[test]
fn validates_archived_purge_targets() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_archived",
        SessionState::Archived,
        true,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_idle",
        SessionState::Idle,
        false,
        installation_id,
    ));

    assert!(validate_purge_archived_session(&state, "ses_archived", installation_id).is_ok());
    assert!(validate_purge_archived_session(&state, "ses_idle", installation_id).is_err());
    assert!(validate_purge_archived_session(&state, "ses_missing", installation_id).is_err());
    assert!(validate_purge_archived_session(&state, "ses_archived", "ins_other").is_err());
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
    let mut archived = session_record("ses_archived", SessionState::Archived, true);
    archived.authority = Some(SessionAuthority {
        installation_id: installation_id.to_string(),
    });

    assert!(is_default_list_session(&current, installation_id));
    assert!(!is_default_list_session(&missing, installation_id));
    assert!(!is_default_list_session(&foreign, installation_id));
    assert!(!is_default_list_session(&archived, installation_id));
    assert!(is_control_list_session(&archived, installation_id, true));
    assert!(!is_control_list_session(&missing, installation_id, true));
    assert!(!is_control_list_session(&foreign, installation_id, true));
}

#[test]
fn current_writer_local_mirror_can_claim_current_session() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));

    let mut local = session_record("ses_1", SessionState::Idle, false);
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));
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
fn local_mirror_preserves_snapshot_only_current_session_during_claim_merge() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.set_executable_installation(installation_id);
    state.upsert_session(session_record("ses_remote", SessionState::Idle, false));

    let mut local_remote = session_record("ses_remote", SessionState::Idle, false);
    local_remote.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_only = session_record("ses_local", SessionState::Idle, false);
    local_only.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay
        .sessions
        .insert("ses_remote".to_string(), local_remote);
    local_replay
        .sessions
        .insert("ses_local".to_string(), local_only);

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));

    let remote = state.sessions.get("ses_remote").unwrap();
    assert!(session_belongs_to_installation(remote, installation_id));
    let local = state.sessions.get("ses_local").unwrap();
    assert!(session_belongs_to_installation(local, installation_id));
    assert_eq!(
        state
            .room_to_session
            .get("room-ses_local")
            .map(String::as_str),
        Some("ses_local")
    );
    assert_eq!(
        state
            .thread_to_session
            .get("thread-ses_local")
            .map(String::as_str),
        Some("ses_local")
    );
}

#[test]
fn local_mirror_does_not_resurrect_purged_snapshot_only_session() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state
        .remote_purged_session_ids
        .insert("ses_local".to_string());

    let mut local = managed_session_record("ses_local", SessionState::Idle, false, installation_id);
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_local".to_string(), local);

    assert!(!state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));
    assert!(!state.sessions.contains_key("ses_local"));
}

#[test]
fn local_mirror_skips_snapshot_only_sessions_older_than_remote_snapshot() {
    let installation_id = "ins_current";
    let base_time = Utc::now();
    let mut state = WorkerState {
        remote_snapshot_created_at: Some(base_time + Duration::seconds(2)),
        ..WorkerState::default()
    };

    let mut local = managed_session_record("ses_local", SessionState::Idle, false, installation_id);
    local.updated_at = base_time;
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: base_time,
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_local".to_string(), local);

    assert!(!state.merge_local_mirror(
        local_replay,
        installation_id,
        base_time + Duration::seconds(3),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));
    assert!(!state.sessions.contains_key("ses_local"));
}

#[test]
fn local_mirror_preserves_newer_same_session_snapshot_state() {
    let installation_id = "ins_current";
    let base_time = Utc::now();
    let mut state = WorkerState::default();
    let mut remote = managed_session_record("ses_1", SessionState::Running, false, installation_id);
    remote.updated_at = base_time;
    remote.last_checkpoint = Some("remote checkpoint".to_string());
    state.upsert_session(remote);

    let mut local = managed_session_record(
        "ses_1",
        SessionState::WaitingApproval,
        false,
        installation_id,
    );
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: base_time + Duration::seconds(1),
    });
    local.updated_at = base_time + Duration::seconds(1);
    local.last_checkpoint = Some("local checkpoint".to_string());
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        base_time + Duration::seconds(2),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));

    let session = state.sessions.get("ses_1").unwrap();
    assert_eq!(session.state, SessionState::WaitingApproval);
    assert_eq!(session.last_checkpoint.as_deref(), Some("local checkpoint"));
    assert!(session_belongs_to_installation(session, installation_id));
}

#[test]
fn local_mirror_does_not_undo_remote_archive() {
    let installation_id = "ins_current";
    let base_time = Utc::now();
    let mut state = WorkerState::default();
    let mut remote = managed_session_record("ses_1", SessionState::Archived, true, installation_id);
    remote.updated_at = base_time;
    state.upsert_session(remote);
    state
        .remote_archived_session_ids
        .insert("ses_1".to_string());

    let mut local = managed_session_record("ses_1", SessionState::Running, false, installation_id);
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: base_time + Duration::seconds(1),
    });
    local.updated_at = base_time + Duration::seconds(1);
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        base_time + Duration::seconds(2),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));

    let session = state.sessions.get("ses_1").unwrap();
    assert_eq!(session.state, SessionState::Archived);
    assert!(session.archived);
}

#[test]
fn local_mirror_does_not_regress_newer_remote_session_state() {
    let installation_id = "ins_current";
    let base_time = Utc::now();
    let mut state = WorkerState::default();
    let mut remote =
        managed_session_record("ses_1", SessionState::Completed, false, installation_id);
    remote.updated_at = base_time + Duration::seconds(2);
    remote.last_final = Some("remote final".to_string());
    state.upsert_session(remote);

    let mut local = managed_session_record("ses_1", SessionState::Running, false, installation_id);
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: base_time,
    });
    local.updated_at = base_time;
    local.last_checkpoint = Some("local checkpoint".to_string());
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        base_time + Duration::seconds(3),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));

    let session = state.sessions.get("ses_1").unwrap();
    assert_eq!(session.state, SessionState::Completed);
    assert_eq!(session.last_final.as_deref(), Some("remote final"));
    assert_eq!(
        session
            .local_mirror
            .as_ref()
            .map(|mirror| mirror.installation_id.as_str()),
        Some(installation_id)
    );
}

#[test]
fn local_mirror_restores_current_pending_approvals() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_1",
        SessionState::WaitingApproval,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_foreign",
        SessionState::WaitingApproval,
        false,
        "ins_other",
    ));

    let mut local = managed_session_record(
        "ses_1",
        SessionState::WaitingApproval,
        false,
        installation_id,
    );
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);
    local_replay.pending_approvals.insert(
        "apr_current".to_string(),
        pending_approval("apr_current", "ses_1"),
    );
    local_replay.pending_approvals.insert(
        "apr_foreign".to_string(),
        pending_approval("apr_foreign", "ses_foreign"),
    );

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));

    assert!(state.pending_approvals.contains_key("apr_current"));
    assert!(!state.pending_approvals.contains_key("apr_foreign"));
}

#[test]
fn local_mirror_does_not_readd_remote_resolved_or_snapshot_stale_approvals() {
    let installation_id = "ins_current";
    let base_time = Utc::now();
    let mut state = WorkerState {
        remote_snapshot_created_at: Some(base_time + Duration::seconds(2)),
        ..WorkerState::default()
    };
    state
        .remote_resolved_approval_ids
        .insert("apr_resolved".to_string());
    state.upsert_session(managed_session_record(
        "ses_1",
        SessionState::WaitingApproval,
        false,
        installation_id,
    ));

    let mut local = managed_session_record(
        "ses_1",
        SessionState::WaitingApproval,
        false,
        installation_id,
    );
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: base_time + Duration::seconds(3),
    });
    local.updated_at = base_time + Duration::seconds(3);
    let mut resolved = pending_approval("apr_resolved", "ses_1");
    resolved.requested_at = base_time + Duration::seconds(3);
    let mut stale = pending_approval("apr_stale", "ses_1");
    stale.requested_at = base_time;
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);
    local_replay
        .pending_approvals
        .insert("apr_resolved".to_string(), resolved);
    local_replay
        .pending_approvals
        .insert("apr_stale".to_string(), stale);

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        base_time + Duration::seconds(4),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));

    assert!(!state.pending_approvals.contains_key("apr_resolved"));
    assert!(!state.pending_approvals.contains_key("apr_stale"));
}

#[test]
fn current_writer_scope_retries_authorityless_listed_threads() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));

    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert(
        "ses_1".to_string(),
        session_record("ses_1", SessionState::Idle, false),
    );
    let local_thread_ids = std::iter::once("thread-ses_1".to_string()).collect();

    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshotOrListedThreads(&local_thread_ids)
    ));
    assert!(session_belongs_to_installation(
        state.sessions.get("ses_1").unwrap(),
        installation_id
    ));
}

#[test]
fn current_writer_local_mirror_does_not_claim_authorityless_legacy_session() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));

    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert(
        "ses_1".to_string(),
        session_record("ses_1", SessionState::Idle, false),
    );

    assert!(!state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));
    let session = state.sessions.get("ses_1").unwrap();
    assert!(!session_belongs_to_installation(session, installation_id));
    assert!(session.authority.is_none());
    assert!(session.local_mirror.is_none());
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
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(!state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));
    let session = state.sessions.get("ses_1").unwrap();
    assert!(!session_belongs_to_installation(session, installation_id));
    assert!(session.local_mirror.is_none());
}

#[test]
fn stale_local_mirror_does_not_claim_legacy_remote_session() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));

    let mut local = session_record("ses_1", SessionState::Idle, false);
    local.authority = Some(SessionAuthority {
        installation_id: "ins_other".to_string(),
    });
    local.local_mirror = Some(LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });
    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert("ses_1".to_string(), local);

    assert!(!state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::CurrentWriterSnapshot
    ));
    let session = state.sessions.get("ses_1").unwrap();
    assert!(!session_belongs_to_installation(session, installation_id));
    assert!(session.authority.is_none());
    assert!(session.local_mirror.is_none());
}

#[test]
fn legacy_local_mirror_claim_requires_listed_thread() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));

    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert(
        "ses_1".to_string(),
        session_record("ses_1", SessionState::Idle, false),
    );
    let local_thread_ids = std::collections::HashSet::new();

    assert!(!state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::ListedThreads(&local_thread_ids)
    ));
    assert!(!session_belongs_to_installation(
        state.sessions.get("ses_1").unwrap(),
        installation_id
    ));

    let mut local_replay = wxcd_eventlog::ReplayState::default();
    local_replay.sessions.insert(
        "ses_1".to_string(),
        session_record("ses_1", SessionState::Idle, false),
    );
    let local_thread_ids = std::iter::once("thread-ses_1".to_string()).collect();
    assert!(state.merge_local_mirror(
        local_replay,
        installation_id,
        Utc::now(),
        LocalMirrorClaimScope::ListedThreads(&local_thread_ids)
    ));
    assert!(session_belongs_to_installation(
        state.sessions.get("ses_1").unwrap(),
        installation_id
    ));
}

#[test]
fn foreign_authority_takes_precedence_over_stale_local_mirror() {
    let installation_id = "ins_current";
    let mut session = managed_session_record("ses_1", SessionState::Idle, false, "ins_other");
    session.local_mirror = Some(wxcd_proto::LocalSessionMirror {
        installation_id: installation_id.to_string(),
        mirrored_at: Utc::now(),
    });

    assert!(!session_belongs_to_installation(&session, installation_id));
}

#[test]
fn legacy_local_snapshot_sessions_are_claimed_on_fallback() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));
    state.set_executable_installation(installation_id);
    let local_thread_ids = std::iter::once("thread-ses_1".to_string()).collect();

    assert!(state.thread_to_session.is_empty());
    assert!(state.claim_legacy_local_sessions(installation_id, Utc::now(), &local_thread_ids));

    let session = state.sessions.get("ses_1").unwrap();
    assert!(session_belongs_to_installation(session, installation_id));
    assert_eq!(
        state.thread_to_session.get(&session.thread_id),
        Some(&session.session_id)
    );
}

#[test]
fn legacy_local_snapshot_claim_requires_listed_thread() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_1", SessionState::Idle, false));
    state.set_executable_installation(installation_id);
    let local_thread_ids = std::collections::HashSet::new();

    assert!(!state.claim_legacy_local_sessions(installation_id, Utc::now(), &local_thread_ids));

    let session = state.sessions.get("ses_1").unwrap();
    assert!(!session_belongs_to_installation(session, installation_id));
    assert!(state.thread_to_session.is_empty());
}

#[test]
fn legacy_local_snapshot_claims_archived_session_without_indexing() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(session_record("ses_archived", SessionState::Archived, true));
    state.set_executable_installation(installation_id);
    let local_thread_ids = std::iter::once("thread-ses_archived".to_string()).collect();

    assert!(state.claim_legacy_local_sessions(installation_id, Utc::now(), &local_thread_ids));

    let session = state.sessions.get("ses_archived").unwrap();
    assert!(session_belongs_to_installation(session, installation_id));
    assert!(validate_purge_archived_session(&state, "ses_archived", installation_id).is_ok());
    assert!(state.room_to_session.is_empty());
    assert!(state.thread_to_session.is_empty());
}

#[test]
fn local_snapshot_records_writer_installation() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.set_executable_installation(installation_id);

    let snapshot = state.to_snapshot();

    assert_eq!(
        snapshot.writer_installation_id.as_deref(),
        Some(installation_id)
    );
}

#[tokio::test]
async fn local_snapshot_writer_metadata_round_trips() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-snapshot-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let snapshot_path = state_dir.join("bridge-state.json");
    let snapshot = BridgeSnapshot {
        created_at: Utc::now(),
        writer_installation_id: Some("ins_current".to_string()),
        sessions: Vec::new(),
        pending_approvals: Vec::new(),
    };
    tokio::fs::write(&snapshot_path, serde_json::to_string(&snapshot).unwrap())
        .await
        .unwrap();

    let local_snapshot = load_local_snapshot_with_metadata(&snapshot_path)
        .await
        .unwrap();

    assert!(local_snapshot.metadata.existed);
    assert_eq!(
        local_snapshot.metadata.writer_installation_id.as_deref(),
        Some("ins_current")
    );
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[test]
fn durable_snapshot_uses_plugin_home_in_cbth_plugin_mode() {
    let mut config = app_config_with_plugin(true);
    config.bridge.state_dir = "/tmp/wxcd-state".into();
    config.bridge.cbth_plugin.plugin_home = "/tmp/wxcd-plugin-home".into();

    assert_eq!(
        durable_local_snapshot_path(&config),
        std::path::PathBuf::from("/tmp/wxcd-plugin-home/bridge-state.json")
    );
}

#[test]
fn lifecycle_socket_empty_env_falls_back_to_release_scoped_path() {
    let mut config = app_config_with_plugin(true);
    config.bridge.cbth_plugin.plugin_home = "/tmp/wxcd-plugin-home".into();
    let mut next_release = config.clone();
    next_release.bridge.cbth_plugin.plugin_release_id = "0.2.0".to_string();

    let socket_path =
        lifecycle_control_socket_path_from_env(&config, Some(std::ffi::OsStr::new("")))
            .expect("lifecycle socket path");
    let next_socket_path =
        lifecycle_control_socket_path_from_env(&next_release, Some(std::ffi::OsStr::new("")))
            .expect("next lifecycle socket path");

    assert_eq!(
        socket_path,
        std::path::PathBuf::from("/tmp").join(format!(
            "wxcd-lifecycle-{}.sock",
            stable_fnv1a_hex("instance-1\n0.1.0\n/tmp/wxcd-plugin-home")
        ))
    );
    assert_ne!(socket_path, next_socket_path);
    assert!(socket_path.as_os_str().len() < 108);
}

#[test]
fn pre_active_startup_defers_snapshot_persistence() {
    let mut pending = false;

    assert!(!startup_snapshot_persist_now(false, true, &mut pending));
    assert!(!pending);

    assert!(startup_snapshot_persist_now(true, false, &mut pending));
    assert!(!pending);

    assert!(!startup_snapshot_persist_now(true, true, &mut pending));
    assert!(pending);
}

#[tokio::test]
async fn durable_snapshot_loader_falls_back_to_legacy_state_dir_snapshot() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-snapshot-fallback-test-{}",
        generate_installation_id(Utc::now())
    ));
    let plugin_home = state_dir.join("plugin-home");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let snapshot_path = state_dir.join("bridge-state.json");
    let snapshot = BridgeSnapshot {
        created_at: Utc::now(),
        writer_installation_id: Some("ins_legacy".to_string()),
        sessions: Vec::new(),
        pending_approvals: Vec::new(),
    };
    tokio::fs::write(&snapshot_path, serde_json::to_string(&snapshot).unwrap())
        .await
        .unwrap();
    let mut config = app_config_with_plugin(true);
    config.bridge.state_dir = state_dir.clone();
    config.bridge.cbth_plugin.plugin_home = plugin_home;

    let loaded = load_durable_local_snapshot_with_metadata(&config)
        .await
        .unwrap();

    assert!(loaded.metadata.existed);
    assert_eq!(
        loaded.metadata.writer_installation_id.as_deref(),
        Some("ins_legacy")
    );
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn lifecycle_drain_persists_plugin_home_snapshot_before_reporting_complete() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-lifecycle-drain-test-{}",
        generate_installation_id(Utc::now())
    ));
    let plugin_home = state_dir.join("plugin-home");
    let mut config = app_config_with_plugin(true);
    config.bridge.state_dir = state_dir.clone();
    config.bridge.cbth_plugin.plugin_home = plugin_home.clone();
    let mut state = WorkerState::default();
    state.set_executable_installation("ins_current");
    state.upsert_session(managed_session_record(
        "ses_current",
        SessionState::Idle,
        false,
        "ins_current",
    ));

    let response =
        handle_lifecycle_command(&config, &mut state, lifecycle_drain_command("upgrade"))
            .await
            .unwrap();

    assert!(matches!(
        response,
        LifecycleCommandResponse::Drain(response) if response.drained
    ));
    let snapshot_path = plugin_home.join("bridge-state.json");
    let loaded = load_local_snapshot_with_metadata(&snapshot_path)
        .await
        .unwrap();
    assert!(loaded.metadata.existed);
    assert_eq!(
        loaded.metadata.writer_installation_id.as_deref(),
        Some("ins_current")
    );
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[test]
fn lifecycle_codex_in_flight_count_tracks_current_executable_work() {
    let mut state = WorkerState::default();
    state.set_executable_installation("ins_current");
    state.upsert_session(managed_session_record(
        "ses_idle",
        SessionState::Idle,
        false,
        "ins_current",
    ));
    let mut running =
        managed_session_record("ses_running", SessionState::Running, false, "ins_current");
    running.active_turn_id = Some("turn_running".to_string());
    state.upsert_session(running);
    state.upsert_session(managed_session_record(
        "ses_creating",
        SessionState::Creating,
        false,
        "ins_current",
    ));
    state.upsert_session(managed_session_record(
        "ses_waiting",
        SessionState::WaitingApproval,
        false,
        "ins_current",
    ));
    state.upsert_session(managed_session_record(
        "ses_foreign",
        SessionState::Running,
        false,
        "ins_other",
    ));
    state.upsert_session(managed_session_record(
        "ses_archived",
        SessionState::Running,
        true,
        "ins_current",
    ));

    assert_eq!(state.lifecycle_codex_in_flight_count(), 3);
}

#[tokio::test]
async fn lifecycle_quiesce_blocks_new_external_work_until_unquiesce() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let work_permit = lifecycle
        .try_begin_external_work()
        .expect("active lifecycle accepts work");

    assert_eq!(lifecycle.in_flight_count(), 1);
    assert!(lifecycle.quiesce());
    assert!(lifecycle.try_begin_external_work().is_err());
    assert!(
        !lifecycle
            .wait_until_drained(std::time::Duration::from_millis(5))
            .await
    );
    drop(work_permit);
    assert!(
        lifecycle
            .wait_until_drained(std::time::Duration::from_secs(1))
            .await
    );
    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    assert!(lifecycle.unquiesce_if_current(token));
    assert!(lifecycle.try_begin_external_work().is_ok());
}

#[test]
fn lifecycle_unquiesce_token_is_invalidated_by_later_quiesce() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Quiescing));
    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");

    assert!(lifecycle.quiesce());
    assert!(!lifecycle.unquiesce_if_current(token));
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Quiescing);

    let current_token = lifecycle
        .prepare_unquiesce()
        .expect("current lifecycle prepares unquiesce");
    assert!(lifecycle.unquiesce_if_current(current_token));
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Active);
}

#[test]
fn lifecycle_drain_token_is_cancelled_by_unquiesce() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let drain_token = lifecycle
        .quiesce_for_drain()
        .expect("active lifecycle begins drain quiesce");
    assert!(lifecycle.is_current_quiesce(drain_token));

    let unquiesce_token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    assert!(lifecycle.unquiesce_if_current(unquiesce_token));

    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Active);
    assert!(!lifecycle.is_current_quiesce(drain_token));
}

#[test]
fn lifecycle_drain_token_is_invalidated_by_later_quiesce() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let stale_token = lifecycle
        .quiesce_for_drain()
        .expect("active lifecycle begins drain quiesce");
    let current_token = lifecycle
        .quiesce_for_drain()
        .expect("quiesced lifecycle refreshes drain quiesce");

    assert_ne!(stale_token, current_token);
    assert!(!lifecycle.is_current_quiesce(stale_token));
    assert!(lifecycle.is_current_quiesce(current_token));
}

#[test]
fn lifecycle_rejects_drainable_sidecar_work_while_quiescing() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Quiescing));
    assert!(lifecycle.try_begin_drainable_external_work(None).is_err());
    assert!(lifecycle.try_begin_external_work().is_err());
    assert_eq!(lifecycle.in_flight_count(), 0);

    let active = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let work_permit = active
        .try_begin_drainable_external_work(None)
        .expect("active lifecycle accepts sidecar-owned drainable work");
    assert_eq!(active.in_flight_count(), 1);
    drop(work_permit);
    assert_eq!(active.in_flight_count(), 0);

    let shutting_down = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::ShuttingDown));
    assert!(
        shutting_down
            .try_begin_drainable_external_work(None)
            .is_err()
    );
    assert_eq!(shutting_down.in_flight_count(), 0);
}

#[test]
fn lifecycle_accepts_already_received_sidecar_work_while_quiescing() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let received_before_quiesce = Utc::now();
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(lifecycle.quiesce());

    let work_permit = lifecycle
        .try_begin_drainable_external_work(Some(received_before_quiesce))
        .expect("sidecar work received before quiesce remains drainable");
    assert_eq!(lifecycle.in_flight_count(), 1);
    drop(work_permit);

    let received_after_quiesce = Utc::now() + Duration::seconds(1);
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );
}

#[test]
fn lifecycle_rejects_same_millisecond_sidecar_work_after_cutoff() {
    let cutoff = Utc.timestamp_millis_opt(1_700_000_000_123).unwrap() + Duration::microseconds(750);
    let previous_millisecond = Utc.timestamp_millis_opt(1_700_000_000_122).unwrap();
    let same_millisecond = Utc.timestamp_millis_opt(1_700_000_000_123).unwrap();

    assert!(sidecar_received_before_cutoff(previous_millisecond, cutoff));
    assert!(!sidecar_received_before_cutoff(same_millisecond, cutoff));

    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    {
        let mut state = lifecycle.phase.lock().expect("lifecycle phase poisoned");
        state.phase = LifecycleAdmissionPhase::Quiescing;
        state.phase_started_at = cutoff;
    }

    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(same_millisecond))
            .is_err()
    );
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(previous_millisecond))
            .is_ok()
    );
}

#[test]
fn lifecycle_preserves_first_quiesce_cutoff_through_drain_and_shutdown() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let received_before_quiesce = Utc::now();
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(lifecycle.quiesce());
    let received_after_quiesce = Utc::now() + Duration::seconds(1);

    assert!(lifecycle.quiesce());
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );

    let previous_phase = lifecycle.begin_shutdown().expect("shutdown begins");
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );
    lifecycle.restore_shutdown_phase(previous_phase);
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );

    let work_permit = lifecycle
        .try_begin_drainable_external_work(Some(received_before_quiesce))
        .expect("work received before the first quiesce remains drainable");
    drop(work_permit);
}

#[test]
fn lifecycle_runtime_count_includes_drainable_ingress() {
    assert_eq!(lifecycle_runtime_in_flight_total(2, 3, 5, 7), 17);
}

#[tokio::test]
async fn lifecycle_runtime_count_includes_sidecar_drain_state() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-sidecar-drain-state-test-{}",
        generate_installation_id(Utc::now())
    ));
    let mut config = app_config_with_state_dir(&state_dir, true);
    let plugin_home = config.bridge.cbth_plugin.plugin_home.clone();
    tokio::fs::create_dir_all(&plugin_home).await.unwrap();
    let drain_state_dir = plugin_home.join(SIDECAR_DRAIN_STATE_DIR);
    tokio::fs::create_dir_all(&drain_state_dir).await.unwrap();
    let matching_prefix = sidecar_drain_state_file_prefix(&config);
    tokio::fs::write(
        drain_state_dir.join(format!("{matching_prefix}12345.json")),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "pid": std::process::id(),
            "in_flight_count": 2,
            "updated_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        drain_state_dir.join("instance-1--old-release--12346.json"),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "old-release",
            "pid": 12346,
            "in_flight_count": 5,
            "updated_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        drain_state_dir.join("instance-1--old-release--malformed.json"),
        b"not-json",
    )
    .await
    .unwrap();
    let stale_current_pid_path =
        drain_state_dir.join(format!("{matching_prefix}stale-current-pid.json"));
    tokio::fs::write(
        &stale_current_pid_path,
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "pid": std::process::id(),
            "in_flight_count": 11,
            "updated_at": (Utc::now() - Duration::seconds(121)).to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        drain_state_dir.join(format!("{matching_prefix}2147483647.json")),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "pid": 2_147_483_647_u32,
            "in_flight_count": 7,
            "updated_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(sidecar_drain_in_flight_count(&config).await, 2);
    assert!(
        !tokio::fs::try_exists(&stale_current_pid_path)
            .await
            .unwrap()
    );

    config.bridge.cbth_plugin.enabled = false;
    assert_eq!(sidecar_drain_in_flight_count(&config).await, 0);
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[test]
fn sidecar_drain_state_file_prefix_is_bounded() {
    let state_dir = std::env::temp_dir().join("wxcd-worker-sidecar-drain-state-prefix-test");
    let mut config = app_config_with_state_dir(&state_dir, true);
    config.bridge.cbth_plugin.plugin_instance_id = "instance-".repeat(128);
    config.bridge.cbth_plugin.plugin_release_id = "release-".repeat(128);

    let prefix = sidecar_drain_state_file_prefix(&config);

    assert!(prefix.starts_with("scope-"));
    assert!(prefix.ends_with("--"));
    assert!(prefix.len() < 64, "{prefix}");
}

#[tokio::test]
async fn deferred_ingress_records_do_not_block_lifecycle_drain() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-sidecar-deferred-ingress-test-{}",
        generate_installation_id(Utc::now())
    ));
    let mut config = app_config_with_state_dir(&state_dir, true);
    let plugin_home = config.bridge.cbth_plugin.plugin_home.clone();
    let deferred_ingress_dir = plugin_home.join(SIDECAR_DEFERRED_INGRESS_DIR);
    tokio::fs::create_dir_all(&deferred_ingress_dir)
        .await
        .unwrap();
    tokio::fs::write(
        deferred_ingress_dir.join("current.json"),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "event_id": "event-current"
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        deferred_ingress_dir.join("old-release.json"),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "old-release",
            "event_id": "event-old",
            "deferred_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        deferred_ingress_dir.join("stale-old-release.json"),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "old-release",
            "event_id": "event-stale-old",
            "deferred_at": (Utc::now() - Duration::days(2)).to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        deferred_ingress_dir.join("foreign-instance.json"),
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-other",
            "plugin_release_id": "0.1.0",
            "event_id": "event-foreign",
            "deferred_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::fs::write(deferred_ingress_dir.join("malformed.json"), b"not-json")
        .await
        .unwrap();

    assert_eq!(lifecycle_sidecar_in_flight_count(&config, None).await, 0);

    config.bridge.cbth_plugin.enabled = false;
    assert_eq!(lifecycle_sidecar_in_flight_count(&config, None).await, 0);
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn sidecar_drain_requires_post_cutoff_inactive_observation() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-sidecar-drain-barrier-test-{}",
        generate_installation_id(Utc::now())
    ));
    let config = app_config_with_state_dir(&state_dir, true);
    let plugin_home = config.bridge.cbth_plugin.plugin_home.clone();
    let drain_state_dir = plugin_home.join(SIDECAR_DRAIN_STATE_DIR);
    tokio::fs::create_dir_all(&drain_state_dir).await.unwrap();
    let cutoff = Utc.timestamp_millis_opt(1_700_000_000_123).unwrap() + Duration::microseconds(750);
    let state_path = drain_state_dir.join(format!(
        "{}current.json",
        sidecar_drain_state_file_prefix(&config)
    ));

    tokio::fs::write(
        &state_path,
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "pid": std::process::id(),
            "in_flight_count": 0,
            "updated_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        sidecar_drain_in_flight_count_after(&config, Some(cutoff)).await,
        1
    );

    tokio::fs::write(
        &state_path,
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "pid": std::process::id(),
            "in_flight_count": 0,
            "worker_inactive_observed_at": Utc.timestamp_millis_opt(1_700_000_000_123).unwrap().to_rfc3339(),
            "updated_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        sidecar_drain_in_flight_count_after(&config, Some(cutoff)).await,
        1
    );

    tokio::fs::write(
        &state_path,
        serde_json::to_vec(&json!({
            "plugin_instance_id": "instance-1",
            "plugin_release_id": "0.1.0",
            "pid": std::process::id(),
            "in_flight_count": 0,
            "worker_inactive_observed_at": Utc.timestamp_millis_opt(1_700_000_000_124).unwrap().to_rfc3339(),
            "updated_at": Utc::now().to_rfc3339()
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        sidecar_drain_in_flight_count_after(&config, Some(cutoff)).await,
        0
    );

    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn lifecycle_command_response_times_out_when_queue_is_full() {
    let (events_tx, _events_rx) = tokio::sync::mpsc::channel(1);
    let (completion, _completion_rx) = tokio::sync::oneshot::channel();
    events_tx
        .send(WorkerQueueItem::Lifecycle(QueuedLifecycleCommand {
            command: lifecycle_drain_command("held"),
            completion,
            response_flushed: None,
            expires_at: tokio::time::Instant::now() + std::time::Duration::from_secs(60),
            response_expires_at: tokio::time::Instant::now() + std::time::Duration::from_secs(60),
        }))
        .await
        .unwrap();

    let error = match lifecycle_command_response(
        &events_tx,
        lifecycle_drain_command("upgrade"),
        None,
    )
    .await
    {
        Ok(_) => panic!("lifecycle command response should time out"),
        Err(error) => error,
    };

    assert_eq!(error.kind, PluginRpcErrorKind::TransientDaemonUnavailable);
    assert!(
        error
            .message
            .contains("enqueueing worker lifecycle command"),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn lifecycle_command_response_preserves_typed_worker_errors() {
    let (events_tx, mut events_rx) = tokio::sync::mpsc::channel(1);
    let response_task = tokio::spawn(async move {
        lifecycle_command_response(&events_tx, lifecycle_drain_command("upgrade"), None).await
    });

    let WorkerQueueItem::Lifecycle(command) =
        events_rx.recv().await.expect("queued lifecycle command")
    else {
        panic!("expected lifecycle command");
    };
    command
        .completion
        .send(Err(PluginRpcError::new(
            PluginRpcErrorKind::TransientDaemonUnavailable,
            "worker loop is busy",
        )))
        .ok();

    let error = match response_task.await.unwrap() {
        Ok(_) => panic!("lifecycle command should return typed worker error"),
        Err(error) => error,
    };

    assert_eq!(error.kind, PluginRpcErrorKind::TransientDaemonUnavailable);
    assert!(error.retryable);
}

#[tokio::test]
async fn lifecycle_response_flush_reports_dropped_writer() {
    let (flushed_tx, flushed_rx) = tokio::sync::oneshot::channel();
    drop(flushed_tx);

    assert!(!wait_for_lifecycle_response_flush(Some(flushed_rx)).await);
    assert!(wait_for_lifecycle_response_flush(None).await);
}

#[tokio::test]
async fn lifecycle_socket_cleanup_refuses_non_socket_path() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-lifecycle-socket-cleanup-test-{}",
        generate_installation_id(Utc::now())
    ));
    let socket_path = state_dir.join("lifecycle.sock");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    tokio::fs::write(&socket_path, b"not a socket")
        .await
        .unwrap();

    let error = remove_stale_lifecycle_socket(&socket_path)
        .await
        .expect_err("non-socket lifecycle path should be refused");

    assert!(
        error
            .to_string()
            .contains("refusing to replace non-socket lifecycle socket path"),
        "{error:#}"
    );
    assert!(tokio::fs::try_exists(&socket_path).await.unwrap());
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn lifecycle_socket_cleanup_refuses_live_socket_path() {
    let state_dir = std::path::PathBuf::from("/tmp")
        .join(format!("wxcd-ls-{}", generate_installation_id(Utc::now())));
    let socket_path = state_dir.join("l.sock");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

    let error = remove_stale_lifecycle_socket(&socket_path)
        .await
        .expect_err("live lifecycle path should be refused");

    assert!(
        error
            .to_string()
            .contains("refusing to replace live lifecycle socket"),
        "{error:#}"
    );
    assert!(tokio::fs::try_exists(&socket_path).await.unwrap());
    drop(listener);
    tokio::fs::remove_file(&socket_path).await.unwrap();
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn lifecycle_shutdown_timeout_restores_previous_phase() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let work_permit = lifecycle
        .try_begin_external_work()
        .expect("active lifecycle accepts work");
    let previous_phase = lifecycle.begin_shutdown().expect("shutdown begins");

    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::ShuttingDown);
    assert!(
        !lifecycle
            .wait_until_drained(std::time::Duration::from_millis(5))
            .await
    );
    lifecycle.restore_shutdown_phase(previous_phase);

    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Active);
    drop(work_permit);
    assert!(lifecycle.try_begin_external_work().is_ok());
}

#[tokio::test]
async fn lifecycle_shutdown_writes_supervisor_marker_before_accepting() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-lifecycle-shutdown-marker-test-{}",
        generate_installation_id(Utc::now())
    ));
    let marker_path = state_dir.join("supervisor-shutdown-requested.json");
    let mut config = app_config_with_plugin(true);
    config.bridge.state_dir = state_dir.clone();
    config.bridge.cbth_plugin.plugin_home = state_dir.join("plugin-home");

    write_supervisor_shutdown_marker_at(
        &marker_path,
        &config,
        &PluginShutdownRequest {
            reason: "upgrade".to_string(),
        },
    )
    .await
    .unwrap();

    let marker: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&marker_path).await.unwrap()).unwrap();
    assert_eq!(
        marker
            .get("plugin_instance_id")
            .and_then(|value| value.as_str()),
        Some("instance-1")
    );
    assert_eq!(
        marker
            .get("plugin_release_id")
            .and_then(|value| value.as_str()),
        Some("0.1.0")
    );
    assert_eq!(
        marker.get("reason").and_then(|value| value.as_str()),
        Some("upgrade")
    );
    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[test]
fn pre_active_health_requires_quiesced_admission_fence() {
    let active = LifecycleControl::new(LifecycleAdmissionPhase::Active);
    let quiesced = LifecycleControl::new(LifecycleAdmissionPhase::Quiescing);
    let activating = LifecycleControl::new(LifecycleAdmissionPhase::Activating);
    let shutting_down = LifecycleControl::new(LifecycleAdmissionPhase::ShuttingDown);
    let request = PluginHealthCheckRequest::pre_active();
    let active_request = PluginHealthCheckRequest::active();

    assert!(!active.health_check(&request, true).healthy);
    assert!(quiesced.health_check(&request, true).healthy);
    assert!(!activating.health_check(&request, true).healthy);
    assert!(!quiesced.health_check(&active_request, true).healthy);
    assert!(!shutting_down.health_check(&request, true).healthy);
    assert!(!shutting_down.health_check(&active_request, true).healthy);
}

#[test]
fn pre_active_startup_is_plugin_mode_only() {
    assert_eq!(
        initial_lifecycle_phase_from_env(true, Some("1")),
        LifecycleAdmissionPhase::Quiescing
    );
    assert_eq!(
        initial_lifecycle_phase_from_env(true, Some("true")),
        LifecycleAdmissionPhase::Quiescing
    );
    assert_eq!(
        initial_lifecycle_phase_from_env(false, Some("1")),
        LifecycleAdmissionPhase::Active
    );
    assert_eq!(
        initial_lifecycle_phase_from_env(true, None),
        LifecycleAdmissionPhase::Active
    );
}

#[test]
fn sidecar_active_check_waits_for_unquiesce() {
    let quiesced = worker_active_check_ack(true, LifecycleAdmissionPhase::Quiescing);
    assert!(!quiesced.ok);
    assert!(quiesced.healthy);
    assert!(
        quiesced
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("not accepting new Webex work"))
    );

    let active = worker_active_check_ack(true, LifecycleAdmissionPhase::Active);
    assert!(active.ok);
    assert!(active.healthy);
    assert!(active.detail.is_none());
}

#[test]
fn lifecycle_unquiesce_activation_claim_fences_new_transitions() {
    let lifecycle = LifecycleControl::new(LifecycleAdmissionPhase::Quiescing);
    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    let activation = lifecycle
        .begin_unquiesce_activation(token)
        .expect("current unquiesce token is claimed");

    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Activating);
    assert!(lifecycle.begin_quiesce().is_none());
    assert!(lifecycle.begin_shutdown().is_none());
    assert!(!lifecycle.unquiesce_if_current(token));
    assert!(lifecycle.complete_unquiesce_activation(activation));
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Active);
}

#[test]
fn lifecycle_cancelled_unquiesce_does_not_claim_activation() {
    let lifecycle = LifecycleControl::new(LifecycleAdmissionPhase::Quiescing);
    let stale = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    assert!(lifecycle.quiesce());

    assert!(lifecycle.begin_unquiesce_activation(stale).is_none());
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Quiescing);
}

#[test]
fn lifecycle_cancelled_activation_rejects_stale_completion() {
    let lifecycle = LifecycleControl::new(LifecycleAdmissionPhase::Quiescing);
    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    let activation = lifecycle
        .begin_unquiesce_activation(token)
        .expect("current unquiesce token is claimed");

    lifecycle.cancel_unquiesce_activation(activation);

    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Quiescing);
    assert!(!lifecycle.complete_unquiesce_activation(activation));
    assert!(lifecycle.prepare_unquiesce().is_some());
}

#[test]
fn lifecycle_cancelled_activation_preserves_quiesce_cutoff() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let received_before_quiesce = Utc::now();
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(lifecycle.quiesce());
    let original_cutoff = lifecycle
        .sidecar_drain_barrier_started_at()
        .expect("quiesce records a sidecar drain barrier");
    let received_after_quiesce = Utc::now() + Duration::seconds(1);

    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    let activation = lifecycle
        .begin_unquiesce_activation(token)
        .expect("current unquiesce token is claimed");
    std::thread::sleep(std::time::Duration::from_millis(2));
    lifecycle.cancel_unquiesce_activation(activation);

    assert_eq!(
        lifecycle.sidecar_drain_barrier_started_at(),
        Some(original_cutoff)
    );
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );
    let work_permit = lifecycle
        .try_begin_drainable_external_work(Some(received_before_quiesce))
        .expect("work received before the original quiesce remains drainable");
    drop(work_permit);
}

#[test]
fn lifecycle_unquiesce_response_failure_restores_quiesce_cutoff() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    let received_before_quiesce = Utc::now();
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(lifecycle.quiesce());
    let original_cutoff = lifecycle
        .sidecar_drain_barrier_started_at()
        .expect("quiesce records a sidecar drain barrier");
    let received_after_quiesce = Utc::now() + Duration::seconds(1);

    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    let (accepted, activation) = lifecycle.begin_unquiesce_if_current(token);
    assert!(accepted);
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Activating);
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );
    lifecycle.cancel_unquiesce_activation(
        activation.expect("quiesced unquiesce provides pending activation"),
    );

    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Quiescing);
    assert_eq!(
        lifecycle.sidecar_drain_barrier_started_at(),
        Some(original_cutoff)
    );
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );
    let work_permit = lifecycle
        .try_begin_drainable_external_work(Some(received_before_quiesce))
        .expect("work received before the original quiesce remains drainable");
    drop(work_permit);
}

#[test]
fn lifecycle_activation_waits_for_response_flush_before_accepting_work() {
    let lifecycle = Arc::new(LifecycleControl::new(LifecycleAdmissionPhase::Active));
    assert!(lifecycle.quiesce());
    let received_after_quiesce = Utc::now() + Duration::seconds(1);

    let token = lifecycle
        .prepare_unquiesce()
        .expect("quiesced lifecycle prepares unquiesce");
    let activation = lifecycle
        .begin_unquiesce_activation(token)
        .expect("current unquiesce token is claimed");
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Activating);
    assert!(lifecycle.try_begin_external_work().is_err());
    assert!(
        lifecycle
            .try_begin_drainable_external_work(Some(received_after_quiesce))
            .is_err()
    );

    assert!(lifecycle.complete_unquiesce_activation(activation));
    assert_eq!(lifecycle.phase(), LifecycleAdmissionPhase::Active);
    let active_work = lifecycle
        .try_begin_external_work()
        .expect("committed activation accepts new work");
    drop(active_work);
}

#[test]
fn executable_indexes_only_include_current_installation_sessions() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_current",
        SessionState::Idle,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_failed",
        SessionState::Failed,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_foreign",
        SessionState::Idle,
        false,
        "ins_other",
    ));

    state.set_executable_installation(installation_id);

    assert_eq!(
        state
            .room_to_session
            .get("room-ses_current")
            .map(String::as_str),
        Some("ses_current")
    );
    assert_eq!(
        state
            .thread_to_session
            .get("thread-ses_current")
            .map(String::as_str),
        Some("ses_current")
    );
    assert_eq!(
        state
            .room_to_session
            .get("room-ses_failed")
            .map(String::as_str),
        Some("ses_failed")
    );
    assert!(!state.thread_to_session.contains_key("thread-ses_failed"));
    assert!(!state.room_to_session.contains_key("room-ses_foreign"));
    assert!(!state.thread_to_session.contains_key("thread-ses_foreign"));
}

#[test]
fn diagnose_summary_filters_foreign_sessions() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_current",
        SessionState::Failed,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_foreign",
        SessionState::Failed,
        false,
        "ins_other",
    ));

    let sessions = sessions_for_diagnostics(&state, installation_id);

    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, "ses_current");
}

#[test]
fn duplicate_thread_detection_uses_current_installation_sessions() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_failed",
        SessionState::Failed,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_foreign",
        SessionState::Idle,
        false,
        "ins_other",
    ));
    state.set_executable_installation(installation_id);

    assert!(!state.thread_to_session.contains_key("thread-ses_failed"));
    assert_eq!(
        attached_session_for_thread(&state, "thread-ses_failed", installation_id),
        Some("ses_failed")
    );
    assert_eq!(
        attached_session_for_thread(&state, "thread-ses_foreign", installation_id),
        None
    );
}

#[test]
fn installation_guards_reject_foreign_session_and_approval() {
    let installation_id = "ins_current";
    let mut state = WorkerState::default();
    state.upsert_session(managed_session_record(
        "ses_current",
        SessionState::Idle,
        false,
        installation_id,
    ));
    state.upsert_session(managed_session_record(
        "ses_foreign",
        SessionState::Idle,
        false,
        "ins_other",
    ));
    state.pending_approvals.insert(
        "apr_current".to_string(),
        pending_approval("apr_current", "ses_current"),
    );
    state.pending_approvals.insert(
        "apr_foreign".to_string(),
        pending_approval("apr_foreign", "ses_foreign"),
    );

    assert!(
        ensure_session_id_belongs_to_installation(&state, "ses_current", installation_id).is_ok()
    );
    assert!(
        ensure_session_id_belongs_to_installation(&state, "ses_foreign", installation_id).is_err()
    );
    assert!(
        ensure_approval_belongs_to_installation(&state, "apr_current", installation_id).is_ok()
    );
    assert!(
        ensure_approval_belongs_to_installation(&state, "apr_foreign", installation_id).is_err()
    );
}

#[tokio::test]
async fn installation_identity_persists_and_loads() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();

    let config = app_config_with_state_dir(&state_dir, false);
    let created = load_or_create_installation_identity(&config).await.unwrap();
    let loaded = load_or_create_installation_identity(&config).await.unwrap();

    assert_eq!(created, loaded);
    assert!(created.installation_id.starts_with("ins_"));

    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn installation_identity_lookup_error_is_not_treated_as_missing() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-lookup-error-test-{}",
        "a".repeat(300)
    ));

    let config = app_config_with_state_dir(&state_dir, false);
    let error = load_or_create_installation_identity(&config)
        .await
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("failed to inspect installation identity"),
        "{error:#}"
    );
}

#[tokio::test]
async fn installation_identity_recovers_from_snapshot_writer() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-recovery-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let snapshot_path = state_dir.join("bridge-state.json");
    let snapshot = BridgeSnapshot {
        created_at: Utc::now(),
        writer_installation_id: Some("ins_recovered".to_string()),
        sessions: Vec::new(),
        pending_approvals: Vec::new(),
    };
    tokio::fs::write(&snapshot_path, serde_json::to_string(&snapshot).unwrap())
        .await
        .unwrap();

    let config = app_config_with_state_dir(&state_dir, false);
    let recovered = load_or_create_installation_identity(&config).await.unwrap();
    let loaded = load_or_create_installation_identity(&config).await.unwrap();

    assert_eq!(recovered.installation_id, "ins_recovered");
    assert_eq!(loaded, recovered);

    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn installation_identity_mints_when_snapshot_recovery_is_malformed() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-malformed-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let snapshot_path = state_dir.join("bridge-state.json");
    tokio::fs::write(&snapshot_path, "{not json").await.unwrap();

    let config = app_config_with_state_dir(&state_dir, false);
    let created = load_or_create_installation_identity(&config).await.unwrap();
    let loaded = load_or_create_installation_identity(&config).await.unwrap();

    assert!(created.installation_id.starts_with("ins_"));
    assert_eq!(loaded, created);

    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn installation_identity_mints_for_legacy_snapshot_without_writer() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-legacy-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let snapshot_path = state_dir.join("bridge-state.json");
    let snapshot = BridgeSnapshot {
        created_at: Utc::now(),
        writer_installation_id: None,
        sessions: Vec::new(),
        pending_approvals: Vec::new(),
    };
    tokio::fs::write(&snapshot_path, serde_json::to_string(&snapshot).unwrap())
        .await
        .unwrap();

    let config = app_config_with_state_dir(&state_dir, false);
    let created = load_or_create_installation_identity(&config).await.unwrap();

    assert!(created.installation_id.starts_with("ins_"));
    assert_ne!(created.installation_id, "ins_recovered");

    tokio::fs::remove_dir_all(&state_dir).await.unwrap();
}

#[tokio::test]
async fn installation_identity_recovers_from_plugin_home_snapshot_writer() {
    let state_dir = std::env::temp_dir().join(format!(
        "wxcd-worker-installation-plugin-home-recovery-test-{}",
        generate_installation_id(Utc::now())
    ));
    tokio::fs::create_dir_all(&state_dir).await.unwrap();
    let config = app_config_with_state_dir(&state_dir, true);
    let snapshot_path = config
        .bridge
        .cbth_plugin
        .plugin_home
        .join("bridge-state.json");
    tokio::fs::create_dir_all(snapshot_path.parent().unwrap())
        .await
        .unwrap();
    let snapshot = BridgeSnapshot {
        created_at: Utc::now(),
        writer_installation_id: Some("ins_plugin_home".to_string()),
        sessions: Vec::new(),
        pending_approvals: Vec::new(),
    };
    tokio::fs::write(&snapshot_path, serde_json::to_string(&snapshot).unwrap())
        .await
        .unwrap();

    let recovered = load_or_create_installation_identity(&config).await.unwrap();

    assert_eq!(recovered.installation_id, "ins_plugin_home");

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

fn managed_session_record(
    session_id: &str,
    state: SessionState,
    archived: bool,
    installation_id: &str,
) -> SessionRecord {
    let mut session = session_record(session_id, state, archived);
    session.authority = Some(SessionAuthority {
        installation_id: installation_id.to_string(),
    });
    session
}

fn pending_approval(approval_id: &str, session_id: &str) -> wxcd_proto::PendingApproval {
    wxcd_proto::PendingApproval {
        approval_id: approval_id.to_string(),
        session_id: session_id.to_string(),
        thread_id: format!("thread-{session_id}"),
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
    }
}
