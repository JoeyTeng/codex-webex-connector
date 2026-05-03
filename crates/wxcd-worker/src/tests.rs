use super::{
    ListCommand, ListMode, RECENT_EVENT_ID_LIMIT, WorkerState, abbreviate,
    extract_thread_history_turns, normalize_control_command_text, normalize_session_command_text,
    parse_attach_session_id, parse_list_command, parse_resume_local_thread_id,
    parse_session_history_page, repo_name_for_cwd, slice_thread_history_page,
};
use serde_json::json;
use wxcd_proto::{AppConfig, BridgeConfig, RepoConfig, WebexConfig};
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
