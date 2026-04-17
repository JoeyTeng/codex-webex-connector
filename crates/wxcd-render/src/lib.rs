use std::path::Path;

use chrono::SecondsFormat;
use serde_json::json;
use wxcd_proto::{PendingApproval, SessionRecord, SessionState};
use wxcd_webex::MessageAttachment;

pub fn render_help() -> &'static str {
    "Control room commands:\n- `help` or `/help`\n- `list` or `/list`\n- `list local` or `/list local`\n- `list local page <n>` or `/list local page <n>`\n- `list all` or `/list all`\n- `list all page <n>` or `/list all page <n>`\n- `resume local <thread_id>` or `/resume local <thread_id>`\n- `new <repo> :: <task>` or `/new <repo> :: <task>`\n- `archive <session_id>` or `/archive <session_id>`\n- In a group space, mention the bot before the command.\n\nInside a session room:\n- `help` or `/help`\n- plain text: send a new turn\n- `/status`\n- `/history`\n- `/history page <n>`\n- `/resume`\n- `/pause`\n- `/stop`"
}

#[derive(Debug, Clone)]
pub struct LocalThreadListItem {
    pub thread_id: String,
    pub title: String,
    pub cwd: Option<String>,
    pub status: String,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedHistoryTurn {
    pub user_text: String,
    pub assistant_text: Option<String>,
}

pub fn render_control_list(sessions: &[SessionRecord]) -> String {
    if sessions.is_empty() {
        return "No bridge sessions found.".to_string();
    }

    let mut lines = Vec::with_capacity(sessions.len() + 1);
    lines.push("Bridge sessions:".to_string());
    for session in sessions {
        lines.push(format!(
            "- `{}` {} {} ({})",
            session.session_id,
            state_emoji(&session.state),
            session.title,
            session.owner_email
        ));
    }
    lines.join("\n")
}

pub fn render_local_thread_list(
    threads: &[LocalThreadListItem],
    total_count: usize,
    has_more: bool,
    page: usize,
    page_size: usize,
) -> String {
    if total_count == 0 {
        return "No local-only Codex threads found.".to_string();
    }

    let mut lines = Vec::with_capacity(threads.len() + 2);
    if threads.is_empty() {
        let last_page = total_count.div_ceil(page_size);
        return format!(
            "No local-only Codex threads on page {}. Last available page is {}.",
            page, last_page
        );
    }

    let start = (page - 1) * page_size + 1;
    let end = start + threads.len() - 1;
    lines.push(format!("Local-only Codex threads (page {}):", page));
    for thread in threads {
        lines.push(format!(
            "- `{}` [{}] {} ({}, {})",
            thread.thread_id,
            display_cwd(thread.cwd.as_deref()),
            abbreviate(&thread.title, 72),
            thread.status,
            format_unix_seconds(thread.updated_at)
        ));
    }
    if has_more {
        lines.push(format!(
            "Showing {}-{} of at least {} local-only threads.",
            start, end, total_count
        ));
        lines.push(format!(
            "Use `list local page {}` for the next page.",
            page + 1
        ));
    } else {
        lines.push(format!(
            "Showing {}-{} of {} local-only threads.",
            start, end, total_count
        ));
    }
    lines.push("Use `resume local <thread_id>` to attach one to a new Webex space.".to_string());
    lines.join("\n")
}

pub fn render_status_summary(session: &SessionRecord) -> String {
    format!(
        "[{}] {} {}\nRepo: `{}`\nThread: `{}`\nUpdated: {}",
        session.session_id,
        state_emoji(&session.state),
        session.title,
        session.repo_name,
        session.thread_id,
        session
            .updated_at
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    )
}

pub fn render_imported_history(
    thread_id: &str,
    turns: &[ImportedHistoryTurn],
    total_turns: usize,
) -> Vec<String> {
    if turns.is_empty() || total_turns == 0 {
        return Vec::new();
    }

    let shown_turns = turns.len();
    let start_index = total_turns.saturating_sub(shown_turns).saturating_add(1);
    render_history_messages(
        format!(
            "Imported local Codex history from thread `{thread_id}`.\nShowing latest {} of {} turns.",
            shown_turns, total_turns
        ) + "\nUse `/history` for the newest page and `/history page <n>` for older turns.",
        start_index,
        turns,
    )
}

pub fn render_history_page(
    thread_id: &str,
    turns: &[ImportedHistoryTurn],
    page: usize,
    page_size: usize,
    total_turns: usize,
) -> Vec<String> {
    if total_turns == 0 {
        return vec![format!("No turn history found for thread `{thread_id}`.")];
    }

    let total_pages = total_turns.div_ceil(page_size);
    if turns.is_empty() {
        return vec![format!(
            "No history on page {} for thread `{thread_id}`. Last available page is {}.",
            page, total_pages
        )];
    }

    let shown_turns = turns.len();
    let newer_turns = page.saturating_sub(1).saturating_mul(page_size);
    let end_index = total_turns.saturating_sub(newer_turns);
    let start_index = end_index.saturating_sub(shown_turns).saturating_add(1);
    let mut header = format!(
        "Thread `{thread_id}` history page {page} of {total_pages}.\nShowing turns {start_index}-{end_index} of {total_turns}. Newest page is 1."
    );
    if page > 1 {
        header.push_str(&format!(
            "\nUse `/history page {}` for newer turns.",
            page - 1
        ));
    }
    if page < total_pages {
        header.push_str(&format!(
            "\nUse `/history page {}` for older turns.",
            page + 1
        ));
    }

    render_history_messages(header, start_index, turns)
}

pub fn render_final_summary(session: &SessionRecord) -> String {
    let last_final = session
        .last_final
        .as_deref()
        .unwrap_or("No final answer was captured.");
    format!(
        "[{}] {} {}\n\n{}",
        session.session_id,
        state_emoji(&session.state),
        session.title,
        last_final
    )
}

pub fn build_overview_attachment(session: &SessionRecord) -> MessageAttachment {
    MessageAttachment {
        content_type: "application/vnd.microsoft.card.adaptive".to_string(),
        content: json!({
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": [
                {
                    "type": "TextBlock",
                    "size": "Medium",
                    "weight": "Bolder",
                    "text": format!("{} {}", state_emoji(&session.state), session.title),
                    "wrap": true
                },
                {
                    "type": "FactSet",
                    "facts": [
                        { "title": "Session", "value": session.session_id },
                        { "title": "Repo", "value": session.repo_name },
                        { "title": "Thread", "value": session.thread_id },
                        { "title": "State", "value": format!("{:?}", session.state) },
                        { "title": "Updated", "value": session.updated_at.to_rfc3339_opts(SecondsFormat::Secs, true) }
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": session.last_checkpoint.clone().unwrap_or_else(|| "No checkpoint yet.".to_string()),
                    "wrap": true,
                    "spacing": "Medium"
                }
            ],
            "actions": [
                {
                    "type": "Action.Submit",
                    "title": "Status",
                    "data": {
                        "wxcd_action": "status",
                        "session_id": session.session_id
                    }
                },
                {
                    "type": "Action.Submit",
                    "title": "Resume",
                    "data": {
                        "wxcd_action": "resume",
                        "session_id": session.session_id
                    }
                },
                {
                    "type": "Action.Submit",
                    "title": "Pause",
                    "data": {
                        "wxcd_action": "pause",
                        "session_id": session.session_id
                    }
                },
                {
                    "type": "Action.Submit",
                    "title": "Archive",
                    "data": {
                        "wxcd_action": "archive",
                        "session_id": session.session_id
                    }
                }
            ]
        }),
    }
}

pub fn build_approval_attachment(approval: &PendingApproval) -> MessageAttachment {
    MessageAttachment {
        content_type: "application/vnd.microsoft.card.adaptive".to_string(),
        content: json!({
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": [
                {
                    "type": "TextBlock",
                    "size": "Medium",
                    "weight": "Bolder",
                    "text": format!("Approval required: {:?}", approval.kind),
                    "wrap": true
                },
                {
                    "type": "FactSet",
                    "facts": [
                        { "title": "Approval", "value": approval.approval_id },
                        { "title": "Session", "value": approval.session_id },
                        { "title": "Thread", "value": approval.thread_id }
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": approval.reason.clone().unwrap_or_else(|| "No reason provided.".to_string()),
                    "wrap": true
                },
                {
                    "type": "TextBlock",
                    "text": approval.command.clone().unwrap_or_else(|| "No command preview available.".to_string()),
                    "wrap": true,
                    "fontType": "Monospace"
                }
            ],
            "actions": [
                action_button("Approve once", "accept", approval),
                action_button("Approve for session", "accept_for_session", approval),
                action_button("Deny", "decline", approval),
                action_button("Cancel turn", "cancel", approval)
            ]
        }),
    }
}

fn action_button(title: &str, decision: &str, approval: &PendingApproval) -> serde_json::Value {
    json!({
        "type": "Action.Submit",
        "title": title,
        "data": {
            "wxcd_action": "approval",
            "decision": decision,
            "approval_id": approval.approval_id,
            "session_id": approval.session_id
        }
    })
}

fn state_emoji(state: &SessionState) -> &'static str {
    match state {
        SessionState::Creating => "🟡",
        SessionState::Idle => "🟡",
        SessionState::Running => "▶",
        SessionState::WaitingApproval => "🛑",
        SessionState::Paused => "⏸",
        SessionState::Completed => "✅",
        SessionState::Failed => "❌",
        SessionState::Archived => "📦",
    }
}

fn abbreviate(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let abbreviated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{abbreviated}...")
    } else {
        abbreviated
    }
}

fn display_cwd(cwd: Option<&str>) -> String {
    cwd.and_then(|value| Path::new(value).file_name().and_then(|part| part.to_str()))
        .unwrap_or("?")
        .to_string()
}

fn format_unix_seconds(value: Option<i64>) -> String {
    value
        .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
        .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_else(|| "unknown time".to_string())
}

fn format_history_turn(index: usize, user_text: String, assistant_text: Option<String>) -> String {
    let assistant_text = assistant_text.unwrap_or_else(|| "No final answer captured.".to_string());
    format!("Turn {index}\nUser:\n{user_text}\n\nAssistant:\n{assistant_text}")
}

fn render_history_messages(
    header: String,
    start_index: usize,
    turns: &[ImportedHistoryTurn],
) -> Vec<String> {
    const MESSAGE_LIMIT: usize = 3500;
    const TURN_TEXT_LIMIT: usize = 1200;

    let mut messages = vec![header];
    let mut current = String::new();

    for (offset, turn) in turns.iter().enumerate() {
        let section = format_history_turn(
            start_index + offset,
            truncate_chars(&turn.user_text, TURN_TEXT_LIMIT),
            turn.assistant_text
                .as_deref()
                .map(|text| truncate_chars(text, TURN_TEXT_LIMIT)),
        );
        if current.is_empty() {
            current = section;
            continue;
        }
        if current.len() + 2 + section.len() > MESSAGE_LIMIT {
            messages.push(current);
            current = section;
        } else {
            current.push_str("\n\n");
            current.push_str(&section);
        }
    }

    if !current.is_empty() {
        messages.push(current);
    }
    messages
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let head = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{head}...")
    } else {
        head
    }
}
