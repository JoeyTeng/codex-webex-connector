use chrono::{DateTime, Utc};
use rand::Rng as _;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Creating,
    Idle,
    Running,
    WaitingApproval,
    Paused,
    Completed,
    Failed,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub title: String,
    pub repo_name: String,
    pub repo_path: String,
    pub owner_email: String,
    pub session_room_id: String,
    pub session_room_web_link: Option<String>,
    pub thread_id: String,
    pub overview_message_id: Option<String>,
    pub state: SessionState,
    pub last_checkpoint: Option<String>,
    pub last_final: Option<String>,
    pub active_turn_id: Option<String>,
    pub active_turn_buffer: String,
    pub updated_at: DateTime<Utc>,
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalKind {
    CommandExecution,
    FileChange,
    Permissions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingApproval {
    pub approval_id: String,
    pub session_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub codex_request_id: serde_json::Value,
    pub item_id: String,
    pub kind: ApprovalKind,
    pub reason: Option<String>,
    pub command: Option<String>,
    pub cwd: Option<String>,
    pub requested_permissions: Option<serde_json::Value>,
    pub card_message_id: Option<String>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BridgeEvent {
    SessionCreated { session: SessionRecord },
    SessionUpdated { session: SessionRecord },
    SessionArchived { session_id: String, archived_at: DateTime<Utc> },
    ApprovalRequested { approval: PendingApproval },
    ApprovalResolved {
        approval_id: String,
        session_id: String,
        decision: ApprovalDecision,
        resolved_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeEventEnvelope {
    pub ts: DateTime<Utc>,
    pub event: BridgeEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BridgeSnapshot {
    pub created_at: DateTime<Utc>,
    pub sessions: Vec<SessionRecord>,
    pub pending_approvals: Vec<PendingApproval>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WebexIngressEnvelope {
    MessageCreated(WebexMessageEvent),
    AttachmentActionCreated(WebexAttachmentActionEvent),
    HealthCheck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebexMessageEvent {
    pub event_id: String,
    pub room_id: String,
    pub message_id: String,
    pub person_email: String,
    pub text: String,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebexAttachmentActionEvent {
    pub event_id: String,
    pub room_id: String,
    pub attachment_action_id: String,
    pub person_email: String,
    pub message_id: Option<String>,
    pub inputs: serde_json::Value,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebexIngressAck {
    pub ok: bool,
    pub healthy: bool,
    pub detail: Option<String>,
}

pub fn generate_session_id(now: DateTime<Utc>) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut rng = rand::rng();
    let mut suffix = String::with_capacity(10);
    for _ in 0..10 {
        let idx = rng.random_range(0..ALPHABET.len());
        suffix.push(ALPHABET[idx] as char);
    }

    format!("ses_{}_{}", now.format("%Y%m%d"), suffix)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::generate_session_id;

    #[test]
    fn session_id_has_expected_shape() {
        let id = generate_session_id(
            chrono::Utc
                .with_ymd_and_hms(2026, 4, 8, 12, 0, 0)
                .single()
                .unwrap(),
        );
        assert!(id.starts_with("ses_20260408_"));
        assert_eq!(id.len(), "ses_20260408_".len() + 10);
    }
}
