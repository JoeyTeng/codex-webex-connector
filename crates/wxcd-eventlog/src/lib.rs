use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::warn;
use wxcd_proto::{
    ApprovalDecision, BridgeEvent, BridgeEventEnvelope, BridgeSnapshot, PendingApproval,
    SessionRecord,
};
use wxcd_webex::{CreateMessageRequest, Message, WebexClient};

const EVENT_PREFIX: &str = "WXCD/V1 EVENT ";
const SNAPSHOT_PREFIX: &str = "WXCD/V1 SNAPSHOT ";

#[derive(Debug, Default)]
pub struct ReplayState {
    pub sessions: HashMap<String, SessionRecord>,
    pub pending_approvals: HashMap<String, PendingApproval>,
    pub events_since_snapshot: usize,
}

pub struct EventLog<'a> {
    client: &'a WebexClient,
    room_id: &'a str,
}

impl<'a> EventLog<'a> {
    pub fn new(client: &'a WebexClient, room_id: &'a str) -> Self {
        Self { client, room_id }
    }

    pub async fn append_event(&self, event: BridgeEvent) -> Result<()> {
        let envelope = BridgeEventEnvelope {
            ts: Utc::now(),
            event,
        };
        let payload = format!(
            "{EVENT_PREFIX}{}",
            serde_json::to_string(&envelope).context("failed to encode event envelope")?
        );
        self.client
            .create_message(&CreateMessageRequest {
                room_id: self.room_id.to_string(),
                text: Some(payload),
                markdown: None,
                attachments: None,
            })
            .await
            .context("failed to append Data Space event")?;
        Ok(())
    }

    pub async fn append_snapshot(&self, snapshot: &BridgeSnapshot) -> Result<()> {
        let payload = format!(
            "{SNAPSHOT_PREFIX}{}",
            serde_json::to_string(snapshot).context("failed to encode snapshot")?
        );
        self.client
            .create_message(&CreateMessageRequest {
                room_id: self.room_id.to_string(),
                text: Some(payload),
                markdown: None,
                attachments: None,
            })
            .await
            .context("failed to append Data Space snapshot")?;
        Ok(())
    }

    pub async fn replay(&self) -> Result<ReplayState> {
        let messages = self
            .client
            .list_messages(self.room_id, 100)
            .await
            .context("failed to read Data Space messages")?;
        Ok(replay_from_messages(messages))
    }
}

fn replay_from_messages(mut messages: Vec<Message>) -> ReplayState {
    messages.reverse();

    let mut state = ReplayState::default();
    let mut latest_snapshot_index = None;
    for (index, message) in messages.iter().enumerate() {
        let Some(text) = message.text.as_deref().or(message.markdown.as_deref()) else {
            continue;
        };
        let Some(snapshot_payload) = text.strip_prefix(SNAPSHOT_PREFIX) else {
            continue;
        };
        match serde_json::from_str::<BridgeSnapshot>(snapshot_payload) {
            Ok(snapshot) => {
                state = ReplayState::from_snapshot(snapshot);
                latest_snapshot_index = Some(index);
            }
            Err(error) => {
                warn!(
                    message_id = %message.id,
                    ?error,
                    "failed to decode Data Space snapshot frame, skipping"
                );
            }
        }
    }

    let replay_start = latest_snapshot_index.map_or(0, |idx| idx + 1);
    state.events_since_snapshot = 0;
    for message in &messages[replay_start..] {
        let Some(text) = message.text.as_deref().or(message.markdown.as_deref()) else {
            continue;
        };
        let Some(event_payload) = text.strip_prefix(EVENT_PREFIX) else {
            continue;
        };
        match serde_json::from_str::<BridgeEventEnvelope>(event_payload) {
            Ok(envelope) => {
                state.apply(envelope.event);
                state.events_since_snapshot += 1;
            }
            Err(error) => {
                warn!(
                    message_id = %message.id,
                    ?error,
                    "failed to decode Data Space event frame, skipping"
                );
            }
        }
    }

    state
}

impl ReplayState {
    pub fn to_snapshot(&self) -> BridgeSnapshot {
        BridgeSnapshot {
            created_at: Utc::now(),
            sessions: self.sessions.values().cloned().collect(),
            pending_approvals: self.pending_approvals.values().cloned().collect(),
        }
    }

    pub fn from_snapshot(snapshot: BridgeSnapshot) -> Self {
        Self {
            sessions: snapshot
                .sessions
                .into_iter()
                .map(|session| (session.session_id.clone(), session))
                .collect(),
            pending_approvals: snapshot
                .pending_approvals
                .into_iter()
                .map(|approval| (approval.approval_id.clone(), approval))
                .collect(),
            events_since_snapshot: 0,
        }
    }

    fn apply(&mut self, event: BridgeEvent) {
        match event {
            BridgeEvent::SessionCreated { session } | BridgeEvent::SessionUpdated { session } => {
                self.sessions.insert(session.session_id.clone(), session);
            }
            BridgeEvent::SessionArchived { session_id, .. } => {
                if let Some(session) = self.sessions.get_mut(&session_id) {
                    session.archived = true;
                }
            }
            BridgeEvent::ApprovalRequested { approval } => {
                self.pending_approvals
                    .insert(approval.approval_id.clone(), approval);
            }
            BridgeEvent::ApprovalResolved {
                approval_id,
                decision,
                ..
            } => {
                if matches!(
                    decision,
                    ApprovalDecision::Accept | ApprovalDecision::AcceptForSession
                ) || matches!(
                    decision,
                    ApprovalDecision::Decline | ApprovalDecision::Cancel
                ) {
                    self.pending_approvals.remove(&approval_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use wxcd_proto::{
        ApprovalKind, BridgeEvent, BridgeEventEnvelope, PendingApproval, SessionState,
    };

    use super::{replay_from_messages, ReplayState, EVENT_PREFIX};
    use wxcd_webex::Message;

    #[test]
    fn snapshot_round_trip_preserves_collections() {
        let mut replay = ReplayState::default();
        replay.sessions.insert(
            "ses_1".to_string(),
            wxcd_proto::SessionRecord {
                session_id: "ses_1".to_string(),
                title: "Example".to_string(),
                repo_name: "wxcd".to_string(),
                repo_path: "/tmp/wxcd".to_string(),
                owner_email: "user@example.com".to_string(),
                session_room_id: "room".to_string(),
                session_room_web_link: None,
                thread_id: "thread".to_string(),
                overview_message_id: None,
                state: SessionState::Idle,
                last_checkpoint: None,
                last_final: None,
                active_turn_id: None,
                active_turn_buffer: String::new(),
                updated_at: Utc::now(),
                archived: false,
            },
        );
        replay.pending_approvals.insert(
            "apr_1".to_string(),
            PendingApproval {
                approval_id: "apr_1".to_string(),
                session_id: "ses_1".to_string(),
                thread_id: "thread".to_string(),
                turn_id: "turn".to_string(),
                codex_request_id: serde_json::json!(1),
                item_id: "item".to_string(),
                kind: ApprovalKind::CommandExecution,
                reason: None,
                command: None,
                cwd: None,
                requested_permissions: None,
                card_message_id: None,
                requested_at: Utc::now(),
            },
        );

        let snapshot = replay.to_snapshot();
        let rebuilt = ReplayState::from_snapshot(snapshot);
        assert_eq!(rebuilt.sessions.len(), 1);
        assert_eq!(rebuilt.pending_approvals.len(), 1);
    }

    #[test]
    fn replay_skips_malformed_event_frames() {
        let session = wxcd_proto::SessionRecord {
            session_id: "ses_1".to_string(),
            title: "Example".to_string(),
            repo_name: "wxcd".to_string(),
            repo_path: "/tmp/wxcd".to_string(),
            owner_email: "user@example.com".to_string(),
            session_room_id: "room".to_string(),
            session_room_web_link: None,
            thread_id: "thread".to_string(),
            overview_message_id: None,
            state: SessionState::Idle,
            last_checkpoint: None,
            last_final: None,
            active_turn_id: None,
            active_turn_buffer: String::new(),
            updated_at: Utc::now(),
            archived: false,
        };
        let envelope = BridgeEventEnvelope {
            ts: Utc::now(),
            event: BridgeEvent::SessionUpdated {
                session: session.clone(),
            },
        };
        let messages = vec![
            Message {
                id: "valid".to_string(),
                room_id: None,
                markdown: None,
                text: Some(format!(
                    "{EVENT_PREFIX}{}",
                    serde_json::to_string(&envelope).unwrap()
                )),
                person_email: None,
                created: None,
            },
            Message {
                id: "broken".to_string(),
                room_id: None,
                markdown: None,
                text: Some(format!(r#"{EVENT_PREFIX}{{"type":"probe_event","ts":1}}"#)),
                person_email: None,
                created: None,
            },
        ];

        let replay = replay_from_messages(messages);
        let rebuilt = replay.sessions.get("ses_1").expect("session should replay");
        assert_eq!(rebuilt.title, session.title);
        assert_eq!(replay.events_since_snapshot, 1);
    }
}
