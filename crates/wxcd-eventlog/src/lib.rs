use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use tracing::warn;
use wxcd_proto::{
    ApprovalDecision, BridgeEvent, BridgeEventEnvelope, BridgeSnapshot, PendingApproval,
    SessionRecord, SessionState,
};
use wxcd_webex::{CreateMessageRequest, Message, WebexClient};

const EVENT_PREFIX: &str = "WXCD/V1 EVENT ";
const SNAPSHOT_PREFIX: &str = "WXCD/V1 SNAPSHOT ";
const REPLAY_PAGE_SIZE: usize = 100;
const REPLAY_MAX_PAGES: usize = 50;

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
        let mut messages = Vec::new();
        let mut before_message = None;
        let mut pages_read = 0;
        loop {
            let page = self
                .client
                .list_messages_page(self.room_id, REPLAY_PAGE_SIZE, before_message.as_deref())
                .await
                .context("failed to read Data Space messages")?;
            let page_len = page.len();
            if page_len == 0 {
                break;
            }
            if replay_page_exceeds_limit(page_len, pages_read, REPLAY_MAX_PAGES) {
                bail!("Data Space replay reached page limit before finding a snapshot");
            }
            before_message = page.last().map(|message| message.id.clone());
            messages.extend(page);
            pages_read += 1;

            if !replay_needs_older_page(&messages, page_len, REPLAY_PAGE_SIZE) {
                break;
            }
        }
        Ok(replay_from_messages(messages))
    }
}

fn replay_page_exceeds_limit(page_len: usize, pages_read: usize, max_pages: usize) -> bool {
    page_len > 0 && pages_read >= max_pages
}

fn replay_needs_older_page(messages: &[Message], last_page_len: usize, page_size: usize) -> bool {
    last_page_len == page_size && !messages.iter().any(message_has_snapshot)
}

fn message_has_snapshot(message: &Message) -> bool {
    message
        .text
        .as_deref()
        .or(message.markdown.as_deref())
        .is_some_and(|text| text.starts_with(SNAPSHOT_PREFIX))
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
                    session.state = SessionState::Archived;
                }
                self.pending_approvals
                    .retain(|_, approval| approval.session_id != session_id);
            }
            BridgeEvent::SessionPurged { session_id, .. } => {
                self.sessions.remove(&session_id);
                self.pending_approvals
                    .retain(|_, approval| approval.session_id != session_id);
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
        ApprovalKind, BridgeEvent, BridgeEventEnvelope, BridgeSnapshot, PendingApproval,
        SessionRecord, SessionState,
    };

    use super::{
        EVENT_PREFIX, ReplayState, SNAPSHOT_PREFIX, replay_from_messages, replay_needs_older_page,
        replay_page_exceeds_limit,
    };
    use wxcd_webex::Message;

    #[test]
    fn snapshot_round_trip_preserves_collections() {
        let mut replay = ReplayState::default();
        replay.sessions.insert(
            "ses_1".to_string(),
            session_record("ses_1", "Example", "thread", SessionState::Idle),
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
        let session = session_record("ses_1", "Example", "thread", SessionState::Idle);
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

    #[test]
    fn replay_uses_snapshot_from_older_page() {
        let stale = session_record("ses_1", "Stale", "thread", SessionState::Failed);
        let snapshot = BridgeSnapshot {
            created_at: Utc::now(),
            sessions: vec![session_record(
                "ses_1",
                "Snapshot",
                "thread",
                SessionState::Idle,
            )],
            pending_approvals: Vec::new(),
        };
        let updated = session_record("ses_1", "Latest", "thread", SessionState::Completed);
        let messages = vec![
            event_message(
                "event_latest",
                BridgeEvent::SessionUpdated { session: updated },
            ),
            snapshot_message("snapshot", snapshot),
            event_message(
                "event_stale",
                BridgeEvent::SessionUpdated { session: stale },
            ),
        ];

        let replay = replay_from_messages(messages);
        let rebuilt = replay.sessions.get("ses_1").expect("session should replay");
        assert_eq!(rebuilt.title, "Latest");
        assert_eq!(rebuilt.state, SessionState::Completed);
        assert_eq!(replay.events_since_snapshot, 1);
    }

    #[test]
    fn replay_fetches_older_pages_until_snapshot_or_exhaustion() {
        let full_page_without_snapshot = vec![plain_message("m1", "not wxcd")];
        assert!(replay_needs_older_page(&full_page_without_snapshot, 1, 1));

        let full_page_with_snapshot = vec![snapshot_message(
            "snapshot",
            BridgeSnapshot {
                created_at: Utc::now(),
                sessions: Vec::new(),
                pending_approvals: Vec::new(),
            },
        )];
        assert!(!replay_needs_older_page(&full_page_with_snapshot, 1, 1));
        assert!(!replay_needs_older_page(&full_page_without_snapshot, 1, 2));
    }

    #[test]
    fn replay_page_limit_allows_empty_probe_page() {
        assert!(!replay_page_exceeds_limit(0, 50, 50));
        assert!(replay_page_exceeds_limit(1, 50, 50));
        assert!(!replay_page_exceeds_limit(100, 49, 50));
    }

    #[test]
    fn replay_purge_removes_session_and_related_approvals() {
        let mut replay = ReplayState::default();
        replay.sessions.insert(
            "ses_1".to_string(),
            session_record("ses_1", "Example", "thread", SessionState::Archived),
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

        replay.apply(BridgeEvent::SessionPurged {
            session_id: "ses_1".to_string(),
            purged_at: Utc::now(),
        });

        assert!(replay.sessions.is_empty());
        assert!(replay.pending_approvals.is_empty());
    }

    fn session_record(
        session_id: &str,
        title: &str,
        thread_id: &str,
        state: SessionState,
    ) -> SessionRecord {
        SessionRecord {
            session_id: session_id.to_string(),
            title: title.to_string(),
            repo_name: "wxcd".to_string(),
            repo_path: "/tmp/wxcd".to_string(),
            owner_email: "user@example.com".to_string(),
            session_room_id: "room".to_string(),
            session_room_web_link: None,
            thread_id: thread_id.to_string(),
            overview_message_id: None,
            state,
            last_checkpoint: None,
            last_final: None,
            active_turn_id: None,
            active_turn_buffer: String::new(),
            updated_at: Utc::now(),
            archived: false,
            failure: None,
            authority: None,
            local_mirror: None,
        }
    }

    fn event_message(id: &str, event: BridgeEvent) -> Message {
        let envelope = BridgeEventEnvelope {
            ts: Utc::now(),
            event,
        };
        plain_message(
            id,
            &format!(
                "{EVENT_PREFIX}{}",
                serde_json::to_string(&envelope).unwrap()
            ),
        )
    }

    fn snapshot_message(id: &str, snapshot: BridgeSnapshot) -> Message {
        plain_message(
            id,
            &format!(
                "{SNAPSHOT_PREFIX}{}",
                serde_json::to_string(&snapshot).unwrap()
            ),
        )
    }

    fn plain_message(id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            room_id: None,
            markdown: None,
            text: Some(text.to_string()),
            person_email: None,
            created: None,
        }
    }
}
