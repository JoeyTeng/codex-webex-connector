use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::time::{Duration, sleep};

const BASE_URL: &str = "https://webexapis.com/v1";

#[derive(Debug, Clone)]
pub struct WebexClient {
    inner: reqwest::Client,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Room {
    pub id: String,
    pub title: Option<String>,
    pub room_type: Option<String>,
    pub web_link: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub room_id: Option<String>,
    pub markdown: Option<String>,
    pub text: Option<String>,
    pub person_email: Option<String>,
    pub created: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Membership {
    pub id: String,
    pub room_id: String,
    pub person_email: Option<String>,
    pub is_moderator: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentAction {
    pub id: String,
    pub message_id: Option<String>,
    pub person_email: Option<String>,
    pub room_id: Option<String>,
    pub inputs: Value,
    pub created: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageAttachment {
    pub content_type: String,
    pub content: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageRequest {
    pub room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub markdown: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<MessageAttachment>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMessageRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub markdown: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<MessageAttachment>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateRoomRequest<'a> {
    title: &'a str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateMembershipRequest<'a> {
    room_id: &'a str,
    person_email: &'a str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateRoomRequest<'a> {
    title: &'a str,
}

#[derive(Debug, Deserialize)]
struct ItemsResponse<T> {
    items: Vec<T>,
}

impl WebexClient {
    pub fn new(bot_token: &str) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        let bearer = format!("Bearer {bot_token}");
        headers.insert(
            reqwest::header::AUTHORIZATION,
            bearer.parse().context("invalid bearer token")?,
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );

        let inner = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context("failed to build webex client")?;
        Ok(Self { inner })
    }

    pub async fn resolve_room_reference(&self, room_ref: &str) -> Result<Room> {
        if let Some(space_id) = room_ref.strip_prefix("webexteams://im?space=") {
            let encoded = base64::engine::general_purpose::STANDARD
                .encode(format!("ciscospark://us/ROOM/{space_id}"));
            return self.get_room(&encoded).await;
        }
        if room_ref.starts_with("http://") || room_ref.starts_with("https://") {
            let rooms = self.list_rooms(100).await?;
            return rooms
                .into_iter()
                .find(|room| room.web_link.as_deref() == Some(room_ref))
                .ok_or_else(|| anyhow!("failed to resolve room by webLink {room_ref}"));
        }

        self.get_room(room_ref).await
    }

    pub async fn get_room(&self, room_id: &str) -> Result<Room> {
        self.get(&format!("{BASE_URL}/rooms/{room_id}")).await
    }

    pub async fn list_rooms(&self, max: usize) -> Result<Vec<Room>> {
        let response: ItemsResponse<Room> = self
            .get(&format!("{BASE_URL}/rooms?max={max}"))
            .await
            .context("failed to list rooms")?;
        Ok(response.items)
    }

    pub async fn create_room(&self, title: &str) -> Result<Room> {
        self.post(
            &format!("{BASE_URL}/rooms"),
            &CreateRoomRequest { title },
        )
        .await
    }

    pub async fn update_room_title(&self, room_id: &str, title: &str) -> Result<Room> {
        self.put(
            &format!("{BASE_URL}/rooms/{room_id}"),
            &UpdateRoomRequest { title },
        )
        .await
    }

    pub async fn create_membership(&self, room_id: &str, person_email: &str) -> Result<Membership> {
        self.post(
            &format!("{BASE_URL}/memberships"),
            &CreateMembershipRequest {
                room_id,
                person_email,
            },
        )
        .await
    }

    pub async fn create_message(&self, request: &CreateMessageRequest) -> Result<Message> {
        let url = format!("{BASE_URL}/messages");
        let mut delay = Duration::from_millis(250);
        for attempt in 0..5 {
            let response = self.inner.post(&url).json(request).send().await?;
            let status = response.status();
            let body = response.text().await.context("failed to read response body")?;
            if status.is_success() {
                return serde_json::from_str(&body)
                    .with_context(|| format!("failed to decode Webex response body: {body}"));
            }
            if status == StatusCode::BAD_REQUEST
                && body.contains("Invalid roomId")
                && attempt < 4
            {
                sleep(delay).await;
                delay *= 2;
                continue;
            }
            return decode_response_body(status, body);
        }

        unreachable!("create_message retry loop exhausted unexpectedly")
    }

    pub async fn update_message(&self, message_id: &str, request: &UpdateMessageRequest) -> Result<Message> {
        self.put(&format!("{BASE_URL}/messages/{message_id}"), request).await
    }

    pub async fn list_messages(&self, room_id: &str, max: usize) -> Result<Vec<Message>> {
        let response: ItemsResponse<Message> = self
            .get(&format!("{BASE_URL}/messages?roomId={room_id}&max={max}"))
            .await
            .context("failed to list room messages")?;
        Ok(response.items)
    }

    pub async fn get_attachment_action(&self, attachment_action_id: &str) -> Result<AttachmentAction> {
        self.get(&format!(
            "{BASE_URL}/attachment/actions/{attachment_action_id}"
        ))
        .await
    }

    async fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self.inner.get(url).send().await?;
        decode_response(response).await
    }

    async fn post<T: DeserializeOwned, B: Serialize + ?Sized>(&self, url: &str, body: &B) -> Result<T> {
        let response = self.inner.post(url).json(body).send().await?;
        decode_response(response).await
    }

    async fn put<T: DeserializeOwned, B: Serialize + ?Sized>(&self, url: &str, body: &B) -> Result<T> {
        let response = self.inner.put(url).json(body).send().await?;
        decode_response(response).await
    }
}

async fn decode_response<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    let body = response.text().await.context("failed to read response body")?;
    decode_response_body(status, body)
}

fn decode_response_body<T: DeserializeOwned>(status: StatusCode, body: String) -> Result<T> {
    if !status.is_success() {
        if status == StatusCode::UNAUTHORIZED {
            bail!("webex API returned 401 unauthorized");
        }
        bail!("webex API returned {status}: {body}");
    }
    serde_json::from_str(&body)
        .with_context(|| format!("failed to decode Webex response body: {body}"))
}
