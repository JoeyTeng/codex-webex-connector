use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::time::{Duration, sleep};
use url::Url;

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
pub struct Person {
    pub id: String,
    pub display_name: Option<String>,
    pub emails: Option<Vec<String>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureMembership {
    Created,
    AlreadyPresent,
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

    pub async fn get_me(&self) -> Result<Person> {
        self.get(&format!("{BASE_URL}/people/me")).await
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
        self.get(&resource_url("rooms", room_id)).await
    }

    pub async fn list_rooms(&self, max: usize) -> Result<Vec<Room>> {
        let response: ItemsResponse<Room> = self
            .get(&format!("{BASE_URL}/rooms?max={max}"))
            .await
            .context("failed to list rooms")?;
        Ok(response.items)
    }

    pub async fn create_room(&self, title: &str) -> Result<Room> {
        self.post(&format!("{BASE_URL}/rooms"), &CreateRoomRequest { title })
            .await
    }

    pub async fn update_room_title(&self, room_id: &str, title: &str) -> Result<Room> {
        self.put(
            &resource_url("rooms", room_id),
            &UpdateRoomRequest { title },
        )
        .await
    }

    pub async fn delete_room(&self, room_id: &str) -> Result<()> {
        let response = self
            .inner
            .delete(resource_url("rooms", room_id))
            .send()
            .await?;
        decode_empty_response(response).await
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

    pub async fn ensure_membership(
        &self,
        room_id: &str,
        person_email: &str,
    ) -> Result<EnsureMembership> {
        let url = format!("{BASE_URL}/memberships");
        let mut delay = Duration::from_millis(250);
        for attempt in 0..5 {
            let response = self
                .inner
                .post(&url)
                .json(&CreateMembershipRequest {
                    room_id,
                    person_email,
                })
                .send()
                .await?;
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read response body")?;
            if is_transient_invalid_room(status, &body) && attempt < 4 {
                sleep(delay).await;
                delay *= 2;
                continue;
            }
            return decode_membership_response(status, body);
        }

        unreachable!("ensure_membership retry loop exhausted unexpectedly")
    }

    pub async fn create_message(&self, request: &CreateMessageRequest) -> Result<Message> {
        let url = format!("{BASE_URL}/messages");
        let mut delay = Duration::from_millis(250);
        for attempt in 0..5 {
            let response = self.inner.post(&url).json(request).send().await?;
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read response body")?;
            if status.is_success() {
                return serde_json::from_str(&body)
                    .with_context(|| format!("failed to decode Webex response body: {body}"));
            }
            if is_transient_invalid_room(status, &body) && attempt < 4 {
                sleep(delay).await;
                delay *= 2;
                continue;
            }
            return decode_response_body(status, body);
        }

        unreachable!("create_message retry loop exhausted unexpectedly")
    }

    pub async fn update_message(
        &self,
        message_id: &str,
        request: &UpdateMessageRequest,
    ) -> Result<Message> {
        self.put(&resource_url("messages", message_id), request)
            .await
    }

    pub async fn list_messages(&self, room_id: &str, max: usize) -> Result<Vec<Message>> {
        self.list_messages_page(room_id, max, None).await
    }

    pub async fn list_messages_page(
        &self,
        room_id: &str,
        max: usize,
        before_message: Option<&str>,
    ) -> Result<Vec<Message>> {
        let mut url = Url::parse(&format!("{BASE_URL}/messages"))
            .context("failed to build messages list URL")?;
        url.query_pairs_mut()
            .append_pair("roomId", room_id)
            .append_pair("max", &max.to_string());
        if let Some(before_message) = before_message {
            url.query_pairs_mut()
                .append_pair("beforeMessage", before_message);
        }

        let response: ItemsResponse<Message> = self
            .get(url.as_str())
            .await
            .context("failed to list room messages")?;
        Ok(response.items)
    }

    pub async fn get_attachment_action(
        &self,
        attachment_action_id: &str,
    ) -> Result<AttachmentAction> {
        self.get(&resource_url("attachment/actions", attachment_action_id))
            .await
    }

    async fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self.inner.get(url).send().await?;
        decode_response(response).await
    }

    async fn post<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        let response = self.inner.post(url).json(body).send().await?;
        decode_response(response).await
    }

    async fn put<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        let response = self.inner.put(url).json(body).send().await?;
        decode_response(response).await
    }
}

fn resource_url(collection_path: &str, resource_id: &str) -> String {
    let resource_id = encode_path_segment(resource_id);
    format!("{BASE_URL}/{collection_path}/{resource_id}")
}

fn encode_path_segment(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                encoded.push('%');
                encoded.push(char::from(HEX[(byte >> 4) as usize]));
                encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
            }
        }
    }
    encoded
}

async fn decode_response<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read response body")?;
    decode_response_body(status, body)
}

async fn decode_empty_response(response: reqwest::Response) -> Result<()> {
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read response body")?;
    decode_empty_response_body(status, body)
}

fn decode_empty_response_body(status: StatusCode, body: String) -> Result<()> {
    if status == StatusCode::NOT_FOUND {
        return Ok(());
    }
    if !status.is_success() {
        if status == StatusCode::UNAUTHORIZED {
            bail!("webex API returned 401 unauthorized");
        }
        bail!("webex API returned {status}: {body}");
    }
    Ok(())
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

fn is_transient_invalid_room(status: StatusCode, body: &str) -> bool {
    status == StatusCode::BAD_REQUEST && body.contains("Invalid roomId")
}

fn decode_membership_response(status: StatusCode, body: String) -> Result<EnsureMembership> {
    if status == StatusCode::CONFLICT {
        return Ok(EnsureMembership::AlreadyPresent);
    }

    let lower_body = body.to_ascii_lowercase();
    if status == StatusCode::BAD_REQUEST
        && lower_body.contains("already")
        && (lower_body.contains("member") || lower_body.contains("membership"))
    {
        return Ok(EnsureMembership::AlreadyPresent);
    }

    let _: Membership = decode_response_body(status, body)?;
    Ok(EnsureMembership::Created)
}

#[cfg(test)]
mod tests {
    use super::{EnsureMembership, decode_empty_response_body, decode_membership_response};
    use reqwest::StatusCode;

    #[test]
    fn encodes_resource_ids_as_single_path_segments() {
        assert_eq!(
            super::resource_url("rooms", "room/with+reserved="),
            "https://webexapis.com/v1/rooms/room%2Fwith%2Breserved%3D"
        );
        assert_eq!(
            super::resource_url("attachment/actions", "action/with space"),
            "https://webexapis.com/v1/attachment/actions/action%2Fwith%20space"
        );
    }

    #[test]
    fn decodes_membership_success() {
        let result = decode_membership_response(
            StatusCode::OK,
            r#"{"id":"membership","roomId":"room","personEmail":"user@example.com"}"#.to_string(),
        )
        .expect("membership success should decode");
        assert_eq!(result, EnsureMembership::Created);
    }

    #[test]
    fn treats_conflict_as_existing_membership() {
        let result = decode_membership_response(
            StatusCode::CONFLICT,
            r#"{"message":"person is already a member"}"#.to_string(),
        )
        .expect("membership conflict should be idempotent");
        assert_eq!(result, EnsureMembership::AlreadyPresent);
    }

    #[test]
    fn treats_already_member_bad_request_as_existing_membership() {
        let result = decode_membership_response(
            StatusCode::BAD_REQUEST,
            r#"{"message":"Membership already exists"}"#.to_string(),
        )
        .expect("already-member bad request should be idempotent");
        assert_eq!(result, EnsureMembership::AlreadyPresent);
    }

    #[test]
    fn returns_other_membership_errors() {
        let error = decode_membership_response(
            StatusCode::FORBIDDEN,
            r#"{"message":"Forbidden"}"#.to_string(),
        )
        .expect_err("non-idempotent membership errors should be returned");
        assert!(error.to_string().contains("403"));
    }

    #[test]
    fn treats_delete_room_not_found_as_success() {
        decode_empty_response_body(
            StatusCode::NOT_FOUND,
            r#"{"message":"Not Found"}"#.to_string(),
        )
        .expect("delete room should be idempotent");
    }

    #[test]
    fn detects_transient_invalid_room_errors() {
        assert!(super::is_transient_invalid_room(
            StatusCode::BAD_REQUEST,
            r#"{"message":"Invalid roomId"}"#
        ));
        assert!(!super::is_transient_invalid_room(
            StatusCode::BAD_REQUEST,
            r#"{"message":"Membership already exists"}"#
        ));
    }
}
