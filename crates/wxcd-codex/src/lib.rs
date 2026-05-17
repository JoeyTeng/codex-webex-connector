use std::collections::HashMap;
use std::net::IpAddr;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, warn};
use url::Url;

const MAX_WEBSOCKET_FRAME_BYTES: usize = 16 * 1024 * 1024;
const WEBSOCKET_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

type PendingRequestSender = oneshot::Sender<Result<Value>>;
type SharedConnectionState = Arc<Mutex<ConnectionState>>;

#[derive(Default)]
struct ConnectionState {
    pending: HashMap<u64, PendingRequestSender>,
    closed: Option<String>,
}

#[derive(Debug)]
pub enum CodexEvent {
    Notification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadListPage {
    pub data: Vec<CodexThreadSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadSummary {
    pub id: String,
    pub name: Option<String>,
    pub preview: Option<String>,
    pub cwd: Option<String>,
    pub updated_at: Option<i64>,
    pub status: Option<CodexThreadStatus>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CodexThreadStatus {
    #[serde(rename = "type")]
    pub kind: String,
}

pub struct CodexClient {
    child: Option<Child>,
    writer: Arc<Mutex<CodexWriter>>,
    state: SharedConnectionState,
    next_id: u64,
    events_rx: mpsc::Receiver<CodexEvent>,
}

enum CodexWriter {
    Stdio(BufWriter<ChildStdin>),
    WebSocket(OwnedWriteHalf),
}

impl CodexClient {
    pub async fn spawn() -> Result<Self> {
        let codex_path = std::env::var("WXCD_CODEX_PATH").unwrap_or_else(|_| "codex".to_string());
        let mut child = Command::new(&codex_path)
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn codex app-server")?;

        let stdin = child
            .stdin
            .take()
            .context("codex app-server stdin missing")?;
        let stdout = child
            .stdout
            .take()
            .context("codex app-server stdout missing")?;
        let stderr = child
            .stderr
            .take()
            .context("codex app-server stderr missing")?;

        let writer = Arc::new(Mutex::new(CodexWriter::Stdio(BufWriter::new(stdin))));
        let state = Arc::new(Mutex::new(ConnectionState::default()));
        let (events_tx, events_rx) = mpsc::channel(512);

        tokio::spawn(read_stdout(stdout, Arc::clone(&state), events_tx));
        tokio::spawn(read_stderr(stderr));

        Ok(Self {
            child: Some(child),
            writer,
            state,
            next_id: 1,
            events_rx,
        })
    }

    pub async fn connect_websocket(url: &str) -> Result<Self> {
        let url = parse_loopback_websocket_url(url)?;
        let host = websocket_host(&url)?;
        let port = url
            .port_or_known_default()
            .context("websocket URL missing port")?;
        let mut stream = connect_loopback_websocket(&host, port, &url).await?;
        websocket_handshake(&mut stream, &url).await?;

        let (reader, writer) = stream.into_split();
        let writer = Arc::new(Mutex::new(CodexWriter::WebSocket(writer)));
        let state = Arc::new(Mutex::new(ConnectionState::default()));
        let (events_tx, events_rx) = mpsc::channel(512);

        tokio::spawn(read_websocket(
            reader,
            Arc::clone(&writer),
            Arc::clone(&state),
            events_tx,
        ));

        Ok(Self {
            child: None,
            writer,
            state,
            next_id: 1,
            events_rx,
        })
    }

    pub async fn initialize(&mut self, client_name: &str, experimental_api: bool) -> Result<Value> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": client_name,
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": experimental_api
                }
            }),
        )
        .await
    }

    pub async fn thread_start(
        &mut self,
        cwd: &str,
        approval_policy: &str,
        sandbox_mode: &str,
        developer_instructions: &str,
    ) -> Result<Value> {
        self.request(
            "thread/start",
            json!({
                "cwd": cwd,
                "approvalPolicy": approval_policy,
                "sandbox": sandbox_mode,
                "developerInstructions": developer_instructions,
                "serviceName": "wxcd",
                "experimentalRawEvents": false,
                "persistExtendedHistory": true
            }),
        )
        .await
    }

    pub async fn thread_resume(&mut self, thread_id: &str) -> Result<Value> {
        self.request(
            "thread/resume",
            json!({
                "threadId": thread_id,
                "persistExtendedHistory": true
            }),
        )
        .await
    }

    pub async fn thread_read(&mut self, thread_id: &str, include_turns: bool) -> Result<Value> {
        self.request(
            "thread/read",
            json!({
                "threadId": thread_id,
                "includeTurns": include_turns
            }),
        )
        .await
    }

    pub async fn thread_archive(&mut self, thread_id: &str) -> Result<Value> {
        self.request("thread/archive", json!({ "threadId": thread_id }))
            .await
    }

    pub async fn thread_list(&mut self, include_archived: bool) -> Result<Value> {
        self.request(
            "thread/list",
            json!({
                "includeArchived": include_archived
            }),
        )
        .await
    }

    pub async fn thread_list_page(
        &mut self,
        include_archived: bool,
        cursor: Option<&str>,
    ) -> Result<CodexThreadListPage> {
        let mut params = json!({
            "includeArchived": include_archived
        });
        if let Some(cursor) = cursor {
            params["cursor"] = Value::String(cursor.to_string());
        }

        let response = self.request("thread/list", params).await?;
        serde_json::from_value(response).context("failed to decode thread/list response")
    }

    pub async fn turn_start(&mut self, thread_id: &str, cwd: &str, text: &str) -> Result<Value> {
        self.request(
            "turn/start",
            json!({
                "threadId": thread_id,
                "cwd": cwd,
                "input": [
                    {
                        "type": "text",
                        "text": text,
                        "text_elements": []
                    }
                ]
            }),
        )
        .await
    }

    pub async fn turn_interrupt(&mut self, thread_id: &str, turn_id: &str) -> Result<Value> {
        self.request(
            "turn/interrupt",
            json!({
                "threadId": thread_id,
                "turnId": turn_id
            }),
        )
        .await
    }

    pub async fn respond(&mut self, id: Value, result: Value) -> Result<()> {
        self.write_line(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
        .await
    }

    pub async fn respond_error(&mut self, id: Value, code: i64, message: &str) -> Result<()> {
        self.write_line(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": code,
                "message": message
            }
        }))
        .await
    }

    pub fn events(&mut self) -> &mut mpsc::Receiver<CodexEvent> {
        &mut self.events_rx
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(child) = &mut self.child {
            if let Some(id) = child.id() {
                debug!("shutting down codex app-server pid={id}");
            }
            child.kill().await.ok();
        }
        Ok(())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let request_id = self.next_id;
        self.next_id += 1;
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            if let Some(message) = &state.closed {
                bail!("codex app-server connection is closed: {message}");
            }
            state.pending.insert(request_id, tx);
        }
        let frame = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        });
        if let Err(error) = self.write_line(&frame).await {
            let message =
                format!("failed to write codex app-server request for {method}: {error:#}");
            close_connection_state(&self.state, message).await;
            return Err(error)
                .with_context(|| format!("failed to write codex app-server request for {method}"));
        }
        rx.await
            .context("codex app-server request channel closed")?
            .with_context(|| format!("codex app-server request failed for {method}"))
    }

    async fn write_line(&mut self, value: &Value) -> Result<()> {
        let encoded = serde_json::to_vec(value).context("failed to encode JSON-RPC message")?;
        let mut writer = self.writer.lock().await;
        match &mut *writer {
            CodexWriter::Stdio(writer) => {
                writer.write_all(&encoded).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
            CodexWriter::WebSocket(writer) => {
                write_websocket_text_frame(writer, &encoded).await?;
            }
        }
        Ok(())
    }
}

async fn read_stdout(
    stdout: impl tokio::io::AsyncRead + Unpin,
    state: SharedConnectionState,
    events_tx: mpsc::Sender<CodexEvent>,
) {
    let mut lines = BufReader::new(stdout).lines();
    let close_reason = loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    warn!("ignoring non-JSON stdout line from codex app-server: {line}");
                    continue;
                };
                if !handle_codex_message(value, &state, &events_tx).await {
                    break "codex app-server event receiver closed".to_string();
                }
            }
            Ok(None) => break "codex app-server stdout closed".to_string(),
            Err(error) => {
                let message = format!("failed to read codex app-server stdout: {error:#}");
                warn!("{message}");
                break message;
            }
        }
    };
    close_connection_state(&state, close_reason).await;
}

async fn read_websocket(
    mut reader: OwnedReadHalf,
    writer: Arc<Mutex<CodexWriter>>,
    state: SharedConnectionState,
    events_tx: mpsc::Sender<CodexEvent>,
) {
    let mut read_state = WebSocketReadState::default();
    let close_reason = loop {
        match read_websocket_frame(&mut reader, &mut read_state).await {
            Ok(Some(WebSocketFrame::Text(payload))) => {
                let Ok(value) = serde_json::from_slice::<Value>(&payload) else {
                    warn!(
                        "ignoring non-JSON websocket message from codex app-server: {}",
                        String::from_utf8_lossy(&payload)
                    );
                    continue;
                };
                if !handle_codex_message(value, &state, &events_tx).await {
                    break "codex app-server event receiver closed".to_string();
                }
            }
            Ok(Some(WebSocketFrame::Ping(payload))) => {
                let mut writer = writer.lock().await;
                match &mut *writer {
                    CodexWriter::WebSocket(writer) => {
                        if let Err(error) = write_websocket_frame(writer, 0xA, &payload).await {
                            let message = format!(
                                "failed to write codex app-server websocket pong: {error:#}"
                            );
                            warn!("{message}");
                            break message;
                        }
                    }
                    CodexWriter::Stdio(_) => {
                        break "codex app-server websocket reader has stdio writer".to_string();
                    }
                }
            }
            Ok(Some(WebSocketFrame::Close)) | Ok(None) => {
                break "codex app-server websocket closed".to_string();
            }
            Ok(Some(WebSocketFrame::Pong(payload))) => {
                debug!(
                    "received codex app-server websocket pong with {} bytes",
                    payload.len()
                );
            }
            Ok(Some(WebSocketFrame::Other)) => {}
            Err(error) => {
                let message = format!("failed to read codex app-server websocket: {error:#}");
                warn!("{message}");
                break message;
            }
        }
    };
    close_connection_state(&state, close_reason).await;
}

async fn handle_codex_message(
    value: Value,
    state: &SharedConnectionState,
    events_tx: &mpsc::Sender<CodexEvent>,
) -> bool {
    if let Some(id) = value.get("id").and_then(Value::as_u64)
        && (value.get("result").is_some() || value.get("error").is_some())
    {
        let sender = state.lock().await.pending.remove(&id);
        if let Some(sender) = sender {
            let result = if let Some(result) = value.get("result") {
                Ok(result.clone())
            } else {
                let error = value
                    .get("error")
                    .cloned()
                    .unwrap_or_else(|| json!({"message":"unknown JSON-RPC error"}));
                Err(anyhow!("JSON-RPC error: {error}"))
            };
            let _ = sender.send(result);
        }
        return true;
    }

    let Some(method) = value.get("method").and_then(Value::as_str) else {
        warn!("ignoring codex app-server payload without method: {value}");
        return true;
    };
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    let event = if let Some(id) = value.get("id") {
        CodexEvent::ServerRequest {
            id: id.clone(),
            method: method.to_string(),
            params,
        }
    } else {
        CodexEvent::Notification {
            method: method.to_string(),
            params,
        }
    };
    events_tx.send(event).await.is_ok()
}

async fn close_connection_state(state: &SharedConnectionState, message: String) {
    let (senders, reason) = {
        let mut state = state.lock().await;
        if state.closed.is_none() {
            state.closed = Some(message.clone());
        }
        let reason = state.closed.clone().unwrap_or(message);
        let senders = state.pending.drain().collect::<Vec<_>>();
        (senders, reason)
    };
    for (_, sender) in senders {
        let _ = sender.send(Err(anyhow!(reason.clone())));
    }
}

async fn read_stderr(stderr: impl tokio::io::AsyncRead + Unpin) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        warn!("codex app-server stderr: {line}");
    }
}

fn parse_loopback_websocket_url(input: &str) -> Result<Url> {
    let url = Url::parse(input).with_context(|| format!("invalid codex app-server URL {input}"))?;
    if url.scheme() != "ws" {
        bail!("codex app-server URL must use ws://");
    }
    let host = websocket_host(&url)?;
    let loopback = host == "localhost"
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !loopback {
        bail!("codex app-server URL must use a loopback host");
    }
    Ok(url)
}

async fn connect_loopback_websocket(host: &str, port: u16, url: &Url) -> Result<TcpStream> {
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("failed to resolve codex app-server host {host}"))?
        .filter(|address| address.ip().is_loopback())
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        bail!("codex app-server URL resolved to no loopback addresses");
    }

    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .map(anyhow::Error::new)
        .unwrap_or_else(|| anyhow!("failed to connect codex app-server at {url}")))
    .with_context(|| format!("failed to connect codex app-server at {url}"))
}

fn websocket_host(url: &Url) -> Result<String> {
    url.host_str()
        .map(|host| {
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .to_string()
        })
        .context("websocket URL missing host")
}

async fn websocket_handshake(stream: &mut TcpStream, url: &Url) -> Result<()> {
    let host = websocket_host(url)?;
    let port = url
        .port_or_known_default()
        .context("websocket URL missing port")?;
    let host_header = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    let authority = match url.port() {
        Some(_) => format!("{host_header}:{port}"),
        None => host_header,
    };
    let mut path = url.path().to_string();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }

    let key = base64::engine::general_purpose::STANDARD.encode(rand::random::<[u8; 16]>());
    let request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {authority}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\
         \r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to write websocket handshake")?;

    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .context("failed to read websocket handshake")?;
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
        if response.len() > 16 * 1024 {
            bail!("codex app-server websocket handshake response is too large");
        }
    }

    let response = String::from_utf8_lossy(&response);
    validate_websocket_handshake_response(&response, &key)
}

fn validate_websocket_handshake_response(response: &str, websocket_key: &str) -> Result<()> {
    let mut lines = response.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    if !status_line.starts_with("HTTP/1.1 101 ") && !status_line.starts_with("HTTP/1.0 101 ") {
        bail!("codex app-server websocket handshake failed: {status_line}");
    }
    let expected_accept = websocket_accept_key(websocket_key);
    let accept = lines
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("Sec-WebSocket-Accept")
                .then(|| value.trim())
        })
        .context("codex app-server websocket handshake missing accept header")?;
    if accept != expected_accept {
        bail!("codex app-server websocket handshake accept header did not match request key");
    }
    Ok(())
}

fn websocket_accept_key(websocket_key: &str) -> String {
    let mut material = Vec::with_capacity(websocket_key.len() + WEBSOCKET_GUID.len());
    material.extend_from_slice(websocket_key.as_bytes());
    material.extend_from_slice(WEBSOCKET_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(sha1_digest(&material))
}

fn sha1_digest(input: &[u8]) -> [u8; 20] {
    let mut message = input.to_vec();
    let bit_len = (message.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    let mut h0 = 0x67452301_u32;
    let mut h1 = 0xEFCDAB89_u32;
    let mut h2 = 0x98BADCFE_u32;
    let mut h3 = 0x10325476_u32;
    let mut h4 = 0xC3D2E1F0_u32;

    for chunk in message.chunks_exact(64) {
        let mut words = [0_u32; 80];
        for (idx, word) in words[..16].iter_mut().enumerate() {
            let offset = idx * 4;
            *word = u32::from_be_bytes([
                chunk[offset],
                chunk[offset + 1],
                chunk[offset + 2],
                chunk[offset + 3],
            ]);
        }
        for idx in 16..80 {
            words[idx] = (words[idx - 3] ^ words[idx - 8] ^ words[idx - 14] ^ words[idx - 16])
                .rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;
        for (idx, word) in words.iter().enumerate() {
            let (f, k) = match idx {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut digest = [0_u8; 20];
    for (idx, word) in [h0, h1, h2, h3, h4].iter().enumerate() {
        digest[idx * 4..idx * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

enum WebSocketFrame {
    Text(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
    Other,
}

#[derive(Default)]
struct WebSocketReadState {
    partial_text: Option<Vec<u8>>,
}

struct RawWebSocketFrame {
    fin: bool,
    opcode: u8,
    payload: Vec<u8>,
}

async fn read_websocket_frame<R>(
    reader: &mut R,
    state: &mut WebSocketReadState,
) -> Result<Option<WebSocketFrame>>
where
    R: AsyncRead + Unpin,
{
    loop {
        let Some(frame) = read_raw_websocket_frame(reader).await? else {
            return Ok(None);
        };
        match frame.opcode {
            0x1 => {
                if state.partial_text.is_some() {
                    bail!("received new websocket text frame before continuation completed");
                }
                if frame.fin {
                    return Ok(Some(WebSocketFrame::Text(frame.payload)));
                }
                state.partial_text = Some(frame.payload);
            }
            0x0 => {
                let Some(partial) = state.partial_text.as_mut() else {
                    bail!("received websocket continuation without fragmented text frame");
                };
                if partial.len().saturating_add(frame.payload.len()) > MAX_WEBSOCKET_FRAME_BYTES {
                    bail!("codex app-server websocket fragmented message exceeds maximum size");
                }
                partial.extend_from_slice(&frame.payload);
                if frame.fin {
                    let payload = state.partial_text.take().unwrap_or_default();
                    return Ok(Some(WebSocketFrame::Text(payload)));
                }
            }
            0x8 => return Ok(Some(WebSocketFrame::Close)),
            0x9 => return Ok(Some(WebSocketFrame::Ping(frame.payload))),
            0xA => return Ok(Some(WebSocketFrame::Pong(frame.payload))),
            _ if frame.fin => return Ok(Some(WebSocketFrame::Other)),
            _ => bail!("fragmented non-text codex app-server websocket frames are not supported"),
        }
    }
}

async fn read_raw_websocket_frame<R>(reader: &mut R) -> Result<Option<RawWebSocketFrame>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; 2];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error).context("failed to read websocket frame header"),
    }

    let opcode = header[0] & 0x0f;
    let fin = (header[0] & 0x80) != 0;
    let masked = (header[1] & 0x80) != 0;
    let mut length = u64::from(header[1] & 0x7f);
    if length == 126 {
        let mut extended = [0_u8; 2];
        reader
            .read_exact(&mut extended)
            .await
            .context("failed to read websocket 16-bit length")?;
        length = u64::from(u16::from_be_bytes(extended));
    } else if length == 127 {
        let mut extended = [0_u8; 8];
        reader
            .read_exact(&mut extended)
            .await
            .context("failed to read websocket 64-bit length")?;
        length = u64::from_be_bytes(extended);
    }
    if length as usize > MAX_WEBSOCKET_FRAME_BYTES {
        bail!("codex app-server websocket frame exceeds maximum size");
    }
    if opcode & 0x08 != 0 && (!fin || length > 125) {
        bail!("codex app-server websocket control frame exceeds maximum size");
    }

    let mask = if masked {
        let mut key = [0_u8; 4];
        reader
            .read_exact(&mut key)
            .await
            .context("failed to read websocket mask")?;
        Some(key)
    } else {
        None
    };
    let mut payload = vec![0_u8; length as usize];
    reader
        .read_exact(&mut payload)
        .await
        .context("failed to read websocket payload")?;
    if let Some(mask) = mask {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }

    Ok(Some(RawWebSocketFrame {
        fin,
        opcode,
        payload,
    }))
}

async fn write_websocket_text_frame(writer: &mut OwnedWriteHalf, payload: &[u8]) -> Result<()> {
    write_websocket_frame(writer, 0x1, payload).await
}

async fn write_websocket_frame(
    writer: &mut OwnedWriteHalf,
    opcode: u8,
    payload: &[u8],
) -> Result<()> {
    if payload.len() > MAX_WEBSOCKET_FRAME_BYTES {
        bail!("codex app-server websocket payload exceeds maximum size");
    }
    if opcode & 0x08 != 0 && payload.len() > 125 {
        bail!("codex app-server websocket control payload exceeds maximum size");
    }
    let mut frame = Vec::with_capacity(payload.len() + 14);
    frame.push(0x80 | (opcode & 0x0F));
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else if u16::try_from(payload.len()).is_ok() {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    let mask = rand::random::<[u8; 4]>();
    frame.extend_from_slice(&mask);
    for (index, byte) in payload.iter().enumerate() {
        frame.push(*byte ^ mask[index % 4]);
    }
    writer
        .write_all(&frame)
        .await
        .context("failed to write websocket frame")?;
    writer
        .flush()
        .await
        .context("failed to flush websocket frame")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn websocket_client_sends_json_rpc_to_loopback_app_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut handshake = Vec::new();
            let mut buffer = [0_u8; 512];
            loop {
                let read = stream.read(&mut buffer).await.expect("read handshake");
                assert!(read > 0);
                handshake.extend_from_slice(&buffer[..read]);
                if handshake.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let handshake = String::from_utf8(handshake).expect("handshake utf8");
            assert!(handshake.starts_with("GET / HTTP/1.1"));
            let websocket_key = handshake
                .split("\r\n")
                .filter_map(|line| line.split_once(':'))
                .find_map(|(name, value)| {
                    name.eq_ignore_ascii_case("Sec-WebSocket-Key")
                        .then(|| value.trim().to_string())
                })
                .expect("websocket key");
            let websocket_accept = websocket_accept_key(&websocket_key);
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 101 Switching Protocols\r\n\
                     Upgrade: websocket\r\n\
                     Connection: Upgrade\r\n\
                     Sec-WebSocket-Accept: {websocket_accept}\r\n\
                     \r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("write handshake");

            let mut read_state = WebSocketReadState::default();
            let frame = read_websocket_frame(&mut stream, &mut read_state)
                .await
                .expect("read frame")
                .expect("frame");
            let WebSocketFrame::Text(payload) = frame else {
                panic!("expected text frame");
            };
            let request: Value = serde_json::from_slice(&payload).expect("request json");
            assert_eq!(request["method"], "initialize");
            let id = request["id"].clone();
            write_unmasked_frame(
                &mut stream,
                0x1,
                &serde_json::to_vec(&json!({
                    "id": id,
                    "result": {
                        "ok": true
                    }
                }))
                .expect("response json"),
            )
            .await;
        });

        let mut client = CodexClient::connect_websocket(&format!("ws://{address}/"))
            .await
            .expect("connect websocket");
        let response = client
            .initialize("wxcd-test", true)
            .await
            .expect("initialize");

        assert_eq!(response["ok"], true);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn websocket_client_reads_fragmented_json_rpc_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            accept_test_handshake(&mut stream).await;
            let mut read_state = WebSocketReadState::default();
            let frame = read_websocket_frame(&mut stream, &mut read_state)
                .await
                .expect("read frame")
                .expect("frame");
            let WebSocketFrame::Text(payload) = frame else {
                panic!("expected text frame");
            };
            let request: Value = serde_json::from_slice(&payload).expect("request json");
            assert_eq!(request["method"], "initialize");
            let id = request["id"].clone();
            let response = serde_json::to_vec(&json!({
                "id": id,
                "result": {
                    "ok": true
                }
            }))
            .expect("response json");
            let split = response.len() / 2;
            write_unmasked_frame_with_fin(&mut stream, 0x1, false, &response[..split]).await;
            write_unmasked_frame_with_fin(&mut stream, 0x0, true, &response[split..]).await;
        });

        let mut client = CodexClient::connect_websocket(&format!("ws://{address}/"))
            .await
            .expect("connect websocket");
        let response = client
            .initialize("wxcd-test", true)
            .await
            .expect("initialize");

        assert_eq!(response["ok"], true);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn websocket_client_fails_pending_request_when_server_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            accept_test_handshake(&mut stream).await;
            let mut read_state = WebSocketReadState::default();
            let frame = read_websocket_frame(&mut stream, &mut read_state)
                .await
                .expect("read frame")
                .expect("frame");
            let WebSocketFrame::Text(payload) = frame else {
                panic!("expected text frame");
            };
            let request: Value = serde_json::from_slice(&payload).expect("request json");
            assert_eq!(request["method"], "initialize");
        });

        let mut client = CodexClient::connect_websocket(&format!("ws://{address}/"))
            .await
            .expect("connect websocket");
        let result =
            tokio::time::timeout(Duration::from_secs(1), client.initialize("wxcd-test", true))
                .await
                .expect("request completed");

        assert!(
            format!("{:#}", result.expect_err("closed websocket should fail")).contains("closed")
        );
        let later = tokio::time::timeout(Duration::from_secs(1), client.thread_list(false))
            .await
            .expect("later request completed");
        assert!(
            format!(
                "{:#}",
                later.expect_err("closed websocket should stay closed")
            )
            .contains("closed")
        );
        server.await.expect("server");
    }

    #[tokio::test]
    async fn websocket_client_replies_to_ping_with_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            accept_test_handshake(&mut stream).await;
            write_unmasked_frame(&mut stream, 0x9, b"keepalive").await;
            let mut read_state = WebSocketReadState::default();
            let frame = read_websocket_frame(&mut stream, &mut read_state)
                .await
                .expect("read frame")
                .expect("frame");
            let WebSocketFrame::Pong(payload) = frame else {
                panic!("expected pong frame");
            };
            assert_eq!(payload, b"keepalive");
        });

        let _client = CodexClient::connect_websocket(&format!("ws://{address}/"))
            .await
            .expect("connect websocket");

        server.await.expect("server");
    }

    #[test]
    fn websocket_url_must_be_loopback_ws() {
        assert!(parse_loopback_websocket_url("ws://127.0.0.1:1234").is_ok());
        assert!(parse_loopback_websocket_url("ws://localhost:1234").is_ok());
        assert!(parse_loopback_websocket_url("ws://[::1]:1234").is_ok());
        assert!(parse_loopback_websocket_url("wss://127.0.0.1:1234").is_err());
        assert!(parse_loopback_websocket_url("ws://192.0.2.1:1234").is_err());
    }

    #[test]
    fn websocket_accept_key_matches_rfc_sample() {
        assert_eq!(
            websocket_accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    async fn accept_test_handshake(stream: &mut TcpStream) {
        let mut handshake = Vec::new();
        let mut buffer = [0_u8; 512];
        loop {
            let read = stream.read(&mut buffer).await.expect("read handshake");
            assert!(read > 0);
            handshake.extend_from_slice(&buffer[..read]);
            if handshake.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let handshake = String::from_utf8(handshake).expect("handshake utf8");
        assert!(handshake.starts_with("GET / HTTP/1.1"));
        let websocket_key = handshake
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .find_map(|(name, value)| {
                name.eq_ignore_ascii_case("Sec-WebSocket-Key")
                    .then(|| value.trim().to_string())
            })
            .expect("websocket key");
        let websocket_accept = websocket_accept_key(&websocket_key);
        stream
            .write_all(
                format!(
                    "HTTP/1.1 101 Switching Protocols\r\n\
                     Upgrade: websocket\r\n\
                     Connection: Upgrade\r\n\
                     Sec-WebSocket-Accept: {websocket_accept}\r\n\
                     \r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("write handshake");
    }

    async fn write_unmasked_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
        write_unmasked_frame_with_fin(stream, opcode, true, payload).await;
    }

    async fn write_unmasked_frame_with_fin(
        stream: &mut TcpStream,
        opcode: u8,
        fin: bool,
        payload: &[u8],
    ) {
        let mut frame = Vec::with_capacity(payload.len() + 10);
        frame.push(if fin { 0x80 } else { 0x00 } | (opcode & 0x0F));
        if payload.len() < 126 {
            frame.push(payload.len() as u8);
        } else if u16::try_from(payload.len()).is_ok() {
            frame.push(126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            frame.push(127);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        frame.extend_from_slice(payload);
        stream.write_all(&frame).await.expect("write frame");
        stream.flush().await.expect("flush frame");
    }
}
