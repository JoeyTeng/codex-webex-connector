use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, warn};

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
    child: Child,
    writer: Arc<Mutex<BufWriter<ChildStdin>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    next_id: u64,
    events_rx: mpsc::Receiver<CodexEvent>,
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

        let writer = Arc::new(Mutex::new(BufWriter::new(stdin)));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (events_tx, events_rx) = mpsc::channel(512);

        tokio::spawn(read_stdout(stdout, Arc::clone(&pending), events_tx));
        tokio::spawn(read_stderr(stderr));

        Ok(Self {
            child,
            writer,
            pending,
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
        if let Some(id) = self.child.id() {
            debug!("shutting down codex app-server pid={id}");
        }
        self.child.kill().await.ok();
        Ok(())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let request_id = self.next_id;
        self.next_id += 1;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id, tx);
        self.write_line(&json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }))
        .await?;
        rx.await
            .context("codex app-server request channel closed")?
            .with_context(|| format!("codex app-server request failed for {method}"))
    }

    async fn write_line(&mut self, value: &Value) -> Result<()> {
        let encoded = serde_json::to_vec(value).context("failed to encode JSON-RPC message")?;
        let mut writer = self.writer.lock().await;
        writer.write_all(&encoded).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }
}

async fn read_stdout(
    stdout: impl tokio::io::AsyncRead + Unpin,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    events_tx: mpsc::Sender<CodexEvent>,
) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    warn!("ignoring non-JSON stdout line from codex app-server: {line}");
                    continue;
                };

                if let Some(id) = value.get("id").and_then(Value::as_u64)
                    && (value.get("result").is_some() || value.get("error").is_some())
                {
                    let sender = pending.lock().await.remove(&id);
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
                    continue;
                }

                let Some(method) = value.get("method").and_then(Value::as_str) else {
                    warn!("ignoring codex app-server payload without method: {value}");
                    continue;
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
                if events_tx.send(event).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => {
                warn!("failed to read codex app-server stdout: {error:#}");
                break;
            }
        }
    }
}

async fn read_stderr(stderr: impl tokio::io::AsyncRead + Unpin) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        warn!("codex app-server stderr: {line}");
    }
}
