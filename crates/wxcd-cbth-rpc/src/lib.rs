use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;

pub const PLUGIN_RPC_PROTOCOL_VERSION_V1: u32 = 1;
pub const PLUGIN_RPC_SUPPORTED_PROTOCOL_VERSIONS: &[u32] = &[PLUGIN_RPC_PROTOCOL_VERSION_V1];
pub const PLUGIN_RPC_MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub const PLUGIN_RPC_HELLO_METHOD: &str = "plugin.hello";

const PLUGIN_RPC_JSONRPC_VERSION: &str = "2.0";
const FRAME_LENGTH_PREFIX_BYTES: usize = 4;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcRequestFrame {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl PluginRpcRequestFrame {
    pub fn new(id: impl Into<String>, method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: PLUGIN_RPC_JSONRPC_VERSION.to_owned(),
            id: id.into(),
            method: method.into(),
            params,
        }
    }

    pub fn plugin_hello(id: impl Into<String>, request: PluginHelloRequest) -> Result<Self> {
        Ok(Self::new(
            id,
            PLUGIN_RPC_HELLO_METHOD,
            serde_json::to_value(request).context("failed to encode plugin hello request")?,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcResponseFrame {
    pub jsonrpc: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<PluginRpcError>,
}

impl PluginRpcResponseFrame {
    pub fn success(id: impl Into<String>, result: Value) -> Self {
        Self {
            jsonrpc: PLUGIN_RPC_JSONRPC_VERSION.to_owned(),
            id: id.into(),
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(id: impl Into<String>, error: PluginRpcError) -> Self {
        Self {
            jsonrpc: PLUGIN_RPC_JSONRPC_VERSION.to_owned(),
            id: id.into(),
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHelloRequest {
    pub plugin_name: String,
    pub plugin_instance_id: String,
    pub plugin_release_id: String,
    pub protocol_versions: Vec<u32>,
    #[serde(default)]
    pub capabilities: Vec<PluginCapability>,
    pub plugin_home: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHelloResponse {
    pub protocol_version: u32,
    pub service_capabilities: Vec<ServiceCapability>,
    pub policy: PluginRpcPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_endpoint: Option<DaemonEndpointHint>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginCapability {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl PluginCapability {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
        }
    }

    pub fn versioned(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version: Some(version),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ServiceCapability {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl ServiceCapability {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
        }
    }

    pub fn versioned(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version: Some(version),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcPolicy {
    pub max_frame_bytes: usize,
    pub requires_idempotency_key: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DaemonEndpointHint {
    pub transport: String,
    pub endpoint: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRpcErrorKind {
    UnsupportedProtocol,
    MissingCapability,
    StaleLease,
    PolicyBlocked,
    TargetUnavailable,
    TransientDaemonUnavailable,
    MalformedFrame,
    FrameTooLarge,
    Io,
    InvalidRequest,
    MethodNotFound,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[error("{kind:?}: {message}")]
pub struct PluginRpcError {
    pub kind: PluginRpcErrorKind,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl PluginRpcError {
    pub fn malformed_frame(message: impl Into<String>) -> Self {
        Self {
            kind: PluginRpcErrorKind::MalformedFrame,
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub fn frame_too_large(frame_bytes: usize, max_frame_bytes: usize) -> Self {
        Self {
            kind: PluginRpcErrorKind::FrameTooLarge,
            message: format!(
                "plugin RPC frame is {frame_bytes} bytes, exceeds {max_frame_bytes} bytes"
            ),
            retryable: false,
            details: Some(json!({
                "frame_bytes": frame_bytes,
                "max_frame_bytes": max_frame_bytes,
            })),
        }
    }
}

pub struct PluginRpcClient {
    stream: UnixStream,
    max_frame_bytes: usize,
    next_id: u64,
}

impl PluginRpcClient {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self> {
        let socket_path = socket_path.as_ref();
        let stream = UnixStream::connect(socket_path).await.with_context(|| {
            format!(
                "failed to connect cbth plugin RPC socket {}",
                socket_path.display()
            )
        })?;
        Ok(Self {
            stream,
            max_frame_bytes: PLUGIN_RPC_MAX_FRAME_BYTES,
            next_id: 1,
        })
    }

    pub async fn plugin_hello(
        &mut self,
        request: PluginHelloRequest,
    ) -> Result<PluginHelloResponse> {
        let supported_protocol_versions = request.protocol_versions.clone();
        let id = self.next_request_id();
        let frame = PluginRpcRequestFrame::plugin_hello(id.clone(), request)?;
        write_plugin_rpc_frame(&mut self.stream, &frame, self.max_frame_bytes).await?;
        let response: PluginRpcResponseFrame =
            read_plugin_rpc_frame(&mut self.stream, self.max_frame_bytes).await?;
        if response.id != id {
            bail!(
                "cbth plugin RPC response id mismatch: expected {}, got {}",
                id,
                response.id
            );
        }
        if let Some(error) = response.error {
            return Err(error).context("cbth plugin hello failed");
        }
        let result = response.result.ok_or_else(|| {
            PluginRpcError::malformed_frame("plugin hello response missing result")
        })?;
        let response: PluginHelloResponse =
            serde_json::from_value(result).context("failed to decode plugin hello response")?;
        if !supported_protocol_versions.contains(&response.protocol_version) {
            return Err(PluginRpcError {
                kind: PluginRpcErrorKind::UnsupportedProtocol,
                message: format!(
                    "cbth selected unsupported plugin RPC protocol version {}",
                    response.protocol_version
                ),
                retryable: false,
                details: Some(json!({
                    "protocol_version": response.protocol_version,
                    "supported_protocol_versions": supported_protocol_versions,
                })),
            })
            .context("cbth plugin hello failed");
        }
        Ok(response)
    }

    fn next_request_id(&mut self) -> String {
        let id = self.next_id.to_string();
        self.next_id += 1;
        id
    }
}

pub async fn read_plugin_rpc_frame<R, T>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<T, PluginRpcError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    if max_frame_bytes == 0 {
        return Err(PluginRpcError::malformed_frame(
            "max frame byte budget must be greater than zero",
        ));
    }

    let mut prefix = [0_u8; FRAME_LENGTH_PREFIX_BYTES];
    reader
        .read_exact(&mut prefix)
        .await
        .map_err(|error| PluginRpcError {
            kind: PluginRpcErrorKind::Io,
            message: error.to_string(),
            retryable: false,
            details: None,
        })?;
    let frame_bytes = u32::from_be_bytes(prefix) as usize;
    if frame_bytes == 0 {
        return Err(PluginRpcError::malformed_frame(
            "plugin RPC frame has zero-length payload",
        ));
    }
    if frame_bytes > max_frame_bytes {
        return Err(PluginRpcError::frame_too_large(
            frame_bytes,
            max_frame_bytes,
        ));
    }

    let mut payload = vec![0_u8; frame_bytes];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|error| PluginRpcError {
            kind: PluginRpcErrorKind::Io,
            message: error.to_string(),
            retryable: false,
            details: None,
        })?;
    serde_json::from_slice(&payload)
        .map_err(|error| PluginRpcError::malformed_frame(format!("invalid JSON frame: {error}")))
}

pub async fn write_plugin_rpc_frame<W, T>(
    writer: &mut W,
    frame: &T,
    max_frame_bytes: usize,
) -> Result<(), PluginRpcError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    if max_frame_bytes == 0 {
        return Err(PluginRpcError::malformed_frame(
            "max frame byte budget must be greater than zero",
        ));
    }

    let payload = serde_json::to_vec(frame)
        .map_err(|error| PluginRpcError::malformed_frame(format!("encode JSON frame: {error}")))?;
    if payload.len() > max_frame_bytes {
        return Err(PluginRpcError::frame_too_large(
            payload.len(),
            max_frame_bytes,
        ));
    }
    if payload.len() > u32::MAX as usize {
        return Err(PluginRpcError::frame_too_large(
            payload.len(),
            u32::MAX as usize,
        ));
    }

    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .map_err(|error| PluginRpcError {
            kind: PluginRpcErrorKind::Io,
            message: error.to_string(),
            retryable: true,
            details: None,
        })?;
    writer
        .write_all(&payload)
        .await
        .map_err(|error| PluginRpcError {
            kind: PluginRpcErrorKind::Io,
            message: error.to_string(),
            retryable: true,
            details: None,
        })?;
    writer.flush().await.map_err(|error| PluginRpcError {
        kind: PluginRpcErrorKind::Io,
        message: error.to_string(),
        retryable: true,
        details: None,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;
    use tokio::net::UnixListener;

    use super::*;

    fn hello_request(protocol_versions: Vec<u32>) -> PluginHelloRequest {
        PluginHelloRequest {
            plugin_name: "webex-connector".to_owned(),
            plugin_instance_id: "instance-1".to_owned(),
            plugin_release_id: "release-1".to_owned(),
            protocol_versions,
            capabilities: vec![PluginCapability::new("diagnostics")],
            plugin_home: "/tmp/webex-connector".to_owned(),
            pid: 42,
        }
    }

    fn test_socket_path(name: &str) -> PathBuf {
        PathBuf::from("/tmp").join(format!(
            "wxcd-{name}-{}-{}.sock",
            std::process::id(),
            rand_suffix()
        ))
    }

    fn rand_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    #[tokio::test]
    async fn plugin_hello_succeeds_against_fake_uds_server() {
        let socket_path = test_socket_path("success");
        let listener = UnixListener::bind(&socket_path).expect("bind fake server");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let request: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read request");
            assert_eq!(request.method, PLUGIN_RPC_HELLO_METHOD);
            let hello: PluginHelloRequest =
                serde_json::from_value(request.params).expect("decode hello");
            assert_eq!(
                hello.protocol_versions,
                vec![PLUGIN_RPC_PROTOCOL_VERSION_V1]
            );
            let response = PluginHelloResponse {
                protocol_version: PLUGIN_RPC_PROTOCOL_VERSION_V1,
                service_capabilities: vec![ServiceCapability::new("plugin-hello")],
                policy: PluginRpcPolicy {
                    max_frame_bytes: PLUGIN_RPC_MAX_FRAME_BYTES,
                    requires_idempotency_key: true,
                },
                daemon_endpoint: Some(DaemonEndpointHint {
                    transport: "uds".to_owned(),
                    endpoint: "/tmp/cbth-daemon.sock".to_owned(),
                }),
            };
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::success(
                    request.id,
                    serde_json::to_value(response).unwrap(),
                ),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write response");
        });

        let mut client = PluginRpcClient::connect(&socket_path)
            .await
            .expect("connect");
        let response = client
            .plugin_hello(hello_request(vec![PLUGIN_RPC_PROTOCOL_VERSION_V1]))
            .await
            .expect("hello");

        assert_eq!(response.protocol_version, PLUGIN_RPC_PROTOCOL_VERSION_V1);
        assert_eq!(
            response.daemon_endpoint.unwrap().endpoint,
            "/tmp/cbth-daemon.sock"
        );
        server.await.expect("server");
        std::fs::remove_file(socket_path).ok();
    }

    #[tokio::test]
    async fn plugin_hello_reports_server_error() {
        let socket_path = test_socket_path("failure");
        let listener = UnixListener::bind(&socket_path).expect("bind fake server");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let request: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read request");
            let error = PluginRpcError {
                kind: PluginRpcErrorKind::UnsupportedProtocol,
                message: "no common protocol version".to_owned(),
                retryable: false,
                details: Some(json!({"service_protocol_versions": [1]})),
            };
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::failure(request.id, error),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write response");
        });

        let mut client = PluginRpcClient::connect(&socket_path)
            .await
            .expect("connect");
        let error = client
            .plugin_hello(hello_request(vec![999]))
            .await
            .expect_err("hello should fail");

        assert!(format!("{error:#}").contains("UnsupportedProtocol"));
        server.await.expect("server");
        std::fs::remove_file(socket_path).ok();
    }

    #[tokio::test]
    async fn plugin_hello_rejects_unsupported_success_protocol() {
        let socket_path = test_socket_path("unsupported");
        let listener = UnixListener::bind(&socket_path).expect("bind fake server");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let request: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read request");
            let response = PluginHelloResponse {
                protocol_version: 999,
                service_capabilities: vec![ServiceCapability::new("plugin-hello")],
                policy: PluginRpcPolicy {
                    max_frame_bytes: PLUGIN_RPC_MAX_FRAME_BYTES,
                    requires_idempotency_key: false,
                },
                daemon_endpoint: None,
            };
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::success(
                    request.id,
                    serde_json::to_value(response).unwrap(),
                ),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write response");
        });

        let mut client = PluginRpcClient::connect(&socket_path)
            .await
            .expect("connect");
        let error = client
            .plugin_hello(hello_request(vec![PLUGIN_RPC_PROTOCOL_VERSION_V1]))
            .await
            .expect_err("unsupported protocol should fail");

        let error = format!("{error:#}");
        assert!(error.contains("UnsupportedProtocol"));
        assert!(error.contains("unsupported plugin RPC protocol version 999"));
        server.await.expect("server");
        std::fs::remove_file(socket_path).ok();
    }
}
