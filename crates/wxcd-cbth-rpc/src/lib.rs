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
pub const PLUGIN_RPC_APP_SERVER_ENSURE_METHOD: &str = "app_server.ensure";
pub const PLUGIN_RPC_APP_SERVER_REFRESH_METHOD: &str = "app_server.refresh";
pub const PLUGIN_RPC_APP_SERVER_STOP_METHOD: &str = "app_server.stop";
pub const PLUGIN_RPC_DELIVERY_ENQUEUE_METHOD: &str = "delivery.enqueue";
pub const PLUGIN_RPC_DELIVERY_INSPECT_METHOD: &str = "delivery.inspect";
pub const PLUGIN_RPC_DELIVERY_MANUALIZE_METHOD: &str = "delivery.manualize";
pub const PLUGIN_RPC_PLUGIN_HEALTH_CHECK_METHOD: &str = "plugin.health_check";
pub const PLUGIN_RPC_PLUGIN_QUIESCE_METHOD: &str = "plugin.quiesce";
pub const PLUGIN_RPC_PLUGIN_DRAIN_METHOD: &str = "plugin.drain";
pub const PLUGIN_RPC_PLUGIN_SHUTDOWN_METHOD: &str = "plugin.shutdown";
pub const PLUGIN_RPC_PLUGIN_HANDOFF_EXPORT_METHOD: &str = "plugin.handoff_export";
pub const PLUGIN_RPC_PLUGIN_HANDOFF_IMPORT_METHOD: &str = "plugin.handoff_import";
pub const PLUGIN_RPC_PLUGIN_UNQUIESCE_METHOD: &str = "plugin.unquiesce";
pub const PLUGIN_RPC_PLUGIN_LIFECYCLE_CAPABILITY: &str = "plugin-lifecycle-v1";
pub const PLUGIN_RPC_PLUGIN_HANDOFF_CAPABILITY: &str = "plugin-handoff-v1";
pub const SERVICE_CAPABILITY_DELIVERY_OWNED_CODEX_APP_SERVER_TARGET_V1: &str =
    "delivery-owned-codex-app-server-target-v1";

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerEnsureRequest {
    pub managed_session_id: String,
    pub bound_thread_id: String,
    pub session_epoch: i64,
    pub codex_binary: String,
    pub lease_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_ttl_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerRefreshRequest {
    pub managed_session_id: String,
    pub lease_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_ttl_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerStopRequest {
    pub managed_session_id: String,
    pub lease_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDeliveryTarget {
    pub driver: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_server_lease_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_epoch: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codex_binary: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDeliveryArtifactReference {
    pub artifact_id: String,
    pub relative_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_until: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDeliveryEnqueueRequest {
    pub source_thread_id: String,
    pub summary: String,
    pub idempotency_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<PluginDeliveryArtifactReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_policy: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_delivery_attempts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redelivery_window_seconds: Option<i64>,
    pub target: PluginDeliveryTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_metadata: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDeliveryInspectRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_server_lease_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDeliveryManualizeRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub manualize_key: String,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginLifecycleMode {
    PreActive,
    Active,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHealthCheckRequest {
    pub mode: PluginLifecycleMode,
    pub allow_external_side_effects: bool,
    pub allow_cursor_advance: bool,
    pub allow_delivery_commit: bool,
}

impl PluginHealthCheckRequest {
    pub fn pre_active() -> Self {
        Self {
            mode: PluginLifecycleMode::PreActive,
            allow_external_side_effects: false,
            allow_cursor_advance: false,
            allow_delivery_commit: false,
        }
    }

    pub fn active() -> Self {
        Self {
            mode: PluginLifecycleMode::Active,
            allow_external_side_effects: true,
            allow_cursor_advance: true,
            allow_delivery_commit: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHealthCheckResponse {
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginQuiesceRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDrainRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDrainResponse {
    pub drained: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight_count: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginShutdownRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginUnquiesceRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHandoffSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHandoffExportRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHandoffExportResponse {
    pub snapshot: PluginHandoffSnapshot,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHandoffImportRequest {
    pub reason: String,
    pub snapshot: PluginHandoffSnapshot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginLifecycleAckResponse {
    pub accepted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerLeaseResponse {
    pub lease_id: String,
    pub daemon: PluginAppServerDaemon,
    pub app_server: PluginAppServerInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_ensure: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerStopResponse {
    pub lease_id: String,
    pub daemon: PluginAppServerDaemon,
    pub stopped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handoff_daemon_socket_path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerDaemon {
    pub socket_path: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginAppServerInfo {
    pub managed_session_id: String,
    pub bound_thread_id: String,
    pub session_epoch: i64,
    pub url: String,
    pub pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid_identity: Option<String>,
    pub started_at: i64,
    pub lease_seconds_remaining: u64,
    pub ownership: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_daemon_socket_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handoff_daemon_socket_path: Option<String>,
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
    pub fn new(kind: PluginRpcErrorKind, message: impl Into<String>) -> Self {
        let retryable = matches!(
            kind,
            PluginRpcErrorKind::Io | PluginRpcErrorKind::TransientDaemonUnavailable
        );
        Self {
            kind,
            message: message.into(),
            retryable,
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

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
        let result = self
            .request_value(
                PLUGIN_RPC_HELLO_METHOD,
                serde_json::to_value(request).context("failed to encode plugin hello request")?,
            )
            .await
            .context("cbth plugin hello failed")?;
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

    pub async fn app_server_ensure(
        &mut self,
        request: PluginAppServerEnsureRequest,
    ) -> Result<PluginAppServerLeaseResponse> {
        self.request(
            PLUGIN_RPC_APP_SERVER_ENSURE_METHOD,
            request,
            "failed to decode app_server.ensure response",
        )
        .await
        .context("cbth app_server.ensure failed")
    }

    pub async fn app_server_refresh(
        &mut self,
        request: PluginAppServerRefreshRequest,
    ) -> Result<PluginAppServerLeaseResponse> {
        self.request(
            PLUGIN_RPC_APP_SERVER_REFRESH_METHOD,
            request,
            "failed to decode app_server.refresh response",
        )
        .await
        .context("cbth app_server.refresh failed")
    }

    pub async fn app_server_stop(
        &mut self,
        request: PluginAppServerStopRequest,
    ) -> Result<PluginAppServerStopResponse> {
        self.request(
            PLUGIN_RPC_APP_SERVER_STOP_METHOD,
            request,
            "failed to decode app_server.stop response",
        )
        .await
        .context("cbth app_server.stop failed")
    }

    pub async fn delivery_enqueue(
        &mut self,
        request: PluginDeliveryEnqueueRequest,
    ) -> Result<Value> {
        self.request_value(
            PLUGIN_RPC_DELIVERY_ENQUEUE_METHOD,
            serde_json::to_value(request).context("failed to encode delivery.enqueue request")?,
        )
        .await
        .context("cbth delivery.enqueue failed")
    }

    pub async fn plugin_handoff_export(
        &mut self,
        request: PluginHandoffExportRequest,
    ) -> Result<PluginHandoffExportResponse> {
        self.request(
            PLUGIN_RPC_PLUGIN_HANDOFF_EXPORT_METHOD,
            request,
            "failed to decode plugin.handoff_export response",
        )
        .await
        .context("cbth plugin.handoff_export failed")
    }

    pub async fn plugin_handoff_import(
        &mut self,
        request: PluginHandoffImportRequest,
    ) -> Result<PluginLifecycleAckResponse> {
        self.request(
            PLUGIN_RPC_PLUGIN_HANDOFF_IMPORT_METHOD,
            request,
            "failed to decode plugin.handoff_import response",
        )
        .await
        .context("cbth plugin.handoff_import failed")
    }

    async fn request<T, U>(
        &mut self,
        method: &str,
        request: T,
        decode_context: &'static str,
    ) -> Result<U>
    where
        T: Serialize,
        U: DeserializeOwned,
    {
        let params = serde_json::to_value(request)
            .with_context(|| format!("failed to encode {method} request"))?;
        let result = self.request_value(method, params).await?;
        serde_json::from_value(result).context(decode_context)
    }

    async fn request_value(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_request_id();
        let frame = PluginRpcRequestFrame::new(id.clone(), method, params);
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
            return Err(error).with_context(|| format!("cbth plugin RPC {method} failed"));
        }
        response
            .result
            .ok_or_else(|| {
                PluginRpcError::malformed_frame(format!("{method} response missing result"))
            })
            .map_err(Into::into)
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

    #[tokio::test]
    async fn app_server_lease_methods_roundtrip_against_fake_uds_server() {
        let socket_path = test_socket_path("app-server");
        let listener = UnixListener::bind(&socket_path).expect("bind fake server");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");

            let ensure: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read ensure");
            assert_eq!(ensure.method, PLUGIN_RPC_APP_SERVER_ENSURE_METHOD);
            let ensure_params: PluginAppServerEnsureRequest =
                serde_json::from_value(ensure.params).expect("decode ensure");
            assert_eq!(ensure_params.managed_session_id, "managed-1");
            assert_eq!(ensure_params.bound_thread_id, "thread-1");
            assert_eq!(ensure_params.session_epoch, 7);
            assert_eq!(ensure_params.codex_binary, "codex");
            assert_eq!(ensure_params.lease_id, "lease-1");
            assert_eq!(ensure_params.lease_ttl_seconds, Some(120));
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::success(
                    ensure.id,
                    app_server_result("lease-1", "managed-1", "thread-1", 7),
                ),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write ensure response");

            let refresh: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read refresh");
            assert_eq!(refresh.method, PLUGIN_RPC_APP_SERVER_REFRESH_METHOD);
            let refresh_params: PluginAppServerRefreshRequest =
                serde_json::from_value(refresh.params).expect("decode refresh");
            assert_eq!(refresh_params.managed_session_id, "managed-1");
            assert_eq!(refresh_params.lease_id, "lease-1");
            assert_eq!(refresh_params.lease_ttl_seconds, Some(120));
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::success(
                    refresh.id,
                    app_server_result("lease-1", "managed-1", "thread-1", 7),
                ),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write refresh response");

            let stop: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read stop");
            assert_eq!(stop.method, PLUGIN_RPC_APP_SERVER_STOP_METHOD);
            let stop_params: PluginAppServerStopRequest =
                serde_json::from_value(stop.params).expect("decode stop");
            assert_eq!(stop_params.managed_session_id, "managed-1");
            assert_eq!(stop_params.lease_id, "lease-1");
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::success(
                    stop.id,
                    json!({
                        "lease_id": "lease-1",
                        "daemon": {
                            "socket_path": "/tmp/cbth.sock",
                        },
                        "stopped": true,
                        "handoff_daemon_socket_path": null,
                    }),
                ),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write stop response");
        });

        let mut client = PluginRpcClient::connect(&socket_path)
            .await
            .expect("connect");
        let ensure = client
            .app_server_ensure(PluginAppServerEnsureRequest {
                managed_session_id: "managed-1".to_string(),
                bound_thread_id: "thread-1".to_string(),
                session_epoch: 7,
                codex_binary: "codex".to_string(),
                lease_id: "lease-1".to_string(),
                lease_ttl_seconds: Some(120),
            })
            .await
            .expect("ensure");
        assert_eq!(ensure.app_server.url, "ws://127.0.0.1:1234");

        let refresh = client
            .app_server_refresh(PluginAppServerRefreshRequest {
                managed_session_id: "managed-1".to_string(),
                lease_id: "lease-1".to_string(),
                lease_ttl_seconds: Some(120),
            })
            .await
            .expect("refresh");
        assert_eq!(refresh.lease_id, "lease-1");

        let stop = client
            .app_server_stop(PluginAppServerStopRequest {
                managed_session_id: "managed-1".to_string(),
                lease_id: "lease-1".to_string(),
            })
            .await
            .expect("stop");
        assert!(stop.stopped);

        server.await.expect("server");
        std::fs::remove_file(socket_path).ok();
    }

    #[tokio::test]
    async fn delivery_enqueue_replays_idempotency_key_against_fake_uds_server() {
        let socket_path = test_socket_path("delivery-enqueue");
        let listener = UnixListener::bind(&socket_path).expect("bind fake server");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");

            for expected_state in ["accepted_observation_pending", "idempotent_replay"] {
                let frame: PluginRpcRequestFrame =
                    read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                        .await
                        .expect("read delivery enqueue");
                assert_eq!(frame.method, PLUGIN_RPC_DELIVERY_ENQUEUE_METHOD);
                let request: PluginDeliveryEnqueueRequest =
                    serde_json::from_value(frame.params).expect("decode delivery enqueue");
                assert_eq!(request.source_thread_id, "thread-1");
                assert_eq!(request.summary, "deliver async result");
                assert_eq!(request.idempotency_key, "webex-delivery:event-1");
                assert_eq!(request.target.driver, "codex_app_server");
                assert_eq!(request.target.app_server_lease_id, None);
                assert_eq!(request.target.codex_binary.as_deref(), Some("codex"));
                write_plugin_rpc_frame(
                    &mut stream,
                    &PluginRpcResponseFrame::success(
                        frame.id,
                        json!({
                            "delivery": {
                                "idempotency_key": request.idempotency_key,
                            },
                            "target": {
                                "driver": "codex_app_server",
                                "ownership": "delivery_owned",
                            },
                            "driver_state": expected_state,
                        }),
                    ),
                    PLUGIN_RPC_MAX_FRAME_BYTES,
                )
                .await
                .expect("write delivery enqueue response");
            }
        });

        let mut client = PluginRpcClient::connect(&socket_path)
            .await
            .expect("connect");
        let request = delivery_enqueue_request();
        let first = client
            .delivery_enqueue(request.clone())
            .await
            .expect("first enqueue");
        let replay = client
            .delivery_enqueue(request)
            .await
            .expect("replay enqueue");

        assert_eq!(first["driver_state"], "accepted_observation_pending");
        assert_eq!(replay["driver_state"], "idempotent_replay");
        server.await.expect("server");
        std::fs::remove_file(socket_path).ok();
    }

    #[tokio::test]
    async fn delivery_enqueue_preserves_retryable_rpc_error() {
        let socket_path = test_socket_path("delivery-error");
        let listener = UnixListener::bind(&socket_path).expect("bind fake server");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let frame: PluginRpcRequestFrame =
                read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
                    .await
                    .expect("read delivery enqueue");
            assert_eq!(frame.method, PLUGIN_RPC_DELIVERY_ENQUEUE_METHOD);
            write_plugin_rpc_frame(
                &mut stream,
                &PluginRpcResponseFrame::failure(
                    frame.id,
                    PluginRpcError::new(
                        PluginRpcErrorKind::TransientDaemonUnavailable,
                        "daemon is restarting",
                    ),
                ),
                PLUGIN_RPC_MAX_FRAME_BYTES,
            )
            .await
            .expect("write delivery enqueue error");
        });

        let mut client = PluginRpcClient::connect(&socket_path)
            .await
            .expect("connect");
        let error = client
            .delivery_enqueue(delivery_enqueue_request())
            .await
            .expect_err("enqueue should fail");
        let rpc_error = error
            .downcast_ref::<PluginRpcError>()
            .expect("plugin rpc error in chain");

        assert_eq!(
            rpc_error.kind,
            PluginRpcErrorKind::TransientDaemonUnavailable
        );
        assert!(rpc_error.retryable);
        server.await.expect("server");
        std::fs::remove_file(socket_path).ok();
    }

    #[test]
    fn lifecycle_pre_active_health_check_serializes_c7_contract() {
        let frame = PluginRpcRequestFrame::new(
            "lifecycle-1",
            PLUGIN_RPC_PLUGIN_HEALTH_CHECK_METHOD,
            serde_json::to_value(PluginHealthCheckRequest::pre_active()).unwrap(),
        );

        assert_eq!(frame.method, PLUGIN_RPC_PLUGIN_HEALTH_CHECK_METHOD);
        assert_eq!(
            frame.params,
            json!({
                "mode": "pre_active",
                "allow_external_side_effects": false,
                "allow_cursor_advance": false,
                "allow_delivery_commit": false,
            })
        );
    }

    #[test]
    fn lifecycle_drain_response_serializes_incomplete_count() {
        let response = PluginDrainResponse {
            drained: false,
            in_flight_count: Some(2),
        };

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "drained": false,
                "in_flight_count": 2,
            })
        );
    }

    #[test]
    fn lifecycle_handoff_requests_serialize_optional_c7_contract() {
        let snapshot = PluginHandoffSnapshot {
            schema_version: 1,
            payload: json!({
                "plugin_name": "webex-connector",
                "recent_webex_event_ids": ["event-1"],
            }),
        };
        let export = PluginRpcRequestFrame::new(
            "handoff-export-1",
            PLUGIN_RPC_PLUGIN_HANDOFF_EXPORT_METHOD,
            serde_json::to_value(PluginHandoffExportRequest {
                reason: "upgrade".to_string(),
            })
            .unwrap(),
        );
        let import = PluginRpcRequestFrame::new(
            "handoff-import-1",
            PLUGIN_RPC_PLUGIN_HANDOFF_IMPORT_METHOD,
            serde_json::to_value(PluginHandoffImportRequest {
                reason: "upgrade".to_string(),
                snapshot: snapshot.clone(),
            })
            .unwrap(),
        );

        assert_eq!(export.method, PLUGIN_RPC_PLUGIN_HANDOFF_EXPORT_METHOD);
        assert_eq!(export.params, json!({"reason": "upgrade"}));
        assert_eq!(import.method, PLUGIN_RPC_PLUGIN_HANDOFF_IMPORT_METHOD);
        assert_eq!(
            import.params,
            json!({
                "reason": "upgrade",
                "snapshot": {
                    "schema_version": 1,
                    "payload": {
                        "plugin_name": "webex-connector",
                        "recent_webex_event_ids": ["event-1"],
                    },
                },
            })
        );
    }

    fn delivery_enqueue_request() -> PluginDeliveryEnqueueRequest {
        PluginDeliveryEnqueueRequest {
            source_thread_id: "thread-1".to_string(),
            summary: "deliver async result".to_string(),
            idempotency_key: "webex-delivery:event-1".to_string(),
            inline_payload: Some(json!({
                "kind": "webex_async_notification",
                "text": "done",
            })),
            artifact: None,
            delivery_policy: None,
            max_delivery_attempts: Some(2),
            redelivery_window_seconds: Some(3600),
            target: PluginDeliveryTarget {
                driver: "codex_app_server".to_string(),
                app_server_lease_id: None,
                managed_session_id: None,
                session_epoch: None,
                codex_binary: Some("codex".to_string()),
            },
            plugin_metadata: Some(json!({
                "webex_event_id": "event-1",
            })),
        }
    }

    fn app_server_result(
        lease_id: &str,
        managed_session_id: &str,
        bound_thread_id: &str,
        session_epoch: i64,
    ) -> Value {
        json!({
            "lease_id": lease_id,
            "daemon": {
                "socket_path": "/tmp/cbth.sock",
            },
            "app_server": {
                "managed_session_id": managed_session_id,
                "bound_thread_id": bound_thread_id,
                "session_epoch": session_epoch,
                "url": "ws://127.0.0.1:1234",
                "pid": 12345,
                "pid_identity": "pid-start",
                "started_at": 1000,
                "lease_seconds_remaining": 60,
                "ownership": "owned",
            },
        })
    }
}
