use std::fs::Permissions;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, sleep, timeout};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use wxcd_cbth_rpc::{
    PLUGIN_RPC_PROTOCOL_VERSION_V1, PluginAppServerEnsureRequest, PluginAppServerRefreshRequest,
    PluginAppServerStopRequest, PluginCapability, PluginDeliveryEnqueueRequest,
    PluginDeliveryTarget, PluginHelloRequest, PluginHelloResponse, PluginRpcClient, PluginRpcError,
    PluginRpcErrorKind, PluginRpcRequestFrame, PluginRpcResponseFrame,
    SERVICE_CAPABILITY_DELIVERY_OWNED_CODEX_APP_SERVER_TARGET_V1, read_plugin_rpc_frame,
    write_plugin_rpc_frame,
};
use wxcd_proto::{AppConfig, CbthPluginConfig};

const PLUGIN_RPC_STARTUP_TIMEOUT: Duration = Duration::from_secs(3);
const PLUGIN_APP_SERVER_LEASE_TTL_SECONDS: u64 = 120;
const PLUGIN_APP_SERVER_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const PLUGIN_APP_SERVER_REFRESH_SAFETY_MARGIN_SECONDS: u64 = 5;
const PLUGIN_APP_SERVER_ENSURE_TIMEOUT: Duration = Duration::from_secs(20);
const PLUGIN_APP_SERVER_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const PLUGIN_APP_SERVER_TASK_STOP_TIMEOUT: Duration = Duration::from_secs(6);
const PLUGIN_DELIVERY_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(20);
const PLUGIN_DELIVERY_BROKER_FRAME_TIMEOUT: Duration = Duration::from_secs(3);
const PLUGIN_DELIVERY_BROKER_TASK_STOP_TIMEOUT: Duration = Duration::from_secs(6);
const PLUGIN_DELIVERY_BROKER_SOCKET_DIR: &str = "/tmp";
const PLUGIN_DELIVERY_BROKER_SOCKET_MODE: u32 = 0o600;
const WXCD_CODEX_APP_SERVER_URL_ENV: &str = "WXCD_CODEX_APP_SERVER_URL";
const WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV: &str = "WXCD_CBTH_DELIVERY_BROKER_SOCKET";

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Subcommand)]
enum CommandKind {
    Run,
    Activate {
        #[arg(long)]
        release_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.command {
        CommandKind::Run => run_supervisor().await,
        CommandKind::Activate { release_dir } => activate_release(release_dir).await,
    }
}

async fn run_supervisor() -> Result<()> {
    let config = AppConfig::load()?;
    loop {
        let release_dir = current_release_dir(&config)?;
        let worker_path = release_dir.join("bin").join("wxcd-worker");
        let sidecar_path = release_dir
            .join("sidecars")
            .join("webex-ws-sidecar")
            .join("index.cjs");
        if !worker_path.exists() {
            bail!("worker binary not found at {}", worker_path.display());
        }
        if !sidecar_path.exists() {
            bail!("sidecar script not found at {}", sidecar_path.display());
        }
        let mut managed_app_server =
            ensure_cbth_managed_app_server(&config.bridge.cbth_plugin).await?;
        let mut delivery_broker =
            match start_delivery_broker(&config.bridge.cbth_plugin, &config.bridge.state_dir).await
            {
                Ok(broker) => broker,
                Err(error) => {
                    if let Some(app_server) = managed_app_server {
                        app_server.shutdown().await;
                    }
                    return Err(error);
                }
            };

        info!("starting worker from {}", worker_path.display());
        let mut worker_command = Command::new(&worker_path);
        configure_worker_environment(
            &mut worker_command,
            config.bridge.config_path.as_deref(),
            managed_app_server
                .as_ref()
                .map(|app_server| app_server.url.as_str()),
            delivery_broker
                .as_ref()
                .map(|broker| broker.socket_path.as_path()),
        );
        let mut worker = match worker_command
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
        {
            Ok(worker) => worker,
            Err(error) => {
                if let Some(app_server) = managed_app_server {
                    app_server.shutdown().await;
                }
                if let Some(broker) = delivery_broker {
                    broker.shutdown().await;
                }
                return Err(error).context("failed to spawn wxcd-worker");
            }
        };

        info!("starting sidecar from {}", sidecar_path.display());
        let node_path = std::env::var("WXCD_NODE_PATH").unwrap_or_else(|_| "node".to_string());
        let mut sidecar = match Command::new(&node_path)
            .arg(&sidecar_path)
            .env("WXCD_SOCKET_PATH", &config.bridge.socket_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
        {
            Ok(sidecar) => sidecar,
            Err(error) => {
                worker.kill().await.ok();
                worker.wait().await.ok();
                if let Some(app_server) = managed_app_server {
                    app_server.shutdown().await;
                }
                if let Some(broker) = delivery_broker {
                    broker.shutdown().await;
                }
                return Err(error).context("failed to spawn webex sidecar");
            }
        };

        if let Err(error) = wait_for_worker_health(&config.bridge.socket_path).await {
            worker.kill().await.ok();
            sidecar.kill().await.ok();
            if let Some(app_server) = managed_app_server {
                app_server.shutdown().await;
            }
            if let Some(broker) = delivery_broker {
                broker.shutdown().await;
            }
            return Err(error);
        }
        info!("worker health check passed");

        let mut app_server_task_finished = false;
        let mut delivery_broker_task_finished = false;
        match (managed_app_server.as_mut(), delivery_broker.as_mut()) {
            (Some(app_server), Some(broker)) => {
                tokio::select! {
                    status = worker.wait() => {
                        error!("worker exited: {status:?}");
                        sidecar.kill().await.ok();
                    }
                    status = sidecar.wait() => {
                        error!("sidecar exited: {status:?}");
                        worker.kill().await.ok();
                    }
                    result = app_server.wait() => {
                        app_server_task_finished = true;
                        error!("cbth-managed app-server lease task exited: {result:#?}");
                        worker.kill().await.ok();
                        sidecar.kill().await.ok();
                    }
                    result = broker.wait() => {
                        delivery_broker_task_finished = true;
                        error!("cbth delivery broker task exited: {result:#?}");
                        worker.kill().await.ok();
                        sidecar.kill().await.ok();
                    }
                }
            }
            (Some(app_server), None) => {
                tokio::select! {
                    status = worker.wait() => {
                        error!("worker exited: {status:?}");
                        sidecar.kill().await.ok();
                    }
                    status = sidecar.wait() => {
                        error!("sidecar exited: {status:?}");
                        worker.kill().await.ok();
                    }
                    result = app_server.wait() => {
                        app_server_task_finished = true;
                        error!("cbth-managed app-server lease task exited: {result:#?}");
                        worker.kill().await.ok();
                        sidecar.kill().await.ok();
                    }
                }
            }
            (None, Some(broker)) => {
                tokio::select! {
                    status = worker.wait() => {
                        error!("worker exited: {status:?}");
                        sidecar.kill().await.ok();
                    }
                    status = sidecar.wait() => {
                        error!("sidecar exited: {status:?}");
                        worker.kill().await.ok();
                    }
                    result = broker.wait() => {
                        delivery_broker_task_finished = true;
                        error!("cbth delivery broker task exited: {result:#?}");
                        worker.kill().await.ok();
                        sidecar.kill().await.ok();
                    }
                }
            }
            (None, None) => {
                tokio::select! {
                    status = worker.wait() => {
                        error!("worker exited: {status:?}");
                        sidecar.kill().await.ok();
                    }
                    status = sidecar.wait() => {
                        error!("sidecar exited: {status:?}");
                        worker.kill().await.ok();
                    }
                }
            }
        }

        if let Some(app_server) = managed_app_server
            && !app_server_task_finished
        {
            app_server.shutdown().await;
        }
        if let Some(broker) = delivery_broker
            && !delivery_broker_task_finished
        {
            broker.shutdown().await;
        }
        sleep(Duration::from_secs(2)).await;
    }
}

struct PluginRpcSession {
    client: PluginRpcClient,
    hello: PluginHelloResponse,
}

async fn connect_cbth_plugin_rpc(config: &CbthPluginConfig) -> Result<Option<PluginRpcSession>> {
    if !config.enabled {
        return Ok(None);
    }
    let Some(socket_path) = config.socket_path.as_ref() else {
        bail!("cbth plugin mode is enabled but no plugin RPC socket is configured");
    };

    let (client, response) = timeout(PLUGIN_RPC_STARTUP_TIMEOUT, async {
        let mut client = PluginRpcClient::connect(socket_path).await?;
        let response = client
            .plugin_hello(plugin_hello_request(config))
            .await
            .context("failed to complete cbth plugin hello")?;
        Ok::<_, anyhow::Error>((client, response))
    })
    .await
    .with_context(|| {
        format!(
            "timed out after {}s completing cbth plugin hello",
            PLUGIN_RPC_STARTUP_TIMEOUT.as_secs()
        )
    })??;
    info!(
        "cbth plugin RPC hello completed with protocol version {}",
        response.protocol_version
    );
    Ok(Some(PluginRpcSession {
        client,
        hello: response,
    }))
}

struct ManagedCodexAppServer {
    url: String,
    stop_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl ManagedCodexAppServer {
    async fn wait(&mut self) -> Result<()> {
        (&mut self.task)
            .await
            .context("cbth app-server lease task panicked")?
    }

    async fn shutdown(mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        match timeout(PLUGIN_APP_SERVER_TASK_STOP_TIMEOUT, &mut self.task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => warn!("cbth app-server lease shutdown failed: {error:#}"),
            Ok(Err(error)) => warn!("cbth app-server lease task panicked: {error:#}"),
            Err(_) => {
                warn!(
                    "timed out after {}s stopping cbth-managed app-server lease",
                    PLUGIN_APP_SERVER_TASK_STOP_TIMEOUT.as_secs()
                );
                self.task.abort();
                match self.task.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => warn!("aborted cbth app-server lease task failed: {error:#}"),
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => warn!("aborted cbth app-server lease task panicked: {error:#}"),
                }
            }
        }
    }
}

struct ManagedDeliveryBroker {
    socket_path: PathBuf,
    stop_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl ManagedDeliveryBroker {
    async fn wait(&mut self) -> Result<()> {
        (&mut self.task)
            .await
            .context("cbth delivery broker task panicked")?
    }

    async fn shutdown(mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        match timeout(PLUGIN_DELIVERY_BROKER_TASK_STOP_TIMEOUT, &mut self.task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => warn!("cbth delivery broker shutdown failed: {error:#}"),
            Ok(Err(error)) => warn!("cbth delivery broker task panicked: {error:#}"),
            Err(_) => {
                warn!(
                    "timed out after {}s stopping cbth delivery broker",
                    PLUGIN_DELIVERY_BROKER_TASK_STOP_TIMEOUT.as_secs()
                );
                self.task.abort();
                match self.task.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => warn!("aborted cbth delivery broker failed: {error:#}"),
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => warn!("aborted cbth delivery broker task panicked: {error:#}"),
                }
            }
        }
        remove_stale_delivery_broker_socket(&self.socket_path)
            .await
            .ok();
    }
}

async fn ensure_cbth_managed_app_server(
    config: &CbthPluginConfig,
) -> Result<Option<ManagedCodexAppServer>> {
    if !config.enabled {
        return Ok(None);
    }
    let session = connect_cbth_plugin_rpc(config)
        .await?
        .context("cbth plugin mode is enabled but plugin RPC did not connect")?;
    let mut client = session.client;
    let request = plugin_app_server_ensure_request(config)?;
    let managed_session_id = request.managed_session_id.clone();
    let lease_id = request.lease_id.clone();
    let response = timeout(
        PLUGIN_APP_SERVER_ENSURE_TIMEOUT,
        client.app_server_ensure(request),
    )
    .await
    .with_context(|| {
        format!(
            "timed out after {}s ensuring cbth-managed app-server",
            PLUGIN_APP_SERVER_ENSURE_TIMEOUT.as_secs()
        )
    })?
    .context("failed to ensure cbth-managed app-server")?;
    let url = response.app_server.url;
    let lease_seconds_remaining = response.app_server.lease_seconds_remaining;
    info!("cbth-managed app-server ready at {url}");

    let (stop_tx, stop_rx) = oneshot::channel();
    let task = tokio::spawn(run_plugin_app_server_lease(
        client,
        config.clone(),
        managed_session_id,
        lease_id,
        lease_seconds_remaining,
        stop_rx,
    ));
    Ok(Some(ManagedCodexAppServer {
        url,
        stop_tx: Some(stop_tx),
        task,
    }))
}

async fn start_delivery_broker(
    config: &CbthPluginConfig,
    state_dir: &Path,
) -> Result<Option<ManagedDeliveryBroker>> {
    if !config.enabled {
        return Ok(None);
    }
    let session = connect_cbth_plugin_rpc(config)
        .await?
        .context("cbth plugin mode is enabled but plugin RPC did not connect")?;
    if !service_supports_delivery_owned_target(&session.hello) {
        warn!(
            "cbth plugin service is missing `{}` capability; W4 async delivery broker is disabled",
            SERVICE_CAPABILITY_DELIVERY_OWNED_CODEX_APP_SERVER_TARGET_V1
        );
        return Ok(None);
    }
    let socket_path = delivery_broker_socket_path(config, state_dir);
    remove_stale_delivery_broker_socket(&socket_path).await?;
    let listener = UnixListener::bind(&socket_path).with_context(|| {
        format!(
            "failed to bind cbth delivery broker socket {}",
            socket_path.display()
        )
    })?;
    set_delivery_broker_socket_permissions(&socket_path).await?;
    let expected_peer_uid = delivery_broker_socket_owner_uid(&socket_path).await?;
    let (stop_tx, stop_rx) = oneshot::channel();
    let codex_binary = supervisor_codex_binary();
    let task = tokio::spawn(run_delivery_broker(
        listener,
        config.clone(),
        codex_binary,
        expected_peer_uid,
        stop_rx,
    ));
    info!("cbth delivery broker ready at {}", socket_path.display());
    Ok(Some(ManagedDeliveryBroker {
        socket_path,
        stop_tx: Some(stop_tx),
        task,
    }))
}

async fn run_delivery_broker(
    listener: UnixListener,
    config: CbthPluginConfig,
    codex_binary: String,
    expected_peer_uid: u32,
    mut stop_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        warn!("failed to accept cbth delivery broker connection: {error:#}");
                        continue;
                    }
                };
                let config = config.clone();
                let codex_binary = codex_binary.clone();
                connections.spawn(async move {
                    handle_delivery_broker_connection(
                        stream,
                        &config,
                        &codex_binary,
                        expected_peer_uid,
                    ).await
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = joined {
                    log_delivery_broker_connection_result(result);
                }
            }
            _ = &mut stop_rx => {
                connections.abort_all();
                while let Some(result) = connections.join_next().await {
                    log_delivery_broker_connection_result(result);
                }
                return Ok(());
            }
        }
    }
}

fn log_delivery_broker_connection_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!("cbth delivery broker connection failed: {error:#}"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => warn!("cbth delivery broker connection task panicked: {error:#}"),
    }
}

fn configure_worker_environment(
    command: &mut Command,
    config_path: Option<&Path>,
    app_server_url: Option<&str>,
    delivery_broker_socket_path: Option<&Path>,
) {
    command.env_remove(WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV);
    if let Some(config_path) = config_path {
        command.env("WXCD_CONFIG_PATH", config_path);
    }
    if let Some(app_server_url) = app_server_url {
        command.env(WXCD_CODEX_APP_SERVER_URL_ENV, app_server_url);
    }
    if let Some(socket_path) = delivery_broker_socket_path {
        command.env(WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV, socket_path);
    }
}

async fn handle_delivery_broker_connection(
    mut stream: UnixStream,
    config: &CbthPluginConfig,
    codex_binary: &str,
    expected_peer_uid: u32,
) -> Result<()> {
    authenticate_delivery_broker_client(&stream, expected_peer_uid)?;
    let frame: PluginRpcRequestFrame = timeout(
        PLUGIN_DELIVERY_BROKER_FRAME_TIMEOUT,
        read_plugin_rpc_frame(&mut stream, wxcd_cbth_rpc::PLUGIN_RPC_MAX_FRAME_BYTES),
    )
    .await
    .with_context(|| {
        format!(
            "timed out after {}s reading delivery broker frame",
            PLUGIN_DELIVERY_BROKER_FRAME_TIMEOUT.as_secs()
        )
    })?
    .context("failed to read delivery broker frame")?;
    let response = if frame.method != wxcd_cbth_rpc::PLUGIN_RPC_DELIVERY_ENQUEUE_METHOD {
        PluginRpcResponseFrame::failure(
            frame.id,
            PluginRpcError::new(
                PluginRpcErrorKind::MethodNotFound,
                format!(
                    "delivery broker only supports {}",
                    wxcd_cbth_rpc::PLUGIN_RPC_DELIVERY_ENQUEUE_METHOD
                ),
            ),
        )
    } else {
        match serde_json::from_value::<PluginDeliveryEnqueueRequest>(frame.params) {
            Ok(request) => {
                match forward_brokered_delivery_enqueue(config, request, codex_binary).await {
                    Ok(result) => PluginRpcResponseFrame::success(frame.id, result),
                    Err(error) => PluginRpcResponseFrame::failure(frame.id, error),
                }
            }
            Err(error) => PluginRpcResponseFrame::failure(
                frame.id,
                PluginRpcError::new(
                    PluginRpcErrorKind::InvalidRequest,
                    format!("invalid delivery.enqueue request: {error}"),
                ),
            ),
        }
    };
    write_plugin_rpc_frame(
        &mut stream,
        &response,
        wxcd_cbth_rpc::PLUGIN_RPC_MAX_FRAME_BYTES,
    )
    .await
    .context("failed to write delivery broker response")?;
    Ok(())
}

async fn forward_brokered_delivery_enqueue(
    config: &CbthPluginConfig,
    request: PluginDeliveryEnqueueRequest,
    codex_binary: &str,
) -> Result<serde_json::Value, PluginRpcError> {
    let request = prepare_brokered_delivery_request(request, codex_binary)?;
    let mut session = connect_cbth_plugin_rpc(config)
        .await
        .map_err(plugin_rpc_error_from_anyhow)?
        .ok_or_else(|| {
            PluginRpcError::new(
                PluginRpcErrorKind::TransientDaemonUnavailable,
                "cbth plugin RPC is disabled for delivery broker",
            )
        })?;
    if !service_supports_delivery_owned_target(&session.hello) {
        return Err(PluginRpcError::new(
            PluginRpcErrorKind::MissingCapability,
            format!(
                "cbth plugin service is missing `{}` capability required for delivery-owned enqueue",
                SERVICE_CAPABILITY_DELIVERY_OWNED_CODEX_APP_SERVER_TARGET_V1
            ),
        ));
    }
    let result = timeout(
        PLUGIN_DELIVERY_ENQUEUE_TIMEOUT,
        session.client.delivery_enqueue(request),
    )
    .await
    .map_err(|_| {
        PluginRpcError::new(
            PluginRpcErrorKind::TransientDaemonUnavailable,
            format!(
                "timed out after {}s forwarding delivery.enqueue",
                PLUGIN_DELIVERY_ENQUEUE_TIMEOUT.as_secs()
            ),
        )
    })?;
    result.map_err(plugin_rpc_error_from_anyhow)
}

fn prepare_brokered_delivery_request(
    mut request: PluginDeliveryEnqueueRequest,
    codex_binary: &str,
) -> Result<PluginDeliveryEnqueueRequest, PluginRpcError> {
    if request.target.driver != "codex_app_server" {
        return Err(PluginRpcError::new(
            PluginRpcErrorKind::InvalidRequest,
            "delivery broker only supports codex_app_server target driver",
        ));
    }
    if request.target.app_server_lease_id.is_some() {
        return Err(PluginRpcError::new(
            PluginRpcErrorKind::PolicyBlocked,
            "delivery broker only supports delivery-owned target mode",
        ));
    }
    request.target = PluginDeliveryTarget {
        driver: "codex_app_server".to_string(),
        app_server_lease_id: None,
        managed_session_id: None,
        session_epoch: None,
        codex_binary: Some(codex_binary.to_string()),
    };
    Ok(request)
}

fn plugin_rpc_error_from_anyhow(error: anyhow::Error) -> PluginRpcError {
    if let Some(error) = error.downcast_ref::<PluginRpcError>() {
        return error.clone();
    }
    if error.downcast_ref::<std::io::Error>().is_some()
        || error
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    {
        return PluginRpcError::new(
            PluginRpcErrorKind::TransientDaemonUnavailable,
            format!("{error:#}"),
        );
    }
    PluginRpcError::new(PluginRpcErrorKind::Internal, format!("{error:#}"))
}

fn service_supports_delivery_owned_target(response: &PluginHelloResponse) -> bool {
    response.service_capabilities.iter().any(|capability| {
        capability.name == SERVICE_CAPABILITY_DELIVERY_OWNED_CODEX_APP_SERVER_TARGET_V1
    })
}

fn supervisor_codex_binary() -> String {
    std::env::var("WXCD_CODEX_PATH").unwrap_or_else(|_| "codex".to_string())
}

fn delivery_broker_socket_path(config: &CbthPluginConfig, state_dir: &Path) -> PathBuf {
    Path::new(PLUGIN_DELIVERY_BROKER_SOCKET_DIR).join(format!(
        "wxcd-delivery-{}.sock",
        stable_fnv1a_hex(&format!(
            "{}\n{}\n{}",
            config.plugin_instance_id,
            config.plugin_release_id,
            state_dir.display()
        ))
    ))
}

fn stable_fnv1a_hex(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

async fn set_delivery_broker_socket_permissions(socket_path: &Path) -> Result<()> {
    tokio::fs::set_permissions(
        socket_path,
        Permissions::from_mode(PLUGIN_DELIVERY_BROKER_SOCKET_MODE),
    )
    .await
    .with_context(|| {
        format!(
            "failed to set cbth delivery broker socket permissions on {}",
            socket_path.display()
        )
    })
}

async fn delivery_broker_socket_owner_uid(socket_path: &Path) -> Result<u32> {
    let metadata = tokio::fs::symlink_metadata(socket_path)
        .await
        .with_context(|| {
            format!(
                "failed to inspect cbth delivery broker socket {}",
                socket_path.display()
            )
        })?;
    Ok(metadata.uid())
}

fn authenticate_delivery_broker_client(
    stream: &UnixStream,
    expected_peer_uid: u32,
) -> Result<(), PluginRpcError> {
    let credentials = stream.peer_cred().map_err(|error| {
        PluginRpcError::new(
            PluginRpcErrorKind::TransientDaemonUnavailable,
            format!("failed to read delivery broker peer credentials: {error}"),
        )
    })?;
    if credentials.uid() != expected_peer_uid {
        return Err(PluginRpcError::new(
            PluginRpcErrorKind::PolicyBlocked,
            format!(
                "delivery broker peer uid {} does not match expected uid {}",
                credentials.uid(),
                expected_peer_uid
            ),
        ));
    }
    Ok(())
}

async fn remove_stale_delivery_broker_socket(socket_path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(socket_path).await {
        Ok(metadata) if metadata.file_type().is_socket() => {
            tokio::fs::remove_file(socket_path).await.with_context(|| {
                format!(
                    "failed to remove stale cbth delivery broker socket {}",
                    socket_path.display()
                )
            })?;
        }
        Ok(_) => {
            bail!(
                "refusing to replace non-socket cbth delivery broker path {}",
                socket_path.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect cbth delivery broker socket {}",
                    socket_path.display()
                )
            });
        }
    }
    Ok(())
}

async fn run_plugin_app_server_lease(
    mut client: PluginRpcClient,
    config: CbthPluginConfig,
    managed_session_id: String,
    lease_id: String,
    mut lease_seconds_remaining: u64,
    mut stop_rx: oneshot::Receiver<()>,
) -> Result<()> {
    loop {
        let refresh_delay = match plugin_app_server_refresh_delay(lease_seconds_remaining) {
            Ok(delay) => delay,
            Err(error) => {
                cleanup_plugin_app_server_lease_after_error(
                    &config,
                    &managed_session_id,
                    &lease_id,
                )
                .await;
                return Err(error);
            }
        };
        tokio::select! {
            _ = sleep(refresh_delay) => {
                match refresh_plugin_app_server_lease(&mut client, &managed_session_id, &lease_id).await {
                    Ok((url, seconds_remaining)) => {
                        lease_seconds_remaining = seconds_remaining;
                        debug_app_server_refresh(&url);
                    }
                    Err(error) => {
                        cleanup_plugin_app_server_lease_after_error(
                            &config,
                            &managed_session_id,
                            &lease_id,
                        )
                        .await;
                        return Err(error);
                    }
                }
            }
            _ = &mut stop_rx => {
                stop_plugin_app_server_lease(&mut client, &managed_session_id, &lease_id).await?;
                return Ok(());
            }
        }
    }
}

async fn refresh_plugin_app_server_lease(
    client: &mut PluginRpcClient,
    managed_session_id: &str,
    lease_id: &str,
) -> Result<(String, u64)> {
    let response = timeout(
        PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
        client.app_server_refresh(PluginAppServerRefreshRequest {
            managed_session_id: managed_session_id.to_string(),
            lease_id: lease_id.to_string(),
            lease_ttl_seconds: Some(PLUGIN_APP_SERVER_LEASE_TTL_SECONDS),
        }),
    )
    .await
    .with_context(|| {
        format!(
            "timed out after {}s refreshing cbth-managed app-server lease",
            PLUGIN_APP_SERVER_CONTROL_TIMEOUT.as_secs()
        )
    })?
    .context("failed to refresh cbth-managed app-server lease")?;
    Ok((
        response.app_server.url,
        response.app_server.lease_seconds_remaining,
    ))
}

async fn cleanup_plugin_app_server_lease_after_error(
    config: &CbthPluginConfig,
    managed_session_id: &str,
    lease_id: &str,
) {
    match connect_cbth_plugin_rpc(config).await {
        Ok(Some(mut session)) => {
            if let Err(error) =
                stop_plugin_app_server_lease(&mut session.client, managed_session_id, lease_id)
                    .await
            {
                warn!(
                    "failed to stop cbth-managed app-server lease after refresh error: {error:#}"
                );
            }
        }
        Ok(None) => warn!("cbth plugin RPC was disabled before app-server lease cleanup"),
        Err(error) => {
            warn!("failed to reconnect cbth plugin RPC for app-server cleanup: {error:#}")
        }
    }
}

async fn stop_plugin_app_server_lease(
    client: &mut PluginRpcClient,
    managed_session_id: &str,
    lease_id: &str,
) -> Result<()> {
    timeout(
        PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
        client.app_server_stop(PluginAppServerStopRequest {
            managed_session_id: managed_session_id.to_string(),
            lease_id: lease_id.to_string(),
        }),
    )
    .await
    .with_context(|| {
        format!(
            "timed out after {}s stopping cbth-managed app-server lease",
            PLUGIN_APP_SERVER_CONTROL_TIMEOUT.as_secs()
        )
    })?
    .context("failed to stop cbth-managed app-server")?;
    Ok(())
}

fn plugin_app_server_refresh_delay(lease_seconds_remaining: u64) -> Result<Duration> {
    if lease_seconds_remaining <= PLUGIN_APP_SERVER_REFRESH_SAFETY_MARGIN_SECONDS {
        bail!(
            "cbth-managed app-server lease has only {lease_seconds_remaining}s remaining; refusing to refresh after expiry"
        );
    }
    let safe_seconds = lease_seconds_remaining - PLUGIN_APP_SERVER_REFRESH_SAFETY_MARGIN_SECONDS;
    Ok(PLUGIN_APP_SERVER_REFRESH_INTERVAL.min(Duration::from_secs(safe_seconds)))
}

fn debug_app_server_refresh(url: &str) {
    debug!("refreshed cbth-managed app-server lease for {url}");
}

fn plugin_app_server_ensure_request(
    config: &CbthPluginConfig,
) -> Result<PluginAppServerEnsureRequest> {
    let session_id = plugin_app_server_session_id(config);
    Ok(PluginAppServerEnsureRequest {
        managed_session_id: session_id.clone(),
        bound_thread_id: session_id,
        session_epoch: plugin_app_server_session_epoch()?,
        codex_binary: std::env::var("WXCD_CODEX_PATH").unwrap_or_else(|_| "codex".to_string()),
        lease_id: plugin_app_server_lease_id()?,
        lease_ttl_seconds: Some(PLUGIN_APP_SERVER_LEASE_TTL_SECONDS),
    })
}

fn plugin_app_server_session_id(config: &CbthPluginConfig) -> String {
    format!(
        "webex-connector-{}",
        ascii_token_component(&config.plugin_instance_id)
    )
}

fn plugin_app_server_lease_id() -> Result<String> {
    Ok(format!(
        "wxcd-{}-{}",
        std::process::id(),
        current_unix_nanos()?
    ))
}

fn plugin_app_server_session_epoch() -> Result<i64> {
    i64::try_from(current_unix_seconds()?).context("current time does not fit in i64")
}

fn current_unix_nanos() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_nanos())
}

fn current_unix_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_secs())
}

fn ascii_token_component(value: &str) -> String {
    let token = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if token.is_empty() {
        "instance".to_string()
    } else {
        token
    }
}

fn plugin_hello_request(config: &CbthPluginConfig) -> PluginHelloRequest {
    PluginHelloRequest {
        plugin_name: "webex-connector".to_string(),
        plugin_instance_id: config.plugin_instance_id.clone(),
        plugin_release_id: config.plugin_release_id.clone(),
        protocol_versions: vec![PLUGIN_RPC_PROTOCOL_VERSION_V1],
        capabilities: vec![
            PluginCapability::new("diagnostics"),
            PluginCapability::new("standalone-compatible"),
        ],
        plugin_home: config.plugin_home.display().to_string(),
        pid: std::process::id(),
    }
}

async fn activate_release(release_dir: PathBuf) -> Result<()> {
    let config = AppConfig::load()?;
    tokio::fs::create_dir_all(config.bridge.state_dir.join("releases")).await?;
    let current = config.bridge.state_dir.join("current");
    if tokio::fs::try_exists(&current).await.unwrap_or(false) {
        tokio::fs::remove_file(&current).await.ok();
    }
    std::os::unix::fs::symlink(&release_dir, &current).with_context(|| {
        format!(
            "failed to switch current symlink {} -> {}",
            current.display(),
            release_dir.display()
        )
    })?;
    info!("activated release {}", release_dir.display());
    Ok(())
}

fn current_release_dir(config: &AppConfig) -> Result<PathBuf> {
    if let Ok(explicit) = std::env::var("WXCD_RELEASE_DIR") {
        return Ok(PathBuf::from(explicit));
    }
    let current = config.bridge.state_dir.join("current");
    match std::fs::read_link(&current) {
        Ok(path) => Ok(path),
        Err(_) => Ok(current),
    }
}

async fn wait_for_worker_health(socket_path: &Path) -> Result<()> {
    timeout(Duration::from_secs(20), async {
        loop {
            match UnixStream::connect(socket_path).await {
                Ok(stream) => {
                    let (reader, mut writer) = stream.into_split();
                    writer.write_all(br#"{"kind":"health_check"}"#).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                    let mut lines = BufReader::new(reader).lines();
                    if let Some(line) = lines.next_line().await? {
                        let value: serde_json::Value = serde_json::from_str(&line)?;
                        if value.get("healthy").and_then(serde_json::Value::as_bool) == Some(true) {
                            return Ok(());
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        "waiting for worker socket {}: {error:#}",
                        socket_path.display()
                    );
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for worker health"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_server_session_id_is_scoped_to_plugin_instance() {
        let config = CbthPluginConfig {
            enabled: true,
            socket_path: Some("/tmp/cbth.sock".into()),
            plugin_home: "/tmp/plugin".into(),
            plugin_instance_id: "cbth/started at 123".to_string(),
            plugin_release_id: "release-1".to_string(),
            manifest_path: "/tmp/plugin/manifest.json".into(),
        };

        assert_eq!(
            plugin_app_server_session_id(&config),
            "webex-connector-cbth-started-at-123"
        );
    }

    #[test]
    fn app_server_ensure_request_uses_c3_contract_fields() {
        let config = CbthPluginConfig {
            enabled: true,
            socket_path: Some("/tmp/cbth.sock".into()),
            plugin_home: "/tmp/plugin".into(),
            plugin_instance_id: "instance-1".to_string(),
            plugin_release_id: "release-1".to_string(),
            manifest_path: "/tmp/plugin/manifest.json".into(),
        };

        let request = plugin_app_server_ensure_request(&config).expect("request");

        assert_eq!(request.managed_session_id, "webex-connector-instance-1");
        assert_eq!(request.bound_thread_id, "webex-connector-instance-1");
        assert!(request.session_epoch > 0);
        assert!(!request.codex_binary.is_empty());
        assert!(request.lease_id.starts_with("wxcd-"));
        assert_eq!(
            request.lease_ttl_seconds,
            Some(PLUGIN_APP_SERVER_LEASE_TTL_SECONDS)
        );
    }

    #[test]
    fn service_capability_detects_c5_delivery_owned_target() {
        let response = PluginHelloResponse {
            protocol_version: PLUGIN_RPC_PROTOCOL_VERSION_V1,
            service_capabilities: vec![
                wxcd_cbth_rpc::ServiceCapability::new("delivery-driver-codex-app-server-v1"),
                wxcd_cbth_rpc::ServiceCapability::new(
                    SERVICE_CAPABILITY_DELIVERY_OWNED_CODEX_APP_SERVER_TARGET_V1,
                ),
            ],
            policy: wxcd_cbth_rpc::PluginRpcPolicy {
                max_frame_bytes: wxcd_cbth_rpc::PLUGIN_RPC_MAX_FRAME_BYTES,
                requires_idempotency_key: true,
            },
            daemon_endpoint: None,
        };

        assert!(service_supports_delivery_owned_target(&response));
    }

    #[test]
    fn brokered_delivery_request_forces_delivery_owned_codex_target() {
        let request = broker_delivery_request(PluginDeliveryTarget {
            driver: "codex_app_server".to_string(),
            app_server_lease_id: None,
            managed_session_id: Some("worker-supplied-session".to_string()),
            session_epoch: Some(99),
            codex_binary: Some("/tmp/worker-codex".to_string()),
        });

        let prepared =
            prepare_brokered_delivery_request(request, "/usr/local/bin/codex").expect("prepared");

        assert_eq!(prepared.target.driver, "codex_app_server");
        assert_eq!(prepared.target.app_server_lease_id, None);
        assert_eq!(prepared.target.managed_session_id, None);
        assert_eq!(prepared.target.session_epoch, None);
        assert_eq!(
            prepared.target.codex_binary.as_deref(),
            Some("/usr/local/bin/codex")
        );
    }

    #[test]
    fn brokered_delivery_request_rejects_explicit_lease() {
        let request = broker_delivery_request(PluginDeliveryTarget {
            driver: "codex_app_server".to_string(),
            app_server_lease_id: Some("lease-1".to_string()),
            managed_session_id: None,
            session_epoch: None,
            codex_binary: None,
        });

        let error = prepare_brokered_delivery_request(request, "codex")
            .expect_err("explicit lease should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::PolicyBlocked);
    }

    #[test]
    fn delivery_broker_socket_path_stays_short_with_long_state_dir() {
        let config = CbthPluginConfig {
            enabled: true,
            socket_path: Some("/tmp/cbth.sock".into()),
            plugin_home: "/tmp/plugin".into(),
            plugin_instance_id: "instance-with-a-long-managed-runtime-name".repeat(4),
            plugin_release_id: "release-with-a-long-managed-runtime-name".repeat(4),
            manifest_path: "/tmp/plugin/manifest.json".into(),
        };
        let long_state_dir = PathBuf::from(format!(
            "/Users/very-long-managed-user-name/Library/Application Support/{}",
            "codex-webex-connector/".repeat(8)
        ));

        let socket_path = delivery_broker_socket_path(&config, &long_state_dir);
        let rendered = socket_path.to_string_lossy();

        assert!(socket_path.starts_with(PLUGIN_DELIVERY_BROKER_SOCKET_DIR));
        assert!(!socket_path.starts_with(&long_state_dir));
        assert!(
            rendered.len() < 80,
            "broker socket path should stay comfortably below Unix socket path limits: {rendered}"
        );
    }

    #[tokio::test]
    async fn delivery_broker_peer_auth_accepts_same_uid() {
        let (_client, server) = UnixStream::pair().expect("unix stream pair");
        let expected_uid = server.peer_cred().expect("peer credentials").uid();

        authenticate_delivery_broker_client(&server, expected_uid).expect("authenticated");
    }

    #[tokio::test]
    async fn delivery_broker_peer_auth_rejects_wrong_uid() {
        let (_client, server) = UnixStream::pair().expect("unix stream pair");
        let peer_uid = server.peer_cred().expect("peer credentials").uid();

        let error = authenticate_delivery_broker_client(&server, peer_uid.wrapping_add(1))
            .expect_err("wrong uid should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::PolicyBlocked);
    }

    #[tokio::test]
    async fn delivery_broker_socket_permissions_are_owner_only() {
        let socket_path = Path::new(PLUGIN_DELIVERY_BROKER_SOCKET_DIR).join(format!(
            "wxcd-perm-{}-{}.sock",
            std::process::id(),
            stable_fnv1a_hex(
                &SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock")
                    .as_nanos()
                    .to_string()
            )
        ));
        let _ = tokio::fs::remove_file(&socket_path).await;
        let listener = UnixListener::bind(&socket_path).expect("bind test socket");

        set_delivery_broker_socket_permissions(&socket_path)
            .await
            .expect("set socket permissions");

        let metadata = tokio::fs::symlink_metadata(&socket_path)
            .await
            .expect("socket metadata");
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            PLUGIN_DELIVERY_BROKER_SOCKET_MODE
        );

        drop(listener);
        tokio::fs::remove_file(&socket_path)
            .await
            .expect("remove test socket");
    }

    #[test]
    fn broker_anyhow_io_errors_remain_retryable_transient_errors() {
        let error = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "cbth restarting");

        let mapped = plugin_rpc_error_from_anyhow(anyhow::Error::new(error));

        assert_eq!(mapped.kind, PluginRpcErrorKind::TransientDaemonUnavailable);
        assert!(mapped.retryable);
    }

    #[test]
    fn worker_command_env_removes_inherited_delivery_broker_socket_when_absent() {
        let mut command = Command::new("worker");
        command.env(WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV, "/tmp/stale.sock");

        configure_worker_environment(&mut command, None, None, None);

        let broker_env = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV);
        assert!(matches!(broker_env, Some((_, None))));
    }

    #[test]
    fn worker_command_env_sets_current_delivery_broker_socket() {
        let mut command = Command::new("worker");
        let socket_path = Path::new("/tmp/current.sock");

        configure_worker_environment(&mut command, None, None, Some(socket_path));

        let broker_env = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == WXCD_CBTH_DELIVERY_BROKER_SOCKET_ENV);
        assert!(matches!(
            broker_env,
            Some((_, Some(value))) if value == socket_path.as_os_str()
        ));
    }

    fn broker_delivery_request(target: PluginDeliveryTarget) -> PluginDeliveryEnqueueRequest {
        PluginDeliveryEnqueueRequest {
            source_thread_id: "thread-1".to_string(),
            summary: "deliver async result".to_string(),
            idempotency_key: "webex-delivery:event-1".to_string(),
            inline_payload: Some(serde_json::json!({"text": "done"})),
            artifact: None,
            delivery_policy: None,
            max_delivery_attempts: Some(2),
            redelivery_window_seconds: Some(3600),
            target,
            plugin_metadata: Some(serde_json::json!({"webex_event_id": "event-1"})),
        }
    }

    #[test]
    fn app_server_refresh_delay_uses_actual_remaining_ttl() {
        assert_eq!(
            plugin_app_server_refresh_delay(PLUGIN_APP_SERVER_LEASE_TTL_SECONDS).expect("delay"),
            PLUGIN_APP_SERVER_REFRESH_INTERVAL
        );
        assert_eq!(
            plugin_app_server_refresh_delay(20).expect("delay"),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn app_server_refresh_delay_fails_without_safety_margin() {
        assert!(plugin_app_server_refresh_delay(5).is_err());
    }
}
