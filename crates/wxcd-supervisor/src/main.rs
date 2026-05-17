use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use wxcd_cbth_rpc::{
    PLUGIN_RPC_PROTOCOL_VERSION_V1, PluginAppServerEnsureRequest, PluginAppServerRefreshRequest,
    PluginAppServerStopRequest, PluginCapability, PluginHelloRequest, PluginRpcClient,
};
use wxcd_proto::{AppConfig, CbthPluginConfig};

const PLUGIN_RPC_STARTUP_TIMEOUT: Duration = Duration::from_secs(3);
const PLUGIN_APP_SERVER_LEASE_TTL_SECONDS: u64 = 120;
const PLUGIN_APP_SERVER_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const PLUGIN_APP_SERVER_REFRESH_SAFETY_MARGIN_SECONDS: u64 = 5;
const PLUGIN_APP_SERVER_ENSURE_TIMEOUT: Duration = Duration::from_secs(20);
const PLUGIN_APP_SERVER_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const PLUGIN_APP_SERVER_TASK_STOP_TIMEOUT: Duration = Duration::from_secs(6);
const WXCD_CODEX_APP_SERVER_URL_ENV: &str = "WXCD_CODEX_APP_SERVER_URL";

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

        info!("starting worker from {}", worker_path.display());
        let mut worker_command = Command::new(&worker_path);
        if let Some(config_path) = &config.bridge.config_path {
            worker_command.env("WXCD_CONFIG_PATH", config_path);
        }
        if let Some(app_server) = managed_app_server.as_ref() {
            worker_command.env(WXCD_CODEX_APP_SERVER_URL_ENV, &app_server.url);
        }
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
                return Err(error).context("failed to spawn webex sidecar");
            }
        };

        if let Err(error) = wait_for_worker_health(&config.bridge.socket_path).await {
            worker.kill().await.ok();
            sidecar.kill().await.ok();
            if let Some(app_server) = managed_app_server {
                app_server.shutdown().await;
            }
            return Err(error);
        }
        info!("worker health check passed");

        let mut app_server_task_finished = false;
        if let Some(app_server) = managed_app_server.as_mut() {
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
        } else {
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

        if let Some(app_server) = managed_app_server
            && !app_server_task_finished
        {
            app_server.shutdown().await;
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn connect_cbth_plugin_rpc(config: &CbthPluginConfig) -> Result<Option<PluginRpcClient>> {
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
    Ok(Some(client))
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

async fn ensure_cbth_managed_app_server(
    config: &CbthPluginConfig,
) -> Result<Option<ManagedCodexAppServer>> {
    if !config.enabled {
        return Ok(None);
    }
    let mut client = connect_cbth_plugin_rpc(config)
        .await?
        .context("cbth plugin mode is enabled but plugin RPC did not connect")?;
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
        Ok(Some(mut client)) => {
            if let Err(error) =
                stop_plugin_app_server_lease(&mut client, managed_session_id, lease_id).await
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
