use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::time::{Duration, sleep, timeout};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wxcd_proto::AppConfig;

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

        info!("starting worker from {}", worker_path.display());
        let mut worker_command = Command::new(&worker_path);
        if let Some(config_path) = &config.bridge.config_path {
            worker_command.env("WXCD_CONFIG_PATH", config_path);
        }
        let mut worker = worker_command
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("failed to spawn wxcd-worker")?;

        info!("starting sidecar from {}", sidecar_path.display());
        let node_path = std::env::var("WXCD_NODE_PATH").unwrap_or_else(|_| "node".to_string());
        let mut sidecar = Command::new(&node_path)
            .arg(&sidecar_path)
            .env("WXCD_SOCKET_PATH", &config.bridge.socket_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("failed to spawn webex sidecar")?;

        wait_for_worker_health(&config.bridge.socket_path).await?;
        info!("worker health check passed");

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

        sleep(Duration::from_secs(2)).await;
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
