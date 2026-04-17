use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use directories::BaseDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub webex: WebexConfig,
    pub bridge: BridgeConfig,
    pub repos: Vec<RepoConfig>,
}

#[derive(Debug, Clone)]
pub struct WebexConfig {
    pub bot_token: String,
    pub bot_email: String,
    pub control_room_ref: String,
    pub data_room_ref: String,
    pub allowed_user_emails: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub socket_path: PathBuf,
    pub state_dir: PathBuf,
    pub session_title_prefix: String,
    pub approval_policy: String,
    pub sandbox_mode: String,
    pub snapshot_interval: usize,
    pub developer_instructions: String,
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    socket_path: Option<String>,
    state_dir: Option<String>,
    session_title_prefix: Option<String>,
    approval_policy: Option<String>,
    sandbox_mode: Option<String>,
    snapshot_interval: Option<usize>,
    developer_instructions: Option<String>,
    repos: Option<Vec<RepoConfig>>,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        if let Some(env_path) = env::var_os("WXCD_ENV_PATH") {
            dotenvy::from_path(&env_path)
                .with_context(|| format!("failed to load env file {}", PathBuf::from(&env_path).display()))?;
        } else {
            dotenvy::dotenv().ok();
        }

        let config_path = discover_config_path()?;
        let file_config = match &config_path {
            Some(path) => {
                let content = fs::read_to_string(path)
                    .with_context(|| format!("failed to read config file {}", path.display()))?;
                toml::from_str::<FileConfig>(&content)
                    .with_context(|| format!("failed to parse config file {}", path.display()))?
            }
            None => FileConfig::default(),
        };

        let bot_token = required_env("WEBEX_BOT_TOKEN")?;
        let bot_email = required_env("WEBEX_BOT_EMAIL")?;
        let control_room_ref = required_env("WEBEX_CONTROL_ROOM_SPACE_LINK")?;
        let data_room_ref = required_env("WEBEX_DATA_ROOM_SPACE_LINK")?;
        let allowed_user_emails = required_env("WEBEX_ALLOWED_USER_EMAILS")?
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(|item| item.to_ascii_lowercase())
            .collect::<Vec<_>>();
        if allowed_user_emails.is_empty() {
            bail!("WEBEX_ALLOWED_USER_EMAILS must contain at least one email");
        }

        let state_dir = expand_tilde(
            file_config
                .state_dir
                .unwrap_or_else(|| {
                    "~/Library/Application Support/codex-webex-connector".to_string()
                }),
        )?;
        let socket_path = expand_tilde(
            file_config
                .socket_path
                .unwrap_or_else(|| "/tmp/wxcd.sock".to_string()),
        )?;
        let repos = match file_config.repos {
            Some(repos) if !repos.is_empty() => repos,
            _ => {
                let cwd = env::current_dir().context("failed to determine current directory")?;
                let repo_name = cwd
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("codex-webex-connector")
                    .to_string();
                vec![RepoConfig { name: repo_name, path: cwd }]
            }
        };

        Ok(Self {
            webex: WebexConfig {
                bot_token,
                bot_email: bot_email.to_ascii_lowercase(),
                control_room_ref,
                data_room_ref,
                allowed_user_emails,
            },
            bridge: BridgeConfig {
                socket_path,
                state_dir,
                session_title_prefix: file_config
                    .session_title_prefix
                    .unwrap_or_else(|| "WXCD".to_string()),
                approval_policy: file_config
                    .approval_policy
                    .unwrap_or_else(|| "on-request".to_string()),
                sandbox_mode: file_config
                    .sandbox_mode
                    .unwrap_or_else(|| "workspace-write".to_string()),
                snapshot_interval: file_config.snapshot_interval.unwrap_or(20),
                developer_instructions: file_config.developer_instructions.unwrap_or_else(|| {
                    "You are operating through the wxcd Webex bridge. Keep updates concise and act on concrete requests."
                        .to_string()
                }),
                config_path,
            },
            repos,
        })
    }

    pub fn repo_by_name(&self, name: &str) -> Option<&RepoConfig> {
        self.repos
            .iter()
            .find(|repo| repo.name.eq_ignore_ascii_case(name))
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("missing required environment variable {name}"))
}

fn discover_config_path() -> Result<Option<PathBuf>> {
    if let Some(explicit) = env::var_os("WXCD_CONFIG_PATH") {
        return Ok(Some(PathBuf::from(explicit)));
    }

    let cwd_candidate = PathBuf::from("wxcd.toml");
    if cwd_candidate.exists() {
        return Ok(Some(cwd_candidate));
    }

    let base_dirs = BaseDirs::new().ok_or_else(|| anyhow!("failed to determine home directory"))?;
    let app_support = base_dirs
        .home_dir()
        .join("Library")
        .join("Application Support")
        .join("codex-webex-connector")
        .join("config")
        .join("wxcd.toml");
    if app_support.exists() {
        return Ok(Some(app_support));
    }

    Ok(None)
}

fn expand_tilde(value: String) -> Result<PathBuf> {
    let path = Path::new(&value);
    if !value.starts_with("~/") {
        return Ok(path.to_path_buf());
    }

    let base_dirs = BaseDirs::new().ok_or_else(|| anyhow!("failed to determine home directory"))?;
    Ok(base_dirs.home_dir().join(value.trim_start_matches("~/")))
}
