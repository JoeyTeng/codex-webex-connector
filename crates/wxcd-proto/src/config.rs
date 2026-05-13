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
pub struct DiagnosticsConfig {
    pub bridge: BridgeConfig,
    pub repos: Vec<RepoConfig>,
    pub missing_webex_env: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub struct WebexConfig {
    pub bot_token: String,
    pub bot_email: String,
    pub bot_display_name: Option<String>,
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
    pub cbth_plugin: CbthPluginConfig,
}

#[derive(Debug, Clone)]
pub struct CbthPluginConfig {
    pub enabled: bool,
    pub socket_path: Option<PathBuf>,
    pub plugin_home: PathBuf,
    pub plugin_instance_id: String,
    pub plugin_release_id: String,
    pub manifest_path: PathBuf,
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
    cbth_plugin: Option<FileCbthPluginConfig>,
    repos: Option<Vec<RepoConfig>>,
}

#[derive(Debug, Default, Deserialize)]
struct FileCbthPluginConfig {
    enabled: Option<bool>,
    socket_path: Option<String>,
    plugin_home: Option<String>,
    plugin_instance_id: Option<String>,
    plugin_release_id: Option<String>,
    manifest_path: Option<String>,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let sources = load_config_sources()?;
        let webex = load_webex_config()?;
        let bridge = build_bridge_config(&sources.file_config, sources.config_path.clone())?;
        let repos = build_repos(sources.file_config.repos)?;

        Ok(Self {
            webex,
            bridge,
            repos,
        })
    }

    pub fn load_diagnostics() -> Result<DiagnosticsConfig> {
        let sources = load_config_sources()?;
        let bridge = build_bridge_config(&sources.file_config, sources.config_path.clone())?;
        let repos = build_repos(sources.file_config.repos)?;
        let missing_webex_env = WEBEX_REQUIRED_ENV
            .iter()
            .copied()
            .filter(|name| env::var(name).is_err())
            .collect();

        Ok(DiagnosticsConfig {
            bridge,
            repos,
            missing_webex_env,
        })
    }

    pub fn repo_by_name(&self, name: &str) -> Option<&RepoConfig> {
        self.repos
            .iter()
            .find(|repo| repo.name.eq_ignore_ascii_case(name))
    }
}

struct ConfigSources {
    config_path: Option<PathBuf>,
    file_config: FileConfig,
}

const WEBEX_REQUIRED_ENV: &[&str] = &[
    "WEBEX_BOT_TOKEN",
    "WEBEX_BOT_EMAIL",
    "WEBEX_CONTROL_ROOM_SPACE_LINK",
    "WEBEX_DATA_ROOM_SPACE_LINK",
    "WEBEX_ALLOWED_USER_EMAILS",
];

fn load_config_sources() -> Result<ConfigSources> {
    if let Some(env_path) = env::var_os("WXCD_ENV_PATH") {
        dotenvy::from_path(&env_path).with_context(|| {
            format!(
                "failed to load env file {}",
                PathBuf::from(&env_path).display()
            )
        })?;
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

    Ok(ConfigSources {
        config_path,
        file_config,
    })
}

fn load_webex_config() -> Result<WebexConfig> {
    let bot_token = required_env("WEBEX_BOT_TOKEN")?;
    let bot_email = required_env("WEBEX_BOT_EMAIL")?;
    let bot_display_name = optional_env("WEBEX_BOT_DISPLAY_NAME");
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

    Ok(WebexConfig {
        bot_token,
        bot_email: bot_email.to_ascii_lowercase(),
        bot_display_name,
        control_room_ref,
        data_room_ref,
        allowed_user_emails,
    })
}

fn build_bridge_config(
    file_config: &FileConfig,
    config_path: Option<PathBuf>,
) -> Result<BridgeConfig> {
    let state_dir = expand_tilde(
        file_config
            .state_dir
            .clone()
            .unwrap_or_else(|| "~/Library/Application Support/codex-webex-connector".to_string()),
    )?;
    let socket_path = expand_tilde(
        file_config
            .socket_path
            .clone()
            .unwrap_or_else(|| "/tmp/wxcd.sock".to_string()),
    )?;
    let cbth_plugin = build_cbth_plugin_config(file_config.cbth_plugin.as_ref(), &state_dir)?;

    Ok(BridgeConfig {
        socket_path,
        state_dir,
        session_title_prefix: file_config
            .session_title_prefix
            .clone()
            .unwrap_or_else(|| "WXCD".to_string()),
        approval_policy: file_config
            .approval_policy
            .clone()
            .unwrap_or_else(|| "on-request".to_string()),
        sandbox_mode: file_config
            .sandbox_mode
            .clone()
            .unwrap_or_else(|| "workspace-write".to_string()),
        snapshot_interval: file_config.snapshot_interval.unwrap_or(20),
        developer_instructions: file_config.developer_instructions.clone().unwrap_or_else(|| {
            "You are operating through the wxcd Webex bridge. Keep updates concise and act on concrete requests."
                .to_string()
        }),
        config_path,
        cbth_plugin,
    })
}

fn build_cbth_plugin_config(
    file_config: Option<&FileCbthPluginConfig>,
    state_dir: &Path,
) -> Result<CbthPluginConfig> {
    let enabled = env_bool("WXCD_CBTH_PLUGIN")
        .or_else(|| file_config.and_then(|config| config.enabled))
        .unwrap_or(false);
    let socket_path = optional_path_env("WXCD_CBTH_SOCKET_PATH")
        .or_else(|| file_config.and_then(|config| config.socket_path.clone()))
        .map(expand_tilde)
        .transpose()?;
    let plugin_home = optional_path_env("WXCD_PLUGIN_HOME")
        .or_else(|| file_config.and_then(|config| config.plugin_home.clone()))
        .map(expand_tilde)
        .transpose()?
        .unwrap_or_else(|| state_dir.join("plugin"));
    let plugin_instance_id = optional_env("WXCD_PLUGIN_INSTANCE_ID")
        .or_else(|| file_config.and_then(|config| config.plugin_instance_id.clone()))
        .unwrap_or_else(|| "standalone".to_string());
    let plugin_release_id = optional_env("WXCD_PLUGIN_RELEASE_ID")
        .or_else(|| file_config.and_then(|config| config.plugin_release_id.clone()))
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
    let manifest_path = optional_path_env("WXCD_PLUGIN_MANIFEST_PATH")
        .or_else(|| file_config.and_then(|config| config.manifest_path.clone()))
        .map(expand_tilde)
        .transpose()?
        .unwrap_or_else(|| PathBuf::from("plugin/manifest.json"));

    Ok(CbthPluginConfig {
        enabled,
        socket_path,
        plugin_home,
        plugin_instance_id,
        plugin_release_id,
        manifest_path,
    })
}

fn build_repos(repos: Option<Vec<RepoConfig>>) -> Result<Vec<RepoConfig>> {
    match repos {
        Some(repos) if !repos.is_empty() => Ok(repos),
        _ => {
            let cwd = env::current_dir().context("failed to determine current directory")?;
            let repo_name = cwd
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("codex-webex-connector")
                .to_string();
            Ok(vec![RepoConfig {
                name: repo_name,
                path: cwd,
            }])
        }
    }
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|value| {
        let value = value.trim().to_ascii_lowercase();
        match value.as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    })
}

fn optional_path_env(name: &str) -> Option<String> {
    optional_env(name)
}

impl CbthPluginConfig {
    pub fn mode_name(&self) -> &'static str {
        if self.enabled {
            "cbth_plugin"
        } else {
            "standalone"
        }
    }

    pub fn is_ready_for_rpc(&self) -> bool {
        self.enabled && self.socket_path.is_some()
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("missing required environment variable {name}"))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
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

#[cfg(test)]
mod tests {
    use super::{FileConfig, build_bridge_config};

    #[test]
    fn bridge_config_defaults_to_standalone_legacy_mode() {
        let config = build_bridge_config(&FileConfig::default(), None).unwrap();

        assert_eq!(config.socket_path.to_string_lossy(), "/tmp/wxcd.sock");
        assert!(!config.cbth_plugin.enabled);
        assert_eq!(config.cbth_plugin.mode_name(), "standalone");
        assert!(!config.cbth_plugin.is_ready_for_rpc());
        assert_eq!(config.cbth_plugin.socket_path, None);
    }

    #[test]
    fn bridge_config_reads_explicit_cbth_plugin_mode() {
        let file_config: FileConfig = toml::from_str(
            r#"
socket_path = "/tmp/wxcd.sock"

[cbth_plugin]
enabled = true
socket_path = "/tmp/cbth-webex.sock"
plugin_home = "/tmp/wxcd-plugin-home"
plugin_instance_id = "instance-1"
plugin_release_id = "release-1"
manifest_path = "plugin/manifest.json"
"#,
        )
        .unwrap();

        let config = build_bridge_config(&file_config, None).unwrap();

        assert!(config.cbth_plugin.enabled);
        assert_eq!(config.cbth_plugin.mode_name(), "cbth_plugin");
        assert!(config.cbth_plugin.is_ready_for_rpc());
        assert_eq!(
            config
                .cbth_plugin
                .socket_path
                .as_ref()
                .unwrap()
                .to_string_lossy(),
            "/tmp/cbth-webex.sock"
        );
        assert_eq!(config.cbth_plugin.plugin_instance_id, "instance-1");
        assert_eq!(config.cbth_plugin.plugin_release_id, "release-1");
    }
}
