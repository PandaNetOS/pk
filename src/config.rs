use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PkConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default)]
    pub data_dir: Option<String>,
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
    #[serde(default)]
    pub token: String,
    /// pnos-runtime 地址（通信 SDK 注册/发现/事件用）。为空时用 SDK 默认 http://127.0.0.1:8080
    #[serde(default)]
    pub runtime_url: Option<String>,
    /// 注册 pnos-runtime 的最长等待秒数；超时则 pk 以独立模式运行（优雅降级）
    #[serde(default = "default_register_timeout")]
    pub register_timeout_secs: u64,
    /// 区域标识（可选，写入组件注册信息）
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub spde_defaults: SpdeDefaults,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpdeDefaults {
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: u32,
    #[serde(default = "default_true")]
    pub resume: bool,
    #[serde(default = "default_retry")]
    pub retry_times: u32,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default)]
    pub skip_tls_verify: bool,
    #[serde(default = "default_connections")]
    pub connections_per_file: u32,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_save_path")]
    pub save_path: String,
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default)]
    pub https_proxy: String,
}

impl Default for SpdeDefaults {
    fn default() -> Self {
        Self {
            max_concurrent: default_max_concurrent(),
            resume: true,
            retry_times: default_retry(),
            timeout: default_timeout(),
            skip_tls_verify: false,
            connections_per_file: default_connections(),
            dry_run: true,
            save_path: default_save_path(),
            http_proxy: String::new(),
            https_proxy: String::new(),
        }
    }
}

impl Default for PkConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: None,
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            token: String::new(),
            runtime_url: None,
            register_timeout_secs: default_register_timeout(),
            region: None,
            spde_defaults: SpdeDefaults::default(),
        }
    }
}

fn default_listen() -> String {
    "0.0.0.0:5566".into()
}
fn default_heartbeat_timeout() -> u64 {
    45
}
fn default_register_timeout() -> u64 {
    10
}
fn default_true() -> bool {
    true
}
fn default_retry() -> u32 {
    3
}
fn default_timeout() -> u64 {
    1800
}
fn default_connections() -> u32 {
    8
}
fn default_max_concurrent() -> u32 {
    4
}
fn default_save_path() -> String {
    "./download".into()
}

pub fn default_yaml() -> &'static str {
    r#"# PK master config
listen: "0.0.0.0:5566"
data_dir: null
heartbeat_timeout_secs: 45
token: ""
runtime_url: null
register_timeout_secs: 10
region: null
spde_defaults:
  max_concurrent: 4
  resume: true
  retry_times: 3
  timeout: 1800
  skip_tls_verify: false
  connections_per_file: 8
  dry_run: true
  save_path: "./download"
  http_proxy: ""
  https_proxy: ""
"#
}

impl PkConfig {
    pub fn load_or_init(path: &Path) -> Result<Self> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).ok();
            }
            fs::write(path, default_yaml()).context("write default pk config")?;
            tracing::info!("created default config {:?}", path);
        }
        let text = fs::read_to_string(path).context("read pk config")?;
        let cfg: PkConfig = serde_yaml::from_str(&text).context("parse pk config")?;
        Ok(cfg)
    }

    pub fn resolve_data_dir(&self, work_root: &Path) -> PathBuf {
        match &self.data_dir {
            Some(p) if !p.is_empty() => {
                let pb = PathBuf::from(p);
                if pb.is_absolute() {
                    pb
                } else {
                    work_root.join(pb)
                }
            }
            _ => work_root.join("pk-data"),
        }
    }
}
