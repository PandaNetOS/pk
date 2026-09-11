use crate::config::PkConfig;
use crate::models::*;
use crate::service_registry::ServiceRegistry;
use crate::torrent_index::TorrentIndexDb;
use crate::ws::{FrontendWsManager, WsManager};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct AppState {
    pub cfg: PkConfig,
    pub work_root: PathBuf,
    pub data_dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub ws_mgr: WsManager,
    pub service_registry: ServiceRegistry,
    pub frontend_ws_mgr: FrontendWsManager,
    pub conn: Mutex<Connection>,
    pub torrent_index: Arc<TorrentIndexDb>,
    /// 通信 SDK 句柄（pnos-comm 注册到 pnos-runtime 后填充；runtime 不可达时为 None）
    pub pk_app: tokio::sync::RwLock<Option<pnos_comm::PnosApp>>,
}

impl AppState {
    pub async fn open(cfg: PkConfig, work_root: PathBuf) -> Result<Arc<Self>> {
        let data_dir = cfg.resolve_data_dir(&work_root);
        tokio::fs::create_dir_all(&data_dir)
            .await
            .context("create data_dir")?;
        let artifacts_dir = data_dir.join("artifacts");
        tokio::fs::create_dir_all(&artifacts_dir)
            .await
            .context("create artifacts")?;
        write_artifact_readme(&artifacts_dir).await?;

        let db_path = data_dir.join("state.db");
        let conn = Connection::open(&db_path).context("open sqlite")?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;",
        )?;
        create_tables(&conn)?;

        // 数据库迁移：旧库添加节点能力参数字段
        let _ = conn.execute("ALTER TABLE nodes ADD COLUMN max_concurrent INTEGER", []);
        let _ = conn.execute("ALTER TABLE nodes ADD COLUMN max_bandwidth_bps INTEGER", []);
        let _ = conn.execute("ALTER TABLE nodes ADD COLUMN capabilities TEXT", []);

        // 重启时回收所有非终态 dispatch，避免节点崩溃后任务永远卡在 running/acked
        let reclaimed = conn
            .execute(
                "UPDATE dispatches SET state='pending', node_id=NULL, claimed_at=NULL WHERE state IN ('acked','running')",
                [],
            )
            .unwrap_or(0);
        if reclaimed > 0 {
            tracing::info!("启动时回收 {} 个卡住的 dispatch 到待下发池", reclaimed);
        }

        // 启动时清除所有内部节点（容器内spde，重建后旧节点必然失效）
        // 内部节点通过 labels 包含 "internal=true" 标记
        // 清除时先把 node_id 加入 deleted_nodes，防止 spde 用旧 node_id 重新注册时自动通过
        let internal_ids: Vec<String> = conn
            .prepare("SELECT id FROM nodes WHERE labels LIKE '%internal=true%'")
            .and_then(|mut stmt| {
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
            })
            .unwrap_or_default();
        for nid in &internal_ids {
            let _ = conn.execute(
                "INSERT OR IGNORE INTO deleted_nodes (node_id, deleted_at) VALUES (?1, ?2)",
                params![nid, Utc::now().to_rfc3339()],
            );
        }
        let internal_cleared = conn
            .execute("DELETE FROM nodes WHERE labels LIKE '%internal=true%'", [])
            .unwrap_or(0);
        if internal_cleared > 0 {
            tracing::info!(
                "启动时清除 {} 个旧内部节点并标记为已删除（容器重建后自动失效，需重新审批）",
                internal_cleared
            );
        }

        // 从旧 state.json 导入数据
        let json_path = data_dir.join("state.json");
        if json_path.exists() {
            match import_from_json(&conn, &json_path) {
                Ok(n) => tracing::info!("已从 state.json 导入 {} 条数据到 SQLite", n),
                Err(e) => tracing::error!("从 state.json 导入失败: {}", e),
            }
            let bak = data_dir.join("state.json.bak");
            std::fs::rename(&json_path, &bak).ok();
        }

        let torrent_index = Arc::new(
            TorrentIndexDb::open(data_dir.join("torrent_index.db"))
                .context("open torrent_index db")?,
        );
        // 首次启动：tasks 表为空时预置默认任务
        seed_default_tasks(&conn)?;

        Ok(Arc::new(Self {
            cfg,
            work_root,
            data_dir,
            artifacts_dir,
            ws_mgr: WsManager::new(),
            service_registry: ServiceRegistry::new(),
            frontend_ws_mgr: FrontendWsManager::new(),
            conn: Mutex::new(conn),
            torrent_index,
            pk_app: tokio::sync::RwLock::new(None),
        }))
    }

    /// 只读操作：获取连接并执行
    pub async fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let conn = self.conn.lock().await;
        f(&conn)
    }

    /// 写操作：在事务中执行，自动 commit/rollback
    pub async fn with_transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let conn = self.conn.lock().await;
        conn.execute("BEGIN IMMEDIATE", [])?;
        match f(&conn) {
            Ok(v) => {
                conn.execute("COMMIT", [])?;
                Ok(v)
            }
            Err(e) => {
                conn.execute("ROLLBACK", [])?;
                Err(e)
            }
        }
    }

    pub fn heartbeat_timeout(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.cfg.heartbeat_timeout_secs as i64)
    }

    pub async fn refresh_online(&self) {
        let timeout = self.heartbeat_timeout();
        let now = Utc::now();
        let cutoff = (now - timeout).to_rfc3339();
        let conn = self.conn.lock().await;
        // 双向同步：最近有心跳的 offline 节点设为 online，超时的设为 offline
        // 注意：只改 offline 状态，不影响 pending（待审批）和 busy（忙碌）状态
        conn.execute(
            "UPDATE nodes SET status = 'online' WHERE last_seen >= ?1 AND status = 'offline'",
            params![cutoff],
        )
        .ok();
        // 超时的 online/busy → offline（pending 不自动改，保持待审批等用户操作）
        conn.execute(
            "UPDATE nodes SET status = 'offline', active_tasks = 0 WHERE last_seen < ?1 AND status IN ('online', 'busy')",
            params![cutoff],
        )
        .ok();
        drop(conn);
        // 同步更新服务注册中心：超时节点标记为 unhealthy
        self.service_registry
            .mark_timeout_unhealthy(self.cfg.heartbeat_timeout_secs as i64)
            .await;
    }
}

// ── 表创建 ───────────────────────────────────────────────

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            hostname TEXT,
            platform TEXT,
            arch TEXT,
            version TEXT,
            status TEXT,
            last_seen TEXT,
            registered_at TEXT,
            labels TEXT,
            active_tasks INTEGER,
            bytes_downloaded INTEGER,
            last_error TEXT,
            max_concurrent INTEGER,
            max_bandwidth_bps INTEGER,
            capabilities TEXT
        );

        CREATE TABLE IF NOT EXISTS deleted_nodes (
            node_id TEXT PRIMARY KEY,
            deleted_at TEXT,
            reason TEXT
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            name TEXT,
            url TEXT,
            filename TEXT,
            enable INTEGER,
            created_at TEXT,
            note TEXT,
            overrides TEXT
        );

        CREATE TABLE IF NOT EXISTS dispatches (
            id TEXT PRIMARY KEY,
            task_id TEXT,
            node_id TEXT,
            state TEXT,
            created_at TEXT,
            updated_at TEXT,
            claimed_at TEXT,
            target TEXT,
            allowed_nodes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_dispatches_state ON dispatches(state);
        CREATE INDEX IF NOT EXISTS idx_dispatches_task ON dispatches(task_id);

        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            task_id TEXT,
            dispatch_id TEXT,
            node_id TEXT,
            task_name TEXT,
            url TEXT,
            filename TEXT,
            file_size INTEGER,
            downloaded_bytes INTEGER,
            elapsed_secs REAL,
            avg_speed_mbps REAL,
            status TEXT,
            success_chunks INTEGER,
            failed_chunks INTEGER,
            error_msg TEXT,
            timestamp TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_runs_timestamp ON runs(timestamp);
        CREATE INDEX IF NOT EXISTS idx_runs_node ON runs(node_id);

        CREATE TABLE IF NOT EXISTS workflows (
            id TEXT PRIMARY KEY,
            name TEXT,
            enable INTEGER,
            schedule TEXT,
            task_ids TEXT,
            target TEXT,
            node_ids TEXT,
            next_run_at TEXT,
            last_run_at TEXT,
            last_run_status TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS workflow_runs (
            id TEXT PRIMARY KEY,
            workflow_id TEXT,
            workflow_name TEXT,
            triggered_at TEXT,
            status TEXT,
            task_count INTEGER,
            success_count INTEGER,
            failed_count INTEGER,
            dispatch_ids TEXT,
            error_msg TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_wfr_workflow ON workflow_runs(workflow_id);
        "#,
    )?;
    Ok(())
}

// ── 首次启动预置默认任务 ──────────────────────────────────

fn seed_default_tasks(conn: &Connection) -> Result<()> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get(0))?;
    if count > 0 {
        return Ok(());
    }

    let now = Utc::now().to_rfc3339();
    let defaults = [
        (
            "iPhone 4.7 12.1.4 固件",
            "http://updates-http.cdn-apple.com/2019WinterFCS/fullrestores/041-39257/32129B6C-292C-11E9-9E72-4511412B0A59/iPhone_4.7_12.1.4_16D57_Restore.ipsw",
            "iPhone_4.7_12.1.4_16D57_Restore.ipsw",
        ),
        (
            "iPhone 4S iOS 9.3.5 固件",
            "http://appldnld.apple.com/iOS9.3.5/031-73068-20160825-6A2B99DE-6711-11E6-BB8F-133834D2D062/iPhone4,1_9.3.5_13G36_Restore.ipsw",
            "iPhone4,1_9.3.5_13G36_Restore.ipsw",
        ),
        (
            "iPhone iOS 15.3 固件",
            "https://updates.cdn-apple.com/2022FCSWinter/fullrestores/002-57695/F64E2C74-C18B-48B3-8BC2-FF0DB4E21301/iPhone11,2,iPhone11,4,iPhone11,6,iPhone12,3,iPhone12,5_15.3_19D50_Restore.ipsw",
            "iPhone11,2,iPhone11,4,iPhone11,6,iPhone12,3,iPhone12,5_15.3_19D50_Restore.ipsw",
        ),
    ];

    for (name, url, filename) in defaults {
        conn.execute(
            "INSERT INTO tasks (id, name, url, filename, enable, created_at, note, overrides) VALUES (?1, ?2, ?3, ?4, 1, ?5, '', '{}')",
            params![uuid::Uuid::new_v4().to_string(), name, url, filename, now],
        )?;
    }
    tracing::info!("首次启动：已预置 {} 个默认下载任务", defaults.len());
    Ok(())
}

// ── 从旧 state.json 导入 ─────────────────────────────────

fn import_from_json(conn: &Connection, path: &Path) -> Result<usize> {
    let text = std::fs::read_to_string(path)?;
    let snap: Snapshot = serde_json::from_str(&text).context("parse state.json")?;
    let mut count = 0usize;

    for n in &snap.nodes {
        conn.execute(
            "INSERT OR REPLACE INTO nodes VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                n.id.to_string(),
                n.hostname,
                n.platform,
                n.arch,
                n.version,
                serde_json::to_string(&n.status)?,
                n.last_seen.to_rfc3339(),
                n.registered_at.to_rfc3339(),
                serde_json::to_string(&n.labels)?,
                n.active_tasks,
                n.bytes_downloaded,
                n.last_error,
            ],
        )?;
        count += 1;
    }

    for t in &snap.tasks {
        conn.execute(
            "INSERT OR REPLACE INTO tasks VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                t.id.to_string(),
                t.name,
                t.url,
                t.filename,
                t.enable,
                t.created_at.to_rfc3339(),
                t.note,
                serde_json::to_string(&t.overrides)?,
            ],
        )?;
        count += 1;
    }

    for d in &snap.dispatches {
        conn.execute(
            "INSERT OR REPLACE INTO dispatches VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                d.id.to_string(),
                d.task_id.to_string(),
                d.node_id.map(|u| u.to_string()),
                serde_json::to_string(&d.state)?,
                d.created_at.to_rfc3339(),
                d.updated_at.to_rfc3339(),
                d.claimed_at.map(|t| t.to_rfc3339()),
                serde_json::to_string(&d.target)?,
                serde_json::to_string(&d.allowed_nodes)?,
            ],
        )?;
        count += 1;
    }

    for r in &snap.runs {
        conn.execute(
            "INSERT OR REPLACE INTO runs VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                r.id.to_string(),
                r.task_id.map(|u| u.to_string()),
                r.dispatch_id.map(|u| u.to_string()),
                r.node_id.to_string(),
                r.task_name,
                r.url,
                r.filename,
                r.file_size,
                r.downloaded_bytes,
                r.elapsed_secs,
                r.avg_speed_mbps,
                r.status,
                r.success_chunks,
                r.failed_chunks,
                r.error_msg,
                r.timestamp.to_rfc3339(),
            ],
        )?;
        count += 1;
    }

    for wf in &snap.workflows {
        conn.execute(
            "INSERT OR REPLACE INTO workflows VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                wf.id.to_string(),
                wf.name,
                wf.enable,
                serde_json::to_string(&wf.schedule)?,
                serde_json::to_string(&wf.task_ids)?,
                serde_json::to_string(&wf.target)?,
                serde_json::to_string(&wf.node_ids)?,
                wf.next_run_at.map(|t| t.to_rfc3339()),
                wf.last_run_at.map(|t| t.to_rfc3339()),
                wf.last_run_status,
                wf.created_at.to_rfc3339(),
            ],
        )?;
        count += 1;
    }

    for wr in &snap.workflow_runs {
        conn.execute(
            "INSERT OR REPLACE INTO workflow_runs VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                wr.id.to_string(),
                wr.workflow_id.to_string(),
                wr.workflow_name,
                wr.triggered_at.to_rfc3339(),
                wr.status,
                wr.task_count,
                wr.success_count,
                wr.failed_count,
                serde_json::to_string(&wr.dispatch_ids)?,
                wr.error_msg,
            ],
        )?;
        count += 1;
    }

    Ok(count)
}

// ── 辅助函数 ─────────────────────────────────────────────

async fn write_artifact_readme(dir: &Path) -> Result<()> {
    let p = dir.join("README.txt");
    if p.exists() {
        return Ok(());
    }
    let body = r#"将各平台 SPDE 二进制放到本目录，文件名约定：

  spde-windows-x86_64.exe
  spde-linux-x86_64
  spde-linux-aarch64
  spde-macos-x86_64
  spde-macos-aarch64

节点可通过 GET /api/v1/artifacts/<platform> 拉取
"#;
    tokio::fs::write(p, body).await?;
    Ok(())
}

pub fn artifact_filename(platform: &str) -> Option<&'static str> {
    match platform {
        "windows-x86_64" => Some("spde-windows-x86_64.exe"),
        "linux-x86_64" => Some("spde-linux-x86_64"),
        "linux-aarch64" => Some("spde-linux-aarch64"),
        "macos-x86_64" => Some("spde-macos-x86_64"),
        "macos-aarch64" => Some("spde-macos-aarch64"),
        _ => None,
    }
}

pub fn detect_host_platform() -> (String, String) {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let platform = match (os, arch) {
        ("windows", "x86_64") => "windows-x86_64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        _ => "unknown",
    };
    (platform.into(), arch.into())
}
