use crate::config::SpdeDefaults;
use crate::models::*;
use crate::scheduler;
use crate::spde_cfg;
use crate::store::{artifact_filename, detect_host_platform, AppState};
use crate::torrent_index::TorrentIndex;
use crate::workflow_scheduler;
use crate::ws;
use anyhow::Result;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use pnos::error::ErrorCode;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

// ── 通用响应（统一使用 pnos 标准库）──────────────────────

/// 统一 API 响应（来自 pnos 标准库：code=0 成功，data 数据，message 提示）
pub use pnos::response::ApiResponse;

/// 应用错误（携带 pnos 标准错误码）
pub struct AppError {
    pub code: ErrorCode,
    pub message: String,
}

impl AppError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (
            status,
            Json(ApiResponse::<()>::error_code(self.code, self.message)),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        Self::new(ErrorCode::InternalError, e.to_string())
    }
}

pub type ApiResult<T> = Result<Json<ApiResponse<T>>, AppError>;

// ── 鉴权（axum 中间件统一接管，见 auth.rs） ──

// ── 健康检查（pnos-comm AppServer 同语义） ────────────────

/// 存活探针（总是 200）
async fn health_live() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "alive"})))
}

/// 就绪探针：runtime 注册成功才 200，独立模式 503
async fn health_ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let ready = state.pk_app.read().await.is_some();
    if ready {
        (StatusCode::OK, Json(serde_json::json!({"status": "ready"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "reason": "runtime not connected",
            })),
        )
    }
}

/// /health 兼容旧路径：存活 + 版本 + runtime 注册状态
async fn health_overview(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runtime_registered = state.pk_app.read().await.is_some();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "alive",
            "version": env!("CARGO_PKG_VERSION"),
            "runtime_registered": runtime_registered,
        })),
    )
}

// ── 节点管理 ──────────────────────────────────────────────

pub async fn list_nodes(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Node>> {
    let mut nodes = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, hostname, platform, arch, version, status, last_seen, registered_at, labels, active_tasks, bytes_downloaded, last_error, max_concurrent, max_bandwidth_bps, capabilities FROM nodes ORDER BY registered_at DESC")?;
            let nodes: Vec<Node> = stmt
                .query_map([], |r| {
                    let labels_str: String = r.get(8)?;
                    Ok(Node {
                        id: r.get::<_, String>(0)?.parse().unwrap(),
                        hostname: r.get(1)?,
                        platform: r.get(2)?,
                        arch: r.get(3)?,
                        version: r.get(4)?,
                        status: match r.get::<_, String>(5)?.as_str() {
                            "online" => NodeStatus::Online,
                            "busy" => NodeStatus::Busy,
                            "pending" => NodeStatus::Pending,
                            _ => NodeStatus::Offline,
                        },
                        last_seen: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(6)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        registered_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(7)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        labels: serde_json::from_str(&labels_str).unwrap_or_default(),
                        active_tasks: r.get(9)?,
                        bytes_downloaded: r.get(10)?,
                        last_error: r.get(11)?,
                        max_concurrent: r.get(12)?,
                        max_bandwidth_bps: r.get(13)?,
                        capabilities: {
                            let cap_str: Option<String> = r.get(14).ok();
                            cap_str.and_then(|s| serde_json::from_str(&s).ok())
                        },
                        total_speed_bps: 0,
                        active_tasks_progress: Vec::new(),
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(nodes)
        })
        .await?;

    // 从内存态填充实时状态
    let realtime_map = state.ws_mgr.get_all_realtime().await;
    for node in &mut nodes {
        if let Some(rt) = realtime_map.get(&node.id) {
            node.total_speed_bps = rt.total_speed_bps;
            node.active_tasks_progress = rt.active_tasks.clone();
        }
    }

    Ok(Json(ApiResponse::success(nodes)))
}

/// 查询单个节点实时状态
pub async fn get_node_realtime(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<crate::ws::NodeRealtime> {
    let rt = state.ws_mgr.get_node_realtime(id).await.unwrap_or_default();
    Ok(Json(ApiResponse::success(rt)))
}

pub async fn delete_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    let now = Utc::now().to_rfc3339();
    state
        .with_transaction(|conn| {
            // 记录到 deleted_nodes，该节点再次注册时需要审批
            conn.execute(
                "INSERT OR REPLACE INTO deleted_nodes (node_id, deleted_at, reason) VALUES (?1, ?2, 'manual_delete')",
                params![id.to_string(), now],
            )?;
            conn.execute("DELETE FROM nodes WHERE id = ?1", params![id.to_string()])?;
            Ok(())
        })
        .await?;
    // 主动通知 spde 节点已被删除，使其立即暂停任务并重新注册
    ws::notify_node_deleted(&state, id).await;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

/// 批量清理离线节点（DELETE /api/v1/nodes）
pub async fn purge_offline_nodes(State(state): State<Arc<AppState>>) -> ApiResult<u64> {
    let now = Utc::now().to_rfc3339();
    let deleted = state
        .with_transaction(|conn| {
            // 先查出所有 offline 节点的 id，记录到 deleted_nodes
            let offline_ids: Vec<String> = conn
                .prepare("SELECT id FROM nodes WHERE status = 'offline'")?
                .query_map([], |r| r.get::<_, String>(0))?
                .filter_map(|r| r.ok())
                .collect();
            for nid in &offline_ids {
                conn.execute(
                    "INSERT OR REPLACE INTO deleted_nodes (node_id, deleted_at, reason) VALUES (?1, ?2, 'purge_offline')",
                    params![nid, now],
                )?;
            }
            let n = conn.execute("DELETE FROM nodes WHERE status = 'offline'", [])?;
            Ok((n as u64, offline_ids))
        })
        .await?;
    let (deleted, offline_ids) = deleted;
    // 主动通知所有被删的离线节点（如果还连着的话）
    for nid_str in &offline_ids {
        if let Ok(nid) = uuid::Uuid::parse_str(nid_str) {
            ws::notify_node_deleted(&state, nid).await;
        }
    }
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(deleted)))
}

// ── 节点能力参数管理 ──────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct UpdateNodeCapabilitiesReq {
    #[serde(default)]
    pub max_concurrent: Option<u32>,
    #[serde(default)]
    pub max_bandwidth_bps: Option<u64>,
    /// 通用能力参数合并（pk 不认识的字段透传，不做解析）
    #[serde(default)]
    pub capabilities: Option<serde_json::Value>,
}

/// 修改节点能力参数（合并模式：pk 只改认识的字段，其他保留 spde 原值）
pub async fn update_node_capabilities(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateNodeCapabilitiesReq>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            // 读取节点当前的 capabilities（spde 上报的原始能力）
            let current_cap_str: Option<String> = conn
                .query_row("SELECT capabilities FROM nodes WHERE id = ?1", params![id.to_string()], |r| r.get(0))
                .ok();
            let mut current_cap: serde_json::Value = current_cap_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));

            // 合并模式：pk 认识的字段覆盖，其他保留
            if let Some(obj) = current_cap.as_object_mut() {
                if let Some(mc) = req.max_concurrent {
                    obj.insert("max_concurrent".to_string(), serde_json::json!(mc));
                }
                if let Some(mb) = req.max_bandwidth_bps {
                    obj.insert("max_bandwidth_bps".to_string(), serde_json::json!(mb));
                }
                // 合并通用 capabilities（pk 不认识的字段透传）
                if let Some(extra) = &req.capabilities {
                    if let Some(extra_obj) = extra.as_object() {
                        for (k, v) in extra_obj {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            // 更新数据库：独立字段 + 合并后的 capabilities JSON
            conn.execute(
                "UPDATE nodes SET max_concurrent=?1, max_bandwidth_bps=?2, capabilities=?3 WHERE id=?4",
                params![
                    req.max_concurrent,
                    req.max_bandwidth_bps,
                    current_cap.to_string(),
                    id.to_string()
                ],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

// ── 节点审批（删除后再次注册需人工同意） ──────────────────

/// 同意待审批节点
pub async fn approve_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            // 状态改为 online
            conn.execute(
                "UPDATE nodes SET status='online' WHERE id = ?1 AND status='pending'",
                params![id.to_string()],
            )?;
            // 从 deleted_nodes 移除，以后重启不再标记为 pending
            conn.execute(
                "DELETE FROM deleted_nodes WHERE node_id = ?1",
                params![id.to_string()],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

/// 拒绝待审批节点
pub async fn reject_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            // 拒绝不删除节点，保持 pending 状态，用户可随时再点同意
            // 在 labels 中添加 rejected 标记，便于前端识别
            let labels_str: String = conn
                .query_row(
                    "SELECT labels FROM nodes WHERE id = ?1",
                    params![id.to_string()],
                    |r| r.get(0),
                )
                .unwrap_or_else(|_| "[]".to_string());
            let mut labels: Vec<String> = serde_json::from_str(&labels_str).unwrap_or_default();
            if !labels.iter().any(|l| l == "rejected") {
                labels.push("rejected".to_string());
            }
            conn.execute(
                "UPDATE nodes SET labels=?1 WHERE id=?2 AND status='pending'",
                params![serde_json::to_string(&labels)?, id.to_string()],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

// ── 任务管理 ──────────────────────────────────────────────

pub async fn list_tasks(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Task>> {
    let tasks = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, name, url, filename, enable, created_at, note, overrides FROM tasks ORDER BY created_at DESC")?;
            let tasks: Vec<Task> = stmt
                .query_map([], |r| {
                    let overrides_str: String = r.get(7)?;
                    Ok(Task {
                        id: r.get::<_, String>(0)?.parse().unwrap(),
                        name: r.get(1)?,
                        url: r.get(2)?,
                        filename: r.get(3)?,
                        enable: r.get::<_, i64>(4)? != 0,
                        created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        note: r.get(6)?,
                        overrides: serde_json::from_str(&overrides_str).unwrap_or_default(),
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(tasks)
        })
        .await?;
    Ok(Json(ApiResponse::success(tasks)))
}

pub async fn create_task(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateTaskReq>,
) -> ApiResult<Task> {
    let task = Task {
        id: Uuid::new_v4(),
        name: req.name,
        url: req.url,
        filename: req.filename,
        enable: req.enable,
        created_at: Utc::now(),
        note: req.note,
        overrides: req.overrides,
    };
    state
        .with_transaction(|conn| {
            conn.execute(
                "INSERT INTO tasks VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    task.id.to_string(),
                    task.name,
                    task.url,
                    task.filename,
                    task.enable,
                    task.created_at.to_rfc3339(),
                    task.note,
                    serde_json::to_string(&task.overrides)?,
                ],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(task)))
}

pub async fn update_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateTaskReq>,
) -> ApiResult<Task> {
    let task = state
        .with_transaction(|conn| {
            let existing: Option<Task> = conn
                .query_row(
                    "SELECT id, name, url, filename, enable, created_at, note, overrides FROM tasks WHERE id = ?1",
                    params![id.to_string()],
                    |r| {
                        let overrides_str: String = r.get(7)?;
                        Ok(Task {
                            id: r.get::<_, String>(0)?.parse().unwrap(),
                            name: r.get(1)?,
                            url: r.get(2)?,
                            filename: r.get(3)?,
                            enable: r.get::<_, i64>(4)? != 0,
                            created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                            note: r.get(6)?,
                            overrides: serde_json::from_str(&overrides_str).unwrap_or_default(),
                        })
                    },
                )
                .ok();
            let Some(mut task) = existing else {
                return Err(anyhow::anyhow!("任务不存在"));
            };
            if let Some(name) = req.name { task.name = name; }
            if let Some(url) = req.url { task.url = url; }
            if let Some(filename) = req.filename { task.filename = filename; }
            if let Some(enable) = req.enable { task.enable = enable; }
            if let Some(note) = req.note { task.note = note; }
            if let Some(overrides) = req.overrides { task.overrides = overrides; }
            conn.execute(
                "UPDATE tasks SET name=?1, url=?2, filename=?3, enable=?4, note=?5, overrides=?6 WHERE id=?7",
                params![
                    task.name, task.url, task.filename, task.enable, task.note,
                    serde_json::to_string(&task.overrides)?, id.to_string(),
                ],
            )?;
            Ok(task)
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(task)))
}

pub async fn delete_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            scheduler::cancel_task(conn, id)?;
            conn.execute("DELETE FROM tasks WHERE id = ?1", params![id.to_string()])?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

/// 取消任务（不删除任务记录，仅将 pending/running 状态的 dispatch 标记为 cancelled）
pub async fn cancel_task_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            scheduler::cancel_task(conn, id)?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

// ── 运行记录 ──────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RunQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    pub node_id: Option<Uuid>,
}

fn default_limit() -> i64 {
    200
}

pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Query(q): Query<RunQuery>,
) -> ApiResult<Vec<RunRecord>> {
    let runs = state
        .with_conn(|conn| {
            let runs: Vec<RunRecord> = if let Some(nid) = q.node_id {
                let mut stmt = conn.prepare("SELECT id, task_id, dispatch_id, node_id, task_name, url, filename, file_size, downloaded_bytes, elapsed_secs, avg_speed_mbps, status, success_chunks, failed_chunks, error_msg, timestamp FROM runs WHERE node_id = ?1 ORDER BY timestamp DESC LIMIT ?2")?;
                let rows: Vec<RunRecord> = stmt.query_map(params![nid.to_string(), q.limit], map_run)?.filter_map(|r| r.ok()).collect();
                rows
            } else {
                let mut stmt = conn.prepare("SELECT id, task_id, dispatch_id, node_id, task_name, url, filename, file_size, downloaded_bytes, elapsed_secs, avg_speed_mbps, status, success_chunks, failed_chunks, error_msg, timestamp FROM runs ORDER BY timestamp DESC LIMIT ?1")?;
                let rows: Vec<RunRecord> = stmt.query_map(params![q.limit], map_run)?.filter_map(|r| r.ok()).collect();
                rows
            };
            Ok(runs)
        })
        .await?;
    Ok(Json(ApiResponse::success(runs)))
}

fn map_run(r: &rusqlite::Row) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: r.get::<_, String>(0)?.parse().unwrap(),
        task_id: r.get::<_, Option<String>>(1)?.and_then(|s| s.parse().ok()),
        dispatch_id: r.get::<_, Option<String>>(2)?.and_then(|s| s.parse().ok()),
        node_id: r.get::<_, String>(3)?.parse().unwrap(),
        task_name: r.get(4)?,
        url: r.get(5)?,
        filename: r.get(6)?,
        file_size: r.get(7)?,
        downloaded_bytes: r.get(8)?,
        elapsed_secs: r.get(9)?,
        avg_speed_mbps: r.get(10)?,
        status: r.get(11)?,
        success_chunks: r.get(12)?,
        failed_chunks: r.get(13)?,
        error_msg: r.get(14)?,
        timestamp: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(15)?)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(Utc::now()),
    })
}

// ── 总览 / 默认配置 ───────────────────────────────────────

pub async fn get_overview(State(state): State<Arc<AppState>>) -> ApiResult<Overview> {
    state.refresh_online().await;
    let ov = state.with_conn(scheduler::overview).await?;
    Ok(Json(ApiResponse::success(ov)))
}

/// 版本信息接口（pcdn-keeper 场景下返回 pk + spde 组合版本）
#[derive(Debug, Serialize)]
pub struct VersionInfo {
    pub pk_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spde_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pcdn_keeper_version: Option<String>,
}

pub async fn get_version() -> ApiResult<VersionInfo> {
    let info = VersionInfo {
        pk_version: env!("CARGO_PKG_VERSION").to_string(),
        spde_version: std::env::var("SPDE_VERSION").ok(),
        pcdn_keeper_version: std::env::var("PCDN_KEEPER_VERSION").ok(),
    };
    Ok(Json(ApiResponse::success(info)))
}

pub async fn get_defaults(State(state): State<Arc<AppState>>) -> ApiResult<SpdeDefaults> {
    Ok(Json(ApiResponse::success(state.cfg.spde_defaults.clone())))
}

/// 节点拉取 YAML 格式配置（SPDE 节点调用）
pub async fn get_node_config_yaml(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<Uuid>,
) -> impl IntoResponse {
    let master_url = format!("http://{}", state.cfg.listen);
    let heartbeat_interval = state.cfg.heartbeat_timeout_secs / 3;
    let yaml = spde_cfg::render_config(
        &state.cfg.spde_defaults,
        &[],
        &master_url,
        node_id,
        heartbeat_interval,
    );
    ([("content-type", "application/yaml; charset=utf-8")], yaml).into_response()
}

// ── Agent 接口（节点调用） ────────────────────────────────

/// 判断是否为内部地址（本地回环或内网IP）
fn is_internal_addr(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        std::net::IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified(),
    }
}

pub async fn agent_register(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Json(mut req): Json<AgentRegisterReq>,
) -> ApiResult<AgentRegisterResp> {
    let node_id = req.node_id.unwrap_or_else(Uuid::new_v4);
    let now = Utc::now();

    // 自动标记内部节点：本地回环或内网IP连接的agent
    if is_internal_addr(&addr.ip()) && !req.labels.iter().any(|l| l == "internal=true") {
        req.labels.push("internal=true".to_string());
    }
    let node_status: String = state
        .with_transaction(|conn| {
            // 检查该节点是否被删除过（删除过的节点再次注册需要审批）
            let was_deleted: bool = conn
                .query_row("SELECT COUNT(*) FROM deleted_nodes WHERE node_id = ?1", params![node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);

            let exists: bool = conn
                .query_row("SELECT COUNT(*) FROM nodes WHERE id = ?1", params![node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);

            if exists {
                // 已存在的节点：pending/busy 状态保持不变，其他更新为 online
                let current_status: String = conn
                    .query_row("SELECT status FROM nodes WHERE id = ?1", params![node_id.to_string()], |r| r.get(0))
                    .unwrap_or_else(|_| "online".to_string());
                let new_status = match current_status.as_str() {
                    "pending" | "busy" => current_status.as_str(),
                    _ => "online",
                };
                conn.execute(
                    "UPDATE nodes SET hostname=?1, platform=?2, arch=?3, version=?4, status=?5, last_seen=?6, labels=?7, max_concurrent=?8, max_bandwidth_bps=?9, capabilities=?10 WHERE id=?11",
                    params![req.hostname, req.platform, req.arch, req.version, new_status, now.to_rfc3339(), serde_json::to_string(&req.labels)?, req.max_concurrent, req.max_bandwidth_bps, req.capabilities.as_ref().map(|v| v.to_string()), node_id.to_string()],
                )?;
                Ok(new_status.to_string())
            } else {
                // 新节点：被删过的状态为 pending（待审批），否则为 online（自动通过）
                let init_status = if was_deleted { "pending" } else { "online" };
                conn.execute(
                    "INSERT INTO nodes VALUES (?1,?2,?3,?4,?5,?6,?7,?7,?8,0,0,NULL,?9,?10,?11)",
                    params![node_id.to_string(), req.hostname, req.platform, req.arch, req.version, init_status, now.to_rfc3339(), serde_json::to_string(&req.labels)?, req.max_concurrent, req.max_bandwidth_bps, req.capabilities.as_ref().map(|v| v.to_string())],
                )?;
                if was_deleted {
                    tracing::info!("节点 {} 曾被删除，再次注册标记为 pending 待审批", node_id);
                }
                Ok(init_status.to_string())
            }
        })
        .await?;

    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(AgentRegisterResp {
        node_id,
        poll_interval_secs: state.cfg.heartbeat_timeout_secs / 2,
        master_listen: state.cfg.listen.clone(),
        status: node_status,
    })))
}

pub async fn agent_heartbeat(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<Uuid>,
    Json(req): Json<AgentHeartbeatReq>,
) -> ApiResult<()> {
    let now = Utc::now();
    state
        .with_transaction(|conn| {
            // 节点不存在时直接忽略（被删除的节点不会通过心跳自动恢复，必须重新 register）
            let exists: bool = conn
                .query_row("SELECT COUNT(*) FROM nodes WHERE id = ?1", params![node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);
            if !exists {
                return Ok(());
            }
            // 查询节点当前状态和自定义的最大并发数
            let (current_status, node_max_concurrent): (String, Option<u32>) = conn
                .query_row("SELECT status, max_concurrent FROM nodes WHERE id = ?1", params![node_id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap_or_else(|_| ("online".to_string(), None));
            if current_status == "pending" {
                conn.execute(
                    "UPDATE nodes SET last_seen=?1, active_tasks=?2, bytes_downloaded=?3 WHERE id=?4",
                    params![now.to_rfc3339(), req.active_tasks, req.bytes_downloaded, node_id.to_string()],
                )?;
            } else {
                // 优先用节点自定义的最大并发数，否则用全局默认
                let max_concurrent = node_max_concurrent.unwrap_or(state.cfg.spde_defaults.max_concurrent);
                let new_status = if req.active_tasks >= max_concurrent { "busy" } else { "online" };
                conn.execute(
                    "UPDATE nodes SET status=?1, last_seen=?2, active_tasks=?3, bytes_downloaded=?4 WHERE id=?5",
                    params![new_status, now.to_rfc3339(), req.active_tasks, req.bytes_downloaded, node_id.to_string()],
                )?;
            }
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

pub async fn agent_fetch_config(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentFetchReq>,
) -> ApiResult<NodeConfig> {
    let now = Utc::now();
    state
        .with_transaction(|conn| {
            // 节点不存在时直接忽略（不自动创建，必须通过 register 注册）
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM nodes WHERE id = ?1",
                    params![req.node_id.to_string()],
                    |r| r.get::<_, i64>(0),
                )
                .map(|c| c > 0)
                .unwrap_or(false);
            if !exists {
                return Ok(());
            }
            // pending/busy 状态的节点保持不变，只更新 last_seen，不改为 online
            let current_status: String = conn
                .query_row(
                    "SELECT status FROM nodes WHERE id = ?1",
                    params![req.node_id.to_string()],
                    |r| r.get(0),
                )
                .unwrap_or_else(|_| "online".to_string());
            if current_status == "pending" || current_status == "busy" {
                conn.execute(
                    "UPDATE nodes SET last_seen=?1 WHERE id=?2",
                    params![now.to_rfc3339(), req.node_id.to_string()],
                )?;
            } else {
                conn.execute(
                    "UPDATE nodes SET status='online', last_seen=?1 WHERE id=?2",
                    params![now.to_rfc3339(), req.node_id.to_string()],
                )?;
            }
            Ok(())
        })
        .await?;

    // 拉模式：节点通过 /agent/claim 领取任务，这里返回空配置
    let cfg = NodeConfig {
        dispatch_id: Uuid::nil(),
        master: format!(
            "http://127.0.0.1:{}",
            state.cfg.listen.split(':').next_back().unwrap_or("5566")
        ),
        token: if state.cfg.token.is_empty() {
            None
        } else {
            Some(state.cfg.token.clone())
        },
        tasks: vec![],
    };
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(cfg)))
}

pub async fn agent_report(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentReportReq>,
) -> ApiResult<RunRecord> {
    let rec = state
        .with_transaction(|conn| scheduler::apply_report(conn, &req))
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(rec)))
}

/// 节点从共享待下发池领取一个任务
pub async fn agent_claim(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentClaimReq>,
) -> Response {
    let result = state
        .with_transaction(|conn| scheduler::claim_task(conn, req.node_id))
        .await;

    match result {
        Ok(Some(node_task)) => {
            let resp = ClaimTaskResp {
                dispatch_id: node_task.dispatch_id,
                task_id: node_task.task.id,
                name: node_task.task.name,
                url: node_task.task.url,
                filename: node_task.task.filename,
                overrides: node_task.task.overrides,
            };
            state.frontend_ws_mgr.notify_update();
            (StatusCode::OK, Json(ApiResponse::success(resp))).into_response()
        }
        Ok(None) => {
            // 池子空，返回 204
            (StatusCode::NO_CONTENT, Json(ApiResponse::success(()))).into_response()
        }
        Err(e) => {
            let resp = ApiResponse::<()>::error_code(ErrorCode::InternalError, e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

// ── 工作流管理 ────────────────────────────────────────────

pub async fn list_workflows(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Workflow>> {
    let wfs = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows ORDER BY created_at DESC")?;
            let wfs: Vec<Workflow> = stmt.query_map([], map_workflow)?.filter_map(|r| r.ok()).collect();
            Ok(wfs)
        })
        .await?;
    Ok(Json(ApiResponse::success(wfs)))
}

fn map_workflow(r: &rusqlite::Row) -> rusqlite::Result<Workflow> {
    let schedule_str: String = r.get(3)?;
    let task_ids_str: String = r.get(4)?;
    let target_str: String = r.get(5)?;
    let node_ids_str: String = r.get(6)?;
    Ok(Workflow {
        id: r.get::<_, String>(0)?.parse().unwrap(),
        name: r.get(1)?,
        enable: r.get::<_, i64>(2)? != 0,
        schedule: serde_json::from_str(&schedule_str)
            .unwrap_or(WorkflowSchedule::Once { at: Utc::now() }),
        task_ids: serde_json::from_str(&task_ids_str).unwrap_or_default(),
        target: match target_str.as_str() {
            "all" => AssignmentTarget::All,
            "nodes" => AssignmentTarget::Nodes,
            _ => AssignmentTarget::Any,
        },
        node_ids: serde_json::from_str(&node_ids_str).unwrap_or_default(),
        next_run_at: r.get::<_, Option<String>>(7)?.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        }),
        last_run_at: r.get::<_, Option<String>>(8)?.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        }),
        last_run_status: r.get(9)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(10)?)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(Utc::now()),
    })
}

pub async fn create_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateWorkflowReq>,
) -> ApiResult<Workflow> {
    let now = Utc::now();
    let next_run = workflow_scheduler::compute_next_run(&req.schedule, None, now);
    let wf = Workflow {
        id: Uuid::new_v4(),
        name: req.name,
        enable: req.enable,
        schedule: req.schedule,
        task_ids: req.task_ids,
        target: req.target,
        node_ids: req.node_ids,
        next_run_at: next_run,
        last_run_at: None,
        last_run_status: None,
        created_at: now,
    };
    state
        .with_transaction(|conn| {
            conn.execute(
                "INSERT INTO workflows VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    wf.id.to_string(),
                    wf.name,
                    wf.enable,
                    serde_json::to_string(&wf.schedule)?,
                    serde_json::to_string(&wf.task_ids)?,
                    serde_json::to_string(&wf.target)?,
                    serde_json::to_string(&wf.node_ids)?,
                    wf.next_run_at.map(|t| t.to_rfc3339()),
                    None::<String>,
                    None::<String>,
                    wf.created_at.to_rfc3339(),
                ],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(wf)))
}

pub async fn update_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateWorkflowReq>,
) -> ApiResult<Workflow> {
    let wf = state
        .with_transaction(|conn| {
            let existing: Option<Workflow> = conn
                .query_row("SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows WHERE id = ?1", params![id.to_string()], map_workflow)
                .ok();
            let Some(mut wf) = existing else {
                return Err(anyhow::anyhow!("工作流不存在"));
            };
            if let Some(name) = req.name { wf.name = name; }
            if let Some(enable) = req.enable { wf.enable = enable; }
            if let Some(schedule) = req.schedule {
                wf.schedule = schedule;
                wf.next_run_at = workflow_scheduler::compute_next_run(&wf.schedule, None, Utc::now());
            }
            if let Some(task_ids) = req.task_ids { wf.task_ids = task_ids; }
            if let Some(target) = req.target { wf.target = target; }
            if let Some(node_ids) = req.node_ids { wf.node_ids = node_ids; }
            conn.execute(
                "UPDATE workflows SET name=?1, enable=?2, schedule=?3, task_ids=?4, target=?5, node_ids=?6, next_run_at=?7 WHERE id=?8",
                params![
                    wf.name, wf.enable,
                    serde_json::to_string(&wf.schedule)?,
                    serde_json::to_string(&wf.task_ids)?,
                    serde_json::to_string(&wf.target)?,
                    serde_json::to_string(&wf.node_ids)?,
                    wf.next_run_at.map(|t| t.to_rfc3339()),
                    id.to_string(),
                ],
            )?;
            Ok(wf)
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(wf)))
}

pub async fn delete_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            conn.execute(
                "DELETE FROM workflow_runs WHERE workflow_id = ?1",
                params![id.to_string()],
            )?;
            conn.execute(
                "DELETE FROM workflows WHERE id = ?1",
                params![id.to_string()],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::success(())))
}

pub async fn get_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<WorkflowDetail> {
    let wf = state
        .with_conn(|conn| {
            let wf = conn
                .query_row("SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows WHERE id = ?1", params![id.to_string()], map_workflow)
                .ok();
            Ok(wf)
        })
        .await?;
    let Some(wf) = wf else {
        return Err(AppError::new(ErrorCode::WorkflowNotFound, "工作流不存在"));
    };

    let runs = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, workflow_id, workflow_name, triggered_at, status, task_count, success_count, failed_count, dispatch_ids, error_msg FROM workflow_runs WHERE workflow_id = ?1 ORDER BY triggered_at DESC LIMIT 50")?;
            let runs: Vec<WorkflowRun> = stmt.query_map(params![id.to_string()], map_workflow_run)?.filter_map(|r| r.ok()).collect();
            Ok(runs)
        })
        .await?;

    Ok(Json(ApiResponse::success(WorkflowDetail {
        workflow: wf,
        runs,
    })))
}

fn map_workflow_run(r: &rusqlite::Row) -> rusqlite::Result<WorkflowRun> {
    let dispatch_ids_str: String = r.get(8)?;
    Ok(WorkflowRun {
        id: r.get::<_, String>(0)?.parse().unwrap(),
        workflow_id: r.get::<_, String>(1)?.parse().unwrap(),
        workflow_name: r.get(2)?,
        triggered_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(3)?)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(Utc::now()),
        status: r.get(4)?,
        task_count: r.get(5)?,
        success_count: r.get(6)?,
        failed_count: r.get(7)?,
        dispatch_ids: serde_json::from_str(&dispatch_ids_str).unwrap_or_default(),
        error_msg: r.get(9)?,
    })
}

pub async fn trigger_workflow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<WorkflowRun> {
    let run = workflow_scheduler::trigger_workflow(&state, id).await?;
    state.frontend_ws_mgr.notify_update();
    match run {
        Some(r) => Ok(Json(ApiResponse::success(r))),
        None => Err(AppError::new(ErrorCode::WorkflowNotFound, "工作流不存在")),
    }
}

pub async fn list_workflow_runs(State(state): State<Arc<AppState>>) -> ApiResult<Vec<WorkflowRun>> {
    let runs = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, workflow_id, workflow_name, triggered_at, status, task_count, success_count, failed_count, dispatch_ids, error_msg FROM workflow_runs ORDER BY triggered_at DESC LIMIT 100")?;
            let runs: Vec<WorkflowRun> = stmt.query_map([], map_workflow_run)?.filter_map(|r| r.ok()).collect();
            Ok(runs)
        })
        .await?;
    Ok(Json(ApiResponse::success(runs)))
}

// ── 下发记录（执行页面） ──────────────────────────────────

pub async fn list_dispatches(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Dispatch>> {
    let dispatches = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, task_id, node_id, state, created_at, updated_at, claimed_at, target, allowed_nodes FROM dispatches ORDER BY created_at DESC LIMIT 500")?;
            let dispatches: Vec<Dispatch> = stmt
                .query_map([], |r| {
                    let target_str: String = r.get(7)?;
                    let allowed_str: String = r.get(8)?;
                    Ok(Dispatch {
                        id: r.get::<_, String>(0)?.parse().unwrap(),
                        task_id: r.get::<_, String>(1)?.parse().unwrap(),
                        node_id: r.get::<_, Option<String>>(2)?.and_then(|s| s.parse().ok()),
                        state: match r.get::<_, String>(3)?.as_str() {
                            "acked" => DispatchState::Acked,
                            "running" => DispatchState::Running,
                            "success" => DispatchState::Success,
                            "failed" => DispatchState::Failed,
                            "cancelled" => DispatchState::Cancelled,
                            _ => DispatchState::Pending,
                        },
                        created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(4)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        updated_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        claimed_at: r.get::<_, Option<String>>(6)?.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
                        target: match target_str.as_str() {
                            "all" => AssignmentTarget::All,
                            "nodes" => AssignmentTarget::Nodes,
                            _ => AssignmentTarget::Any,
                        },
                        allowed_nodes: serde_json::from_str(&allowed_str).unwrap_or_default(),
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(dispatches)
        })
        .await?;
    Ok(Json(ApiResponse::success(dispatches)))
}

// ── 二进制分发 ─────────────────────────────────

pub async fn serve_web(State(_state): State<Arc<AppState>>, Path(path): Path<String>) -> Response {
    let safe_path = if path.is_empty() {
        "index.html".into()
    } else {
        path
    };
    let web_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("web");
    let file_path = web_dir.join(&safe_path);
    if file_path.exists() && file_path.is_file() {
        let content = tokio::fs::read(&file_path).await.unwrap_or_default();
        let mime = match file_path.extension().and_then(|e| e.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("js") => "application/javascript; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("json") => "application/json",
            Some("svg") => "image/svg+xml",
            Some("png") => "image/png",
            Some("ico") => "image/x-icon",
            _ => "application/octet-stream",
        };
        ([("content-type", mime)], content).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Not Found").into_response()
    }
}

#[derive(Serialize)]
pub struct ArtifactInfo {
    pub platform: String,
    pub filename: String,
    pub present: bool,
    pub size: u64,
}

pub async fn list_artifacts(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<ArtifactInfo>>> {
    let platforms = [
        "windows-x86_64",
        "linux-x86_64",
        "linux-aarch64",
        "macos-x86_64",
        "macos-aarch64",
    ];
    let mut artifacts = Vec::new();
    for platform in platforms {
        if let Some(filename) = artifact_filename(platform) {
            let file_path = state.artifacts_dir.join(filename);
            let (present, size) = if file_path.exists() {
                let size = tokio::fs::metadata(&file_path)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
                (true, size)
            } else {
                (false, 0)
            };
            artifacts.push(ArtifactInfo {
                platform: platform.to_string(),
                filename: filename.to_string(),
                present,
                size,
            });
        }
    }
    Json(ApiResponse::success(artifacts))
}

pub async fn serve_artifact(
    State(state): State<Arc<AppState>>,
    Path(platform): Path<String>,
) -> Response {
    let filename = match artifact_filename(&platform) {
        Some(f) => f,
        None => return (StatusCode::BAD_REQUEST, "不支持的平台").into_response(),
    };
    let file_path = state.artifacts_dir.join(filename);
    if file_path.exists() {
        let content = tokio::fs::read(&file_path).await.unwrap_or_default();
        (
            [("content-type", "application/octet-stream")],
            [(
                "content-disposition",
                format!("attachment; filename=\"{}\"", filename),
            )],
            content,
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("未找到 {} 平台的二进制", platform),
        )
            .into_response()
    }
}

pub async fn host_info() -> Json<ApiResponse<HostInfo>> {
    let (platform, arch) = detect_host_platform();
    Json(ApiResponse::success(HostInfo { platform, arch }))
}

// ── 服务注册与发现（Agent 间点对点通信） ─────────────────

/// 查询服务列表（按能力/类型/健康/区域过滤）
pub async fn list_services(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ServiceQueryParams>,
) -> ApiResult<ServiceQueryResponse> {
    let agents = state
        .service_registry
        .query(
            q.capability.as_deref(),
            q.agent_type.as_deref(),
            q.health.as_deref(),
            q.region.as_deref(),
        )
        .await;
    let total = agents.len();
    Ok(Json(ApiResponse::success(ServiceQueryResponse {
        agents,
        total,
    })))
}

/// 获取单个服务详情
pub async fn get_service(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<ServiceAgentInfo> {
    match state.service_registry.get(id).await {
        Some(info) => Ok(Json(ApiResponse::success(info))),
        None => Err(AppError::new(ErrorCode::NotFound, "服务不存在")),
    }
}

/// 服务注册（HTTP 方式，供不使用 WebSocket 的客户端使用）
pub async fn register_service(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentRegisterReqV2>,
) -> ApiResult<AgentRegisterResp> {
    let node_id = req.node_id.unwrap_or_else(Uuid::new_v4);
    let now = Utc::now();

    // 注册到节点表
    state
        .with_transaction(|conn| {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM nodes WHERE id = ?1",
                    params![node_id.to_string()],
                    |r| r.get::<_, i64>(0),
                )
                .map(|c| c > 0)
                .unwrap_or(false);
            if exists {
                conn.execute(
                    "UPDATE nodes SET hostname=?1, platform=?2, arch=?3, version=?4, status='online', last_seen=?5 WHERE id=?6",
                    params![req.hostname, req.platform, req.arch, req.version, now.to_rfc3339(), node_id.to_string()],
                )?;
            } else {
                conn.execute(
                    "INSERT INTO nodes VALUES (?1,?2,?3,?4,?5,'online',?6,?6,'[]',0,0,NULL)",
                    params![node_id.to_string(), req.hostname, req.platform, req.arch, req.version, now.to_rfc3339()],
                )?;
            }
            Ok(())
        })
        .await?;

    // 如果提供了 serve 地址，注册到服务注册中心
    if let (Some(host), Some(port)) = (req.serve_host, req.serve_port) {
        let agent_type = req.agent_type.unwrap_or_else(|| "unknown".to_string());
        let info = ServiceAgentInfo {
            agent_id: node_id,
            name: req.hostname.clone(),
            agent_type,
            host,
            port,
            capabilities: req.capability_tags.clone(),
            health: "healthy".to_string(),
            load: 0.0,
            region: req.region.clone(),
            version: req.version.clone(),
            last_heartbeat: Some(now.to_rfc3339()),
        };
        let event = state.service_registry.register(info).await;
        // 广播服务变更
        ws::notify_service_changed(&state, &event).await;
    }

    Ok(Json(ApiResponse::success(AgentRegisterResp {
        node_id,
        poll_interval_secs: state.cfg.heartbeat_timeout_secs,
        master_listen: state.cfg.listen.clone(),
        status: "online".to_string(),
    })))
}

// ── 路由 ──────────────────────────────────────────────────

/// 将 `pnos` 标准库的路径常量转成 pk 完整路由：
/// 补上 `/api/v1` 前缀，并把标准里的 `:id` 占位适配为 axum 0.8 的 `{id}` 语法。
fn pnos_api(rel: &str) -> String {
    format!("{}{}", pnos::protocol::API_V1, rel).replace(":id", "{id}")
}

pub fn router(state: Arc<AppState>) -> axum::Router {
    use axum::routing::{delete, get, post, put};
    axum::Router::new()
        // 总览
        .route("/api/v1/overview", get(get_overview))
        .route("/api/v1/version", get(get_version))
        .route("/api/v1/defaults", get(get_defaults))
        .route("/api/v1/host-info", get(host_info))
        // 节点
        .route(&pnos_api(pnos::protocol::NODES), get(list_nodes))
        // pnos 未定义「清理离线节点」路径，保留 pk 自身端点
        .route("/api/v1/nodes/offline", delete(purge_offline_nodes))
        .route("/api/v1/nodes/{id}", delete(delete_node))
        .route("/api/v1/nodes/{id}/approve", post(approve_node))
        // pnos 未定义「节点能力修改」路径，保留 pk 自身端点
        .route(
            "/api/v1/nodes/{id}/capabilities",
            put(update_node_capabilities),
        )
        .route("/api/v1/nodes/{id}/reject", post(reject_node))
        .route("/api/v1/nodes/{id}/config.yaml", get(get_node_config_yaml))
        .route("/api/v1/nodes/{id}/realtime", get(get_node_realtime))
        // 任务
        .route(
            &pnos_api(pnos::protocol::TASKS),
            get(list_tasks).post(create_task),
        )
        .route(
            &pnos_api(pnos::protocol::TASK_DETAIL),
            put(update_task).delete(delete_task),
        )
        .route("/api/v1/tasks/{id}/cancel", post(cancel_task_handler))
        // 运行记录
        .route("/api/v1/runs", get(list_runs))
        // 下发记录
        .route("/api/v1/dispatches", get(list_dispatches))
        // 工作流
        .route(
            "/api/v1/workflows",
            get(list_workflows).post(create_workflow),
        )
        .route(
            "/api/v1/workflows/{id}",
            get(get_workflow)
                .put(update_workflow)
                .delete(delete_workflow),
        )
        .route(
            "/api/v1/workflows/{id}/trigger",
            post(trigger_workflow_handler),
        )
        .route("/api/v1/workflow-runs", get(list_workflow_runs))
        // Agent / 组件（统一走 pnos 标准路径 /components/*）
        .route(
            &pnos_api(pnos::protocol::COMPONENTS_REGISTER),
            post(agent_register),
        )
        .route(
            &pnos_api(pnos::protocol::COMPONENTS_HEARTBEAT),
            post(agent_heartbeat),
        )
        .route("/api/v1/agent/config", post(agent_fetch_config))
        .route("/api/v1/agent/report", post(agent_report))
        .route(&pnos_api(pnos::protocol::DISPATCH_CLAIM), post(agent_claim))
        // 服务注册与发现（统一走 pnos 标准路径 /components）
        .route(
            &pnos_api(pnos::protocol::COMPONENTS_LIST),
            get(list_services),
        )
        .route(
            &pnos_api(pnos::protocol::COMPONENT_DETAIL),
            get(get_service),
        )
        .route("/api/v1/services/register", post(register_service))
        // 监控
        .route("/api/v1/metrics", get(pk_metrics))
        // 种子索引
        .route("/api/v1/torrents", get(list_torrents).post(upsert_torrent))
        .route("/api/v1/torrents/top", get(top_torrents))
        .route("/api/v1/torrents/recent", get(recent_torrents))
        .route("/api/v1/torrents/stats", get(torrent_stats))
        .route(
            "/api/v1/torrents/{infohash}",
            get(get_torrent).delete(delete_torrent),
        )
        .route("/api/v1/torrents/{infohash}/files", get(torrent_files))
        // WebSocket
        .route("/api/v1/agent/ws", get(ws::ws_handler))
        .route("/api/v1/realtime/ws", get(ws::frontend_ws_handler))
        // 二进制分发
        .route("/api/v1/artifacts", get(list_artifacts))
        .route("/api/v1/artifacts/{platform}", get(serve_artifact))
        // 健康检查（/health 为 runtime 健康探测的默认路径）
        .route("/health", get(health_overview))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        // 管理面认证：X-Pnos-Token 校验，节点面/健康检查/静态资源白名单放行
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::auth_middleware,
        ))
        .with_state(state)
}

// ─── 种子索引 API ───

#[derive(Debug, Deserialize)]
struct TorrentQuery {
    q: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_torrents(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TorrentQuery>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    let limit = query.limit.unwrap_or(50).min(200);
    let offset = query.offset.unwrap_or(0);

    let result = if let Some(q) = &query.q {
        state.torrent_index.search(q, limit, offset)
    } else {
        state.torrent_index.get_top(limit)
    };

    match result {
        Ok(torrents) => Ok(axum::Json(torrents)),
        Err(e) => {
            tracing::error!("查询种子索引失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_torrent(
    State(state): State<Arc<AppState>>,
    Path(infohash): Path<String>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.get(&infohash) {
        Ok(Some(torrent)) => Ok(axum::Json(torrent)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("获取种子详情失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn torrent_files(
    State(state): State<Arc<AppState>>,
    Path(infohash): Path<String>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.get_files(&infohash) {
        Ok(files) => Ok(axum::Json(files)),
        Err(e) => {
            tracing::error!("获取种子文件列表失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn top_torrents(
    State(state): State<Arc<AppState>>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.get_top(20) {
        Ok(torrents) => Ok(axum::Json(torrents)),
        Err(e) => {
            tracing::error!("获取热门种子失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn recent_torrents(
    State(state): State<Arc<AppState>>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.get_recent(20) {
        Ok(torrents) => Ok(axum::Json(torrents)),
        Err(e) => {
            tracing::error!("获取最新种子失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn torrent_stats(
    State(state): State<Arc<AppState>>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.stats() {
        Ok(stats) => Ok(axum::Json(stats)),
        Err(e) => {
            tracing::error!("获取索引统计失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn upsert_torrent(
    State(state): State<Arc<AppState>>,
    axum::Json(torrent): axum::Json<TorrentIndex>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.upsert(&torrent) {
        Ok(_) => Ok(axum::Json(serde_json::json!({"status": "ok"}))),
        Err(e) => {
            tracing::error!("插入种子索引失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn delete_torrent(
    State(state): State<Arc<AppState>>,
    Path(infohash): Path<String>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    match state.torrent_index.delete(&infohash) {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => {
            tracing::error!("删除种子失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ─── 监控 API ───

use crate::metrics::PkStats;

async fn pk_metrics() -> impl axum::response::IntoResponse {
    let stats = PkStats::global();
    stats.record_api_request();
    axum::response::Response::builder()
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(stats.to_prometheus())
        .unwrap_or_else(|_| axum::response::Response::new(String::new()))
}
