use crate::models::*;
use crate::scheduler;
use crate::store::AppState;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use pnos::events::{
    WsMessage, EVENT_COMPONENT_REGISTERED, EVENT_CONFIG_CHANGED, EVENT_DISCOVERY_RESULT,
    EVENT_DISCOVERY_STARTED, EVENT_NODE_DELETED, EVENT_NODE_STATUS, EVENT_SERVICE_CHANGED,
    EVENT_TASK_COMPLETED, EVENT_TASK_FAILED, EVENT_TASK_NEW, EVENT_TASK_PROGRESS,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

// ── SPDE 控制面事件（pnos 信封之上的自定义 event_type）──────
//
// pnos 标准库只定义了「事件总线」语义的事件类型（task.* / node.* / component.* …）。
// pk ↔ spde 之间还有一组「控制面」消息（状态上报、任务进度、心跳、保活等），
// pnos 未对其建模；pk 统一以 pnos 的 WsMessage 信封承载，并在此定义 event_type。

/// 保活 ping（pk → spde）
pub const EVENT_PING: &str = "system.ping";
/// 保活 pong（spde → pk）
pub const EVENT_PONG: &str = "system.pong";
/// 节点心跳（spde → pk）
pub const EVENT_NODE_HEARTBEAT: &str = "node.heartbeat";
/// 任务开始（spde → pk）
pub const EVENT_TASK_STARTED: &str = "task.started";

/// 节点状态上报载荷（event_type = node.status）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusPayload {
    #[serde(default)]
    pub active_tasks: u32,
    #[serde(default)]
    pub bytes_downloaded: u64,
    #[serde(default)]
    pub busy: bool,
    #[serde(default)]
    pub total_speed_bps: u64,
    #[serde(default)]
    pub last_error: Option<String>,
}

/// 任务开始载荷（event_type = task.started）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStartedPayload {
    pub dispatch_id: Uuid,
}

/// 任务进度载荷（event_type = task.progress）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgressMsg {
    pub dispatch_id: Uuid,
    pub task_name: String,
    pub percent: f64,
    pub downloaded_bytes: u64,
    pub total_size: u64,
    pub speed_bps: u64,
    pub active_connections: u32,
    pub elapsed_secs: f64,
}

/// 任务完成/失败报告载荷（event_type = task.completed / task.failed）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReportMsg {
    #[serde(default)]
    pub dispatch_id: Option<Uuid>,
    #[serde(default)]
    pub task_id: Option<Uuid>,
    #[serde(default)]
    pub task_name: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub file_size: u64,
    #[serde(default)]
    pub downloaded_bytes: u64,
    #[serde(default)]
    pub elapsed_secs: f64,
    #[serde(default)]
    pub avg_speed_mbps: f64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub success_chunks: u64,
    #[serde(default)]
    pub failed_chunks: u64,
    #[serde(default)]
    pub error_msg: Option<String>,
}

/// WS 内注册载荷（event_type = component.registered）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPayload {
    pub node_id: Uuid,
    pub hostname: String,
    #[serde(default)]
    pub platform: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub version: String,
}

/// 节点心跳载荷（event_type = node.heartbeat）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    pub node_id: Uuid,
    #[serde(default)]
    pub active_tasks: u32,
    #[serde(default)]
    pub bytes_downloaded: u64,
}

/// 发现启动载荷（event_type = discovery.started）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryStartedMsg {
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub infohash: String,
}

/// 发现结果载荷（event_type = discovery.result）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryResultMsg {
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub infohash: String,
    #[serde(default)]
    pub peers_count: u32,
    #[serde(default)]
    pub success: bool,
}

// ── 连接管理 + 实时状态 ───────────────────────────────────
pub struct NodeConn {
    pub tx: mpsc::UnboundedSender<Message>,
    pub node_id: Option<Uuid>,
}

/// 单任务实时进度（内存态）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgressState {
    pub dispatch_id: Uuid,
    pub task_name: String,
    pub percent: f64,
    pub downloaded_bytes: u64,
    pub total_size: u64,
    pub speed_bps: u64,
    pub active_connections: u32,
    pub elapsed_secs: f64,
    pub updated_at: String,
}

/// 节点实时状态（内存态，不持久化）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeRealtime {
    pub total_speed_bps: u64,
    pub active_tasks: Vec<TaskProgressState>,
    pub updated_at: String,
}

pub struct WsManager {
    conns: RwLock<HashMap<Uuid, NodeConn>>,
    realtime: RwLock<HashMap<Uuid, NodeRealtime>>,
}

impl Default for WsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WsManager {
    pub fn new() -> Self {
        Self {
            conns: RwLock::new(HashMap::new()),
            realtime: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(
        &self,
        conn_id: Uuid,
        tx: mpsc::UnboundedSender<Message>,
        node_id: Option<Uuid>,
    ) {
        let mut conns = self.conns.write().await;
        conns.insert(conn_id, NodeConn { tx, node_id });
    }

    pub async fn bind_node(&self, conn_id: Uuid, node_id: Uuid) {
        let mut conns = self.conns.write().await;
        if let Some(c) = conns.get_mut(&conn_id) {
            c.node_id = Some(node_id);
        }
    }

    pub async fn remove(&self, conn_id: Uuid) {
        let mut conns = self.conns.write().await;
        conns.remove(&conn_id);
    }

    /// 向所有已绑定节点的连接广播消息
    pub async fn broadcast(&self, msg: &WsMessage) {
        let text = serde_json::to_string(msg).unwrap_or_default();
        let conns = self.conns.read().await;
        for c in conns.values() {
            if c.node_id.is_some() {
                let _ = c.tx.send(Message::Text(text.clone().into()));
            }
        }
    }

    /// 向特定节点发送消息（用于删除节点时主动通知 spde）
    pub async fn send_to_node(&self, node_id: Uuid, msg: &WsMessage) {
        let text = serde_json::to_string(msg).unwrap_or_default();
        let conns = self.conns.read().await;
        for c in conns.values() {
            if c.node_id == Some(node_id) {
                let _ = c.tx.send(Message::Text(text.clone().into()));
                tracing::info!("[ws] 已向节点 {} 发送消息: {:?}", node_id, msg);
            }
        }
    }

    /// 更新节点总速度
    pub async fn update_node_speed(&self, node_id: Uuid, total_speed_bps: u64) {
        let mut rt = self.realtime.write().await;
        let entry = rt.entry(node_id).or_default();
        entry.total_speed_bps = total_speed_bps;
        entry.updated_at = Utc::now().to_rfc3339();
    }

    /// 更新单任务实时进度
    pub async fn update_task_progress(&self, node_id: Uuid, progress: TaskProgressState) {
        let mut rt = self.realtime.write().await;
        let entry = rt.entry(node_id).or_default();
        entry.updated_at = Utc::now().to_rfc3339();
        if let Some(existing) = entry
            .active_tasks
            .iter_mut()
            .find(|t| t.dispatch_id == progress.dispatch_id)
        {
            *existing = progress;
        } else {
            entry.active_tasks.push(progress);
        }
    }

    /// 任务完成时移除
    pub async fn remove_task_progress(&self, node_id: Uuid, dispatch_id: Uuid) {
        let mut rt = self.realtime.write().await;
        if let Some(entry) = rt.get_mut(&node_id) {
            entry.active_tasks.retain(|t| t.dispatch_id != dispatch_id);
            entry.updated_at = Utc::now().to_rfc3339();
        }
    }

    /// 获取节点实时状态
    pub async fn get_node_realtime(&self, node_id: Uuid) -> Option<NodeRealtime> {
        let rt = self.realtime.read().await;
        rt.get(&node_id).cloned()
    }

    /// 获取所有节点实时状态
    pub async fn get_all_realtime(&self) -> HashMap<Uuid, NodeRealtime> {
        let rt = self.realtime.read().await;
        rt.clone()
    }
}

// ── 通知函数 ──────────────────────────────────────────────
pub async fn notify_config_changed(state: &Arc<AppState>) {
    let msg = WsMessage::new(EVENT_CONFIG_CHANGED, serde_json::json!({})).with_source("pk");
    state.ws_mgr.broadcast(&msg).await;
}

pub async fn notify_new_task(state: &Arc<AppState>) {
    let msg = WsMessage::new(EVENT_TASK_NEW, serde_json::json!({})).with_source("pk");
    state.ws_mgr.broadcast(&msg).await;
}

/// 通知指定节点：该节点已被删除（主动通知 spde 暂停任务并重新注册）
pub async fn notify_node_deleted(state: &Arc<AppState>, node_id: Uuid) {
    let msg = WsMessage::new(
        EVENT_NODE_DELETED,
        serde_json::json!({ "node_id": node_id }),
    )
    .with_source("pk");
    state.ws_mgr.send_to_node(node_id, &msg).await;
}

/// 通知所有节点：服务注册中心有变更（Agent 上下线/能力更新）
pub async fn notify_service_changed(
    state: &Arc<AppState>,
    event: &crate::models::ServiceChangedEvent,
) {
    let payload = serde_json::json!({
        "service_name": event.agent_id.clone(),
        "action": event.change_type.clone(),
        "address": event.host.clone(),
        "port": event.port,
        "healthy": event.health == "healthy",
        "agent_type": event.agent_type.clone(),
        "capabilities": event.capabilities.clone(),
        "region": event.region.clone(),
        "health": event.health.clone(),
        "load": event.load,
    });
    let msg = WsMessage::new(EVENT_SERVICE_CHANGED, payload).with_source("pk");
    state.ws_mgr.broadcast(&msg).await;
}

// ── WebSocket 处理 ────────────────────────────────────────
pub async fn ws_handler(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // 新协议：SPDE 通过 ?node_id=xxx 携带节点 ID
    let node_id = params.get("node_id").and_then(|s| Uuid::parse_str(s).ok());
    ws.on_upgrade(move |socket| handle_socket(socket, state, node_id))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, initial_node_id: Option<Uuid>) {
    let (mut sender, mut receiver) = socket.split();
    let conn_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    state.ws_mgr.register(conn_id, tx, initial_node_id).await;

    // URL 带了 node_id → 新协议，立即标记节点在线
    if let Some(nid) = initial_node_id {
        let now = Utc::now();
        if let Err(e) = state
            .with_transaction(move |conn| {
                conn.execute(
                    "UPDATE nodes SET status=CASE WHEN status='pending' THEN 'pending' ELSE 'online' END, last_seen=?1 WHERE id=?2",
                    params![now.to_rfc3339(), nid.to_string()],
                )?;
                Ok(())
            })
            .await
        {
            tracing::warn!("[ws] 连接时更新节点状态失败: {}", e);
        }
        tracing::info!("[ws] 节点连接 conn_id={} node_id={}", conn_id, nid);
    } else {
        tracing::info!(
            "[ws] 新连接 conn_id={} (旧协议，等待 register 消息)",
            conn_id
        );
    }

    // 发送任务：mpsc 消息 + 每 30 秒发 Ping 保活
    let send_task = tokio::spawn(async move {
        let mut ping = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    let msg = WsMessage::new(EVENT_PING, serde_json::json!({})).with_source("pk");
                    let text = serde_json::to_string(&msg).unwrap_or_default();
                    if sender.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Some(msg) = rx.recv() => {
                    if sender.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // 接收任务：处理客户端消息
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                if let Err(e) = handle_client_msg(&state, conn_id, initial_node_id, &text).await {
                    tracing::warn!("[ws] 处理消息失败: {}", e);
                }
            }
            Message::Binary(_) => {}
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    state.ws_mgr.remove(conn_id).await;
    send_task.abort();
    tracing::info!("[ws] 连接关闭 conn_id={}", conn_id);
}

async fn handle_client_msg(
    state: &Arc<AppState>,
    conn_id: Uuid,
    node_id: Option<Uuid>,
    text: &str,
) -> anyhow::Result<()> {
    let msg: WsMessage = serde_json::from_str(text)?;
    match msg.event_type.as_str() {
        // ── 节点状态上报 ──
        EVENT_NODE_STATUS => {
            let p: StatusPayload = msg.parse_payload().unwrap_or_default();
            if let Some(nid) = node_id {
                let now = Utc::now();
                let (active_tasks, bytes_downloaded) =
                    (p.active_tasks as i64, p.bytes_downloaded as i64);
                let total_speed_bps = p.total_speed_bps;
                state
                    .with_transaction(move |conn| {
                        conn.execute(
                            "UPDATE nodes SET status=CASE WHEN status='pending' THEN 'pending' ELSE 'online' END, last_seen=?1, active_tasks=?2, bytes_downloaded=?3 WHERE id=?4",
                            params![now.to_rfc3339(), active_tasks, bytes_downloaded, nid.to_string()],
                        )?;
                        Ok(())
                    })
                    .await?;
                // 更新内存中的实时总速度
                state.ws_mgr.update_node_speed(nid, total_speed_bps).await;
            }
        }

        // ── 任务开始 ──
        EVENT_TASK_STARTED => {
            if let Ok(p) = msg.parse_payload::<TaskStartedPayload>() {
                state
                    .with_transaction(move |conn| {
                        scheduler::mark_running(conn, p.dispatch_id)?;
                        Ok(())
                    })
                    .await?;
            }
        }

        // ── 任务实时进度 ──
        EVENT_TASK_PROGRESS => {
            if let (Some(nid), Ok(p)) = (node_id, msg.parse_payload::<TaskProgressMsg>()) {
                let progress = TaskProgressState {
                    dispatch_id: p.dispatch_id,
                    task_name: p.task_name,
                    percent: p.percent,
                    downloaded_bytes: p.downloaded_bytes,
                    total_size: p.total_size,
                    speed_bps: p.speed_bps,
                    active_connections: p.active_connections,
                    elapsed_secs: p.elapsed_secs,
                    updated_at: Utc::now().to_rfc3339(),
                };
                state.ws_mgr.update_task_progress(nid, progress).await;
            }
        }

        // ── 任务完成 / 失败报告 ──
        EVENT_TASK_COMPLETED | EVENT_TASK_FAILED => {
            if let (Some(nid), Ok(p)) = (node_id, msg.parse_payload::<TaskReportMsg>()) {
                // 任务完成，从实时状态中移除
                if let Some(did) = p.dispatch_id {
                    state.ws_mgr.remove_task_progress(nid, did).await;
                }
                let req = AgentReportReq {
                    node_id: nid,
                    dispatch_id: p.dispatch_id,
                    task_id: p.task_id,
                    task_name: p.task_name,
                    url: p.url,
                    filename: p.filename,
                    file_size: p.file_size,
                    downloaded_bytes: p.downloaded_bytes,
                    elapsed_secs: p.elapsed_secs,
                    avg_speed_mbps: p.avg_speed_mbps,
                    status: p.status,
                    success_chunks: p.success_chunks,
                    failed_chunks: p.failed_chunks,
                    error_msg: p.error_msg,
                };
                state
                    .with_transaction(move |conn| {
                        scheduler::apply_report(conn, &req)?;
                        Ok(())
                    })
                    .await?;
            }
        }

        // ── Ping 响应（保活） ──
        EVENT_PONG => {
            if let Some(nid) = node_id {
                let now = Utc::now();
                state
                    .with_transaction(move |conn| {
                        conn.execute(
                            "UPDATE nodes SET status=CASE WHEN status='pending' THEN 'pending' ELSE 'online' END, last_seen=?1 WHERE id=?2",
                            params![now.to_rfc3339(), nid.to_string()],
                        )?;
                        Ok(())
                    })
                    .await?;
            }
        }

        // ── WS 内注册 ──
        EVENT_COMPONENT_REGISTERED => {
            if let Ok(p) = msg.parse_payload::<RegisterPayload>() {
                let reg_node_id = p.node_id;
                state.ws_mgr.bind_node(conn_id, reg_node_id).await;
                let now = Utc::now();
                let host = p.hostname.clone();
                state
                    .with_transaction(move |conn| {
                        let exists: bool = conn
                            .query_row(
                                "SELECT COUNT(*) FROM nodes WHERE id = ?1",
                                params![reg_node_id.to_string()],
                                |r| r.get::<_, i64>(0),
                            )
                            .map(|c| c > 0)
                            .unwrap_or(false);
                        if exists {
                            conn.execute(
                                "UPDATE nodes SET hostname=?1, platform=?2, arch=?3, version=?4, status=CASE WHEN status='pending' THEN 'pending' ELSE 'online' END, last_seen=?5 WHERE id=?6",
                                params![p.hostname, p.platform, p.arch, p.version, now.to_rfc3339(), reg_node_id.to_string()],
                            )?;
                        } else {
                            conn.execute(
                                "INSERT INTO nodes VALUES (?1,?2,?3,?4,?5,'online',?6,?6,'[]',0,0,NULL)",
                                params![reg_node_id.to_string(), p.hostname, p.platform, p.arch, p.version, now.to_rfc3339()],
                            )?;
                        }
                        Ok(())
                    })
                    .await?;

                tracing::info!("[ws] 节点注册 node_id={} hostname={}", reg_node_id, host);
            }
        }

        // ── PDC 发现事件（pk 当前不消费，仅记录） ──
        EVENT_DISCOVERY_STARTED => {
            if let Ok(ds) = msg.parse_payload::<DiscoveryStartedMsg>() {
                tracing::debug!(
                    "[ws] PDC discovery started: task_id={} infohash={}",
                    ds.task_id,
                    ds.infohash,
                );
            }
        }
        EVENT_DISCOVERY_RESULT => {
            if let Ok(dr) = msg.parse_payload::<DiscoveryResultMsg>() {
                tracing::debug!(
                    "[ws] PDC discovery result: task_id={} infohash={} peers={} success={}",
                    dr.task_id,
                    dr.infohash,
                    dr.peers_count,
                    dr.success,
                );
            }
        }

        // ── 节点心跳 ──
        EVENT_NODE_HEARTBEAT => {
            if let Ok(p) = msg.parse_payload::<HeartbeatPayload>() {
                let hb_node_id = p.node_id;
                let active_tasks = p.active_tasks;
                let bytes_downloaded = p.bytes_downloaded as i64;
                let now = Utc::now();
                state
                    .with_transaction(move |conn| {
                        conn.execute(
                            "UPDATE nodes SET status=CASE WHEN status='pending' THEN 'pending' ELSE 'online' END, last_seen=?1, active_tasks=?2, bytes_downloaded=?3 WHERE id=?4",
                            params![now.to_rfc3339(), active_tasks as i64, bytes_downloaded, hb_node_id.to_string()],
                        )?;
                        Ok(())
                    })
                    .await?;
                // 同步更新服务注册中心健康状态
                let load = if active_tasks > 0 {
                    (active_tasks as f32 / 4.0).min(1.0)
                } else {
                    0.0
                };
                state
                    .service_registry
                    .update_health(hb_node_id, "healthy", load)
                    .await;
            }
        }

        // 其它事件类型：忽略（pk 不消费的通用事件）
        _ => {}
    }
    // 任何节点消息都可能导致前端需要更新，标记脏数据，50ms内推送
    state.frontend_ws_mgr.notify_update();
    Ok(())
}

// ── 给节点生成任务配置（claim 领取后使用） ────────────────
pub fn build_node_task_config(state: &AppState, node_task: &scheduler::NodeTask) -> NodeConfig {
    let defaults = &state.cfg.spde_defaults;
    let t = &node_task.task;
    let mut item = TaskItem {
        url: t.url.clone(),
        filename: t.filename.clone(),
        save_path: defaults.save_path.clone(),
        max_concurrent: defaults.max_concurrent,
        connections_per_file: defaults.connections_per_file,
        retry_times: defaults.retry_times,
        timeout: defaults.timeout,
        dry_run: defaults.dry_run,
        skip_tls_verify: defaults.skip_tls_verify,
        resume: defaults.resume,
        http_proxy: defaults.http_proxy.clone(),
        https_proxy: defaults.https_proxy.clone(),
    };
    let o = &t.overrides;
    if let Some(v) = o.max_concurrent {
        item.max_concurrent = v;
    }
    if let Some(v) = o.connections_per_file {
        item.connections_per_file = v;
    }
    if let Some(v) = o.retry_times {
        item.retry_times = v;
    }
    if let Some(v) = o.timeout {
        item.timeout = v;
    }
    if let Some(v) = o.skip_tls_verify {
        item.skip_tls_verify = v;
    }
    if let Some(v) = o.dry_run {
        item.dry_run = v;
    }
    if let Some(v) = o.save_path.clone() {
        item.save_path = v;
    }

    NodeConfig {
        dispatch_id: node_task.dispatch_id,
        master: format!(
            "http://127.0.0.1:{}",
            state.cfg.listen.split(':').next_back().unwrap_or("5566")
        ),
        token: if state.cfg.token.is_empty() {
            None
        } else {
            Some(state.cfg.token.clone())
        },
        tasks: vec![item],
    }
}

// ── 前端 WebSocket 实时推送 ────────────────────────────────
/// 前端实时推送消息
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FrontendPushMsg {
    /// 全量实时状态（每秒推送一次）
    Realtime {
        nodes: Vec<FrontendNodeState>,
        timestamp: String,
    },
    /// 保活 ping
    Ping,
}

/// 前端展示用的节点实时状态
#[derive(Debug, Clone, Serialize)]
pub struct FrontendNodeState {
    pub node_id: Uuid,
    pub hostname: String,
    pub platform: String,
    pub version: String,
    pub status: String,
    pub total_speed_bps: u64,
    pub active_tasks: Vec<TaskProgressState>,
    pub last_seen: String,
}

/// 前端 WebSocket 连接管理器
pub struct FrontendWsManager {
    conns: RwLock<HashMap<Uuid, mpsc::UnboundedSender<Message>>>,
    dirty: std::sync::atomic::AtomicBool,
}

impl Default for FrontendWsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontendWsManager {
    pub fn new() -> Self {
        Self {
            conns: RwLock::new(HashMap::new()),
            dirty: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// 标记有新数据需要推送（事件驱动，spde上报进度时调用）
    pub fn notify_update(&self) {
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// 检查并清除脏标志，返回是否需要推送
    pub fn check_and_clear_dirty(&self) -> bool {
        self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn register(&self, conn_id: Uuid, tx: mpsc::UnboundedSender<Message>) {
        let mut conns = self.conns.write().await;
        conns.insert(conn_id, tx);
        tracing::info!(
            "[frontend-ws] 前端连接 conn_id={}, 当前连接数={}",
            conn_id,
            conns.len()
        );
    }

    pub async fn remove(&self, conn_id: Uuid) {
        let mut conns = self.conns.write().await;
        conns.remove(&conn_id);
        tracing::info!(
            "[frontend-ws] 前端连接关闭 conn_id={}, 当前连接数={}",
            conn_id,
            conns.len()
        );
    }

    /// 向所有前端连接广播消息
    pub async fn broadcast(&self, msg: &FrontendPushMsg) {
        let text = match serde_json::to_string(msg) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("[frontend-ws] 序列化消息失败: {}", e);
                return;
            }
        };
        let conns = self.conns.read().await;
        for tx in conns.values() {
            let _ = tx.send(Message::Text(text.clone().into()));
        }
    }

    pub async fn conn_count(&self) -> usize {
        self.conns.read().await.len()
    }
}

/// 前端 WebSocket 处理入口
pub async fn frontend_ws_handler(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_frontend_socket(socket, state))
}

async fn handle_frontend_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let conn_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    state.frontend_ws_mgr.register(conn_id, tx).await;

    // 发送任务：mpsc 消息 + 每 30 秒发 Ping 保活
    let send_task = tokio::spawn(async move {
        let mut ping = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    let text = serde_json::to_string(&FrontendPushMsg::Ping).unwrap_or_default();
                    if sender.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Some(msg) = rx.recv() => {
                    if sender.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // 接收任务：前端消息（目前只处理 Ping/Pong，不处理其他）
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(_) => {}
            Message::Binary(_) => {}
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    state.frontend_ws_mgr.remove(conn_id).await;
    send_task.abort();
}

/// 启动实时状态广播任务（每秒向所有前端推送一次）
/// P3-20 从数据库查询节点基本信息（抽成函数，便于缓存复用）
/// 节点基本信息：(id, hostname, platform, arch, version, status, last_seen)
type NodeInfo = (String, String, String, String, String, String, String);

async fn query_nodes_from_db(state: &Arc<AppState>) -> anyhow::Result<Vec<NodeInfo>> {
    state
        .with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, hostname, platform, arch, version, status, last_seen FROM nodes ORDER BY hostname",
            )?;
            let nodes: Vec<NodeInfo> = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(nodes)
        })
        .await
}

pub fn spawn_realtime_broadcaster(state: Arc<AppState>) {
    tokio::spawn(async move {
        // 50ms 检查一次脏标志，事件驱动 + 节流，人眼感知不到延迟
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        let mut last_full_broadcast = std::time::Instant::now();
        // P3-20 节点基本信息缓存：避免每次广播都查数据库
        // 缓存 1 秒，节点基本信息（hostname/platform/version/status/last_seen）变化不频繁
        let mut nodes_cache: Option<(std::time::Instant, Vec<NodeInfo>)> = None;
        let cache_ttl = std::time::Duration::from_secs(1);
        loop {
            interval.tick().await;
            let conn_count = state.frontend_ws_mgr.conn_count().await;
            if conn_count == 0 {
                continue;
            }

            // 事件驱动：有脏数据才广播；但最多每秒兜底广播一次，确保状态同步
            let dirty = state.frontend_ws_mgr.check_and_clear_dirty();
            let need_full_broadcast =
                last_full_broadcast.elapsed() >= std::time::Duration::from_secs(1);
            if !dirty && !need_full_broadcast {
                continue;
            }
            if need_full_broadcast {
                last_full_broadcast = std::time::Instant::now();
            }

            // P3-20 从缓存或数据库获取节点基本信息
            let nodes = if let Some((cache_time, cached)) = &nodes_cache {
                if cache_time.elapsed() < cache_ttl {
                    cached.clone()
                } else {
                    // 缓存过期，查数据库
                    match query_nodes_from_db(&state).await {
                        Ok(n) => {
                            nodes_cache = Some((std::time::Instant::now(), n.clone()));
                            n
                        }
                        Err(e) => {
                            tracing::warn!("[frontend-ws] 查询节点失败: {}", e);
                            continue;
                        }
                    }
                }
            } else {
                // 无缓存，查数据库
                match query_nodes_from_db(&state).await {
                    Ok(n) => {
                        nodes_cache = Some((std::time::Instant::now(), n.clone()));
                        n
                    }
                    Err(e) => {
                        tracing::warn!("[frontend-ws] 查询节点失败: {}", e);
                        continue;
                    }
                }
            };

            // 获取所有节点实时状态
            let realtime_map = state.ws_mgr.get_all_realtime().await;

            // 组装前端节点状态
            let frontend_nodes: Vec<FrontendNodeState> = nodes
                .into_iter()
                .map(
                    |(id, hostname, platform, _arch, version, status, last_seen)| {
                        let node_id = Uuid::parse_str(&id).unwrap_or(Uuid::nil());
                        let rt = realtime_map.get(&node_id).cloned().unwrap_or_default();
                        FrontendNodeState {
                            node_id,
                            hostname,
                            platform,
                            version,
                            status,
                            total_speed_bps: rt.total_speed_bps,
                            active_tasks: rt.active_tasks,
                            last_seen,
                        }
                    },
                )
                .collect();

            let msg = FrontendPushMsg::Realtime {
                nodes: frontend_nodes,
                timestamp: Utc::now().to_rfc3339(),
            };

            state.frontend_ws_mgr.broadcast(&msg).await;
        }
    });
}
