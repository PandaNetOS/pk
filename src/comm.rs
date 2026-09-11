//! pnos-comm 通信 SDK 接入
//!
//! 将 pk 注册到 pnos-runtime（`ComponentType::Pk`），自动心跳，
//! 并订阅组件事件（`component.*` / `app.*`）同步进 `ServiceRegistry`，
//! 使 pk 能通过 runtime 服务发现 + 自动 Token 调用其他组件（如 pdc）。
//!
//! runtime 不可达时优雅降级：注册超时仅告警，pk 仍以独立模式运行。

use std::sync::Arc;
use std::time::Duration;

use pnos::component::ComponentType;
use pnos::events::WsMessage;
use pnos_comm::PnosApp;
use uuid::Uuid;

use crate::config::PkConfig;
use crate::store::AppState;

/// 命名空间：把字符串组件 id 派生为稳定 Uuid（ServiceRegistry 以 Uuid 为键）
const NS: Uuid = Uuid::NAMESPACE_DNS;

/// 初始化通信 SDK（best-effort：runtime 不可达则返回 None）
pub async fn init_comm(cfg: &PkConfig, state: Arc<AppState>) -> Option<PnosApp> {
    let port = cfg
        .listen
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(5566);
    let runtime_url = cfg
        .runtime_url
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let timeout = Duration::from_secs(cfg.register_timeout_secs.max(1));

    let mut builder = PnosApp::builder("pk")
        .version(env!("CARGO_PKG_VERSION"))
        .component_type(ComponentType::Pk)
        .port(port)
        .runtime_url(&runtime_url);
    if let Some(region) = &cfg.region {
        builder = builder.region(region.clone());
    }

    // 订阅组件/应用事件 → 同步进 ServiceRegistry
    let st = state.clone();
    let builder = builder.on_event("component.registered", move |msg: WsMessage| {
        let st = st.clone();
        async move {
            apply_event(st, &msg).await;
        }
    });
    let st = state.clone();
    let builder = builder.on_event("component.status_changed", move |msg: WsMessage| {
        let st = st.clone();
        async move {
            apply_event(st, &msg).await;
        }
    });
    let st = state.clone();
    let builder = builder.on_event("app.status_changed", move |msg: WsMessage| {
        let st = st.clone();
        async move {
            apply_event(st, &msg).await;
        }
    });
    let st = state.clone();
    let builder = builder.on_event("component.unregistered", move |msg: WsMessage| {
        let st = st.clone();
        async move {
            apply_event(st, &msg).await;
        }
    });
    let st = state.clone();
    let builder = builder.on_event("component.offline", move |msg: WsMessage| {
        let st = st.clone();
        async move {
            apply_event(st, &msg).await;
        }
    });

    match tokio::time::timeout(timeout, builder.init()).await {
        Ok(Ok(app)) => {
            tracing::info!("已通过 pnos-comm 接入 pnos-runtime（{}）", runtime_url);
            Some(app)
        }
        Ok(Err(e)) => {
            tracing::warn!("pnos-comm 注册 pnos-runtime 失败，pk 以独立模式运行: {}", e);
            None
        }
        Err(_) => {
            tracing::warn!(
                "pnos-comm 注册 pnos-runtime 超时（{:?}），pk 以独立模式运行",
                timeout
            );
            None
        }
    }
}

/// 将 runtime 组件事件同步进 ServiceRegistry
async fn apply_event(state: Arc<AppState>, msg: &WsMessage) {
    let p = &msg.payload;
    let id = p
        .get("id")
        .or_else(|| p.get("component_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if id.is_empty() {
        return;
    }
    let agent_id = Uuid::new_v5(&NS, id.as_bytes());
    let et = msg.event_type.as_str();

    match et {
        "component.unregistered" => {
            state.service_registry.unregister(agent_id).await;
            return;
        }
        "component.offline" => {
            state
                .service_registry
                .update_health(agent_id, "unhealthy", 0.0)
                .await;
            return;
        }
        _ => {}
    }

    // registered / status_changed / app.status_changed → upsert
    let info = crate::models::ServiceAgentInfo {
        agent_id,
        name: p
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(id)
            .to_string(),
        agent_type: p
            .get("component_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string(),
        host: p
            .get("serve_host")
            .or_else(|| p.get("host"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        port: p
            .get("serve_port")
            .or_else(|| p.get("port"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u16,
        capabilities: p
            .get("capabilities")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default(),
        health: "healthy".to_string(),
        load: 0.0,
        region: p
            .get("region")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        version: p
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
    };
    state.service_registry.register(info).await;
}
