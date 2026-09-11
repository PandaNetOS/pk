//! API 认证中间件（语义对齐 pnos-comm `auth_middleware`）
//!
//! - 校验 `X-Pnos-Token` 请求头，token 取自 `cfg.token`
//! - `cfg.token` 为空 → 开发模式，放行所有请求（与 SDK 行为一致）
//! - 白名单路径放行：健康检查、Web UI 静态资源、节点面端点
//!   （agent/*、realtime/ws、dispatches/claim、components register/heartbeat、artifacts）

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use serde_json::json;

use crate::store::AppState;

/// 免认证路径前缀（节点面 + 健康检查 + 静态资源）
const WHITELIST_PREFIXES: &[&str] = &[
    "/health",
    // Web UI 静态资源（web.rs）
    "/assets",
    "/favicon",
    "/torrents",
    // 节点面：config / report / ws（spde 直连，不走管理面认证）
    "/api/v1/agent/",
    "/api/v1/realtime/ws",
    "/api/v1/dispatches/claim",
    // 组件注册/心跳（runtime 或节点调用）
    "/api/v1/components/register",
    "/api/v1/components/heartbeat",
    // 节点二进制分发
    "/api/v1/artifacts",
];

fn is_whitelisted(path: &str) -> bool {
    if path == "/" || WHITELIST_PREFIXES.iter().any(|p| path.starts_with(p)) {
        return true;
    }
    // 节点拉取配置：GET /api/v1/nodes/{id}/config.yaml（spde 直连，节点面）
    path.starts_with("/api/v1/nodes/") && path.ends_with("/config.yaml")
}

/// 认证中间件：白名单放行 → token 为空放行（开发模式）→ 校验 X-Pnos-Token
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();

    if is_whitelisted(&path) {
        return next.run(req).await;
    }

    let expected = state.cfg.token.trim().to_string();
    if expected.is_empty() {
        // 开发模式：未配置 token，放行
        return next.run(req).await;
    }

    let provided = req
        .headers()
        .get("X-Pnos-Token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match provided {
        Some(t) if t == expected => next.run(req).await,
        Some(_) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "code": pnos::error::ErrorCode::TokenInvalid.code(),
                "message": "无效的认证 Token",
                "data": null,
            })),
        )
            .into_response(),
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "code": pnos::error::ErrorCode::Unauthorized.code(),
                "message": "缺少 X-Pnos-Token 请求头",
                "data": null,
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_whitelist() {
        assert!(is_whitelisted("/"));
        assert!(is_whitelisted("/health"));
        assert!(is_whitelisted("/health/ready"));
        assert!(is_whitelisted("/assets/app.js"));
        assert!(is_whitelisted("/favicon.png"));
        assert!(is_whitelisted("/torrents"));
        assert!(is_whitelisted("/api/v1/agent/config"));
        assert!(is_whitelisted("/api/v1/agent/report"));
        assert!(is_whitelisted("/api/v1/agent/ws"));
        assert!(is_whitelisted("/api/v1/realtime/ws"));
        assert!(is_whitelisted("/api/v1/dispatches/claim"));
        assert!(is_whitelisted("/api/v1/components/register"));
        assert!(is_whitelisted("/api/v1/components/heartbeat"));
        assert!(is_whitelisted("/api/v1/artifacts/linux"));

        // 管理面不在白名单内
        assert!(!is_whitelisted("/api/v1/nodes"));
        assert!(!is_whitelisted("/api/v1/tasks"));
        // 节点拉配置属节点面
        assert!(is_whitelisted("/api/v1/nodes/x/config.yaml"));
        assert!(!is_whitelisted("/api/v1/nodes/x"));
    }
}
