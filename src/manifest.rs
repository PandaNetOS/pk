//! PK 自描述能力清单（Capability Manifest）
//! 遵循 PandaNetOS 标准：每个构建版本生成自己的说明书，其他程序一看就知道怎么调用

use serde_json::{json, Value};

/// 生成 PK 的完整能力清单（说明书）
pub fn build_capability_manifest() -> Value {
    let build_timestamp = option_env!("PK_BUILD_TIMESTAMP").unwrap_or("0");
    let git_commit = option_env!("PK_GIT_COMMIT").unwrap_or("unknown");
    let rust_version = option_env!("PK_RUST_VERSION").unwrap_or("unknown");
    let target_triple = option_env!("PK_TARGET_TRIPLE").unwrap_or("unknown");

    json!({
        "manifest_version": "1.0",
        "basic": {
            "name": "pk",
            "full_name": "PandaNetPL Keeper",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "PK — SPDE 主控平面，负责生成、下发并控制 SPDE 下载节点",
            "role": "control_plane",
            "license": env!("CARGO_PKG_LICENSE"),
        },
        "capabilities": {
            "node_management": [
                "节点注册（首次自动通过，被删后重新注册待审批）",
                "节点删除（主动通知 spde 取消任务并重新注册）",
                "节点审批（同意/拒绝 pending 节点）",
                "节点能力参数配置（最大并发、带宽上限）",
                "清理离线节点",
                "节点状态管理（online/busy/offline/pending）",
                "internal/external 节点分类管理",
            ],
            "task_management": [
                "任务创建（URL/文件名/优先级/目标节点）",
                "任务编辑",
                "任务删除（可选删除已下载文件）",
                "任务取消",
                "任务重试",
                "任务状态跟踪",
            ],
            "scheduling": [
                "手动下发任务到指定节点",
                "自动调度（按节点负载/带宽/能力分配）",
                "工作流调度（定时/周期/手动触发）",
                "任务优先级队列",
            ],
            "realtime": [
                "WebSocket 实时数据推送（节点状态/任务进度/速度）",
                "前端实时刷新（50ms 级别）",
                "节点心跳接收",
                "节点删除主动通知（WebSocket 事件 node.deleted）",
            ],
            "workflow": [
                "工作流创建/编辑/删除",
                "定时触发（cron 表达式）",
                "周期触发（每日/每周）",
                "手动触发",
                "工作流启用/禁用",
            ],
            "config_management": [
                "spde 默认配置管理",
                "节点级配置覆盖",
                "配置拉取 API（spde 启动时拉取）",
            ],
            "api": [
                "RESTful API（JSON）",
                "API Token 鉴权",
                "CORS 支持",
            ],
        },
        "configurable_params": [
            {
                "name": "listen",
                "type": "string",
                "default": "0.0.0.0:5566",
                "description": "HTTP API 监听地址",
            },
            {
                "name": "token",
                "type": "string",
                "default": "",
                "description": "API/Agent 鉴权 Token，空则不鉴权",
            },
            {
                "name": "spde_defaults.max_concurrent",
                "type": "integer",
                "default": 4,
                "min": 1,
                "max": 64,
                "description": "spde 默认最大并发任务数",
            },
            {
                "name": "spde_defaults.connections_per_file",
                "type": "integer",
                "default": 8,
                "min": 1,
                "max": 64,
                "description": "spde 默认单文件连接数",
            },
            {
                "name": "spde_defaults.retry_times",
                "type": "integer",
                "default": 3,
                "min": 0,
                "max": 20,
                "description": "spde 默认重试次数",
            },
            {
                "name": "spde_defaults.timeout",
                "type": "integer",
                "default": 1800,
                "min": 10,
                "unit": "seconds",
                "description": "spde 默认任务超时时间",
            },
            {
                "name": "heartbeat_timeout_secs",
                "type": "integer",
                "default": 30,
                "min": 5,
                "unit": "seconds",
                "description": "节点心跳超时时间，超时标记为 offline",
            },
        ],
        "api_interfaces": {
            "base_path": "/api/v1",
            "endpoints": [
                {"method": "GET", "path": "/overview", "description": "总览数据"},
                {"method": "GET", "path": "/nodes", "description": "节点列表"},
                {"method": "DELETE", "path": "/nodes", "description": "清理所有离线节点"},
                {"method": "POST", "path": "/agent/register", "description": "spde 节点注册"},
                {"method": "POST", "path": "/agent/heartbeat", "description": "spde 节点心跳上报"},
                {"method": "GET", "path": "/nodes/{id}/config.yaml", "description": "获取节点配置"},
                {"method": "POST", "path": "/nodes/{id}/approve", "description": "同意 pending 节点"},
                {"method": "POST", "path": "/nodes/{id}/reject", "description": "拒绝 pending 节点"},
                {"method": "DELETE", "path": "/nodes/{id}", "description": "删除节点"},
                {"method": "GET", "path": "/tasks", "description": "任务列表"},
                {"method": "POST", "path": "/tasks", "description": "创建任务"},
                {"method": "DELETE", "path": "/tasks/{id}", "description": "删除任务"},
                {"method": "POST", "path": "/tasks/{id}/cancel", "description": "取消任务"},
                {"method": "GET", "path": "/dispatches", "description": "分发列表"},
                {"method": "GET", "path": "/workflows", "description": "工作流列表"},
                {"method": "POST", "path": "/workflows", "description": "创建工作流"},
                {"method": "GET", "path": "/realtime/ws", "description": "WebSocket 实时推送"},
            ],
        },
        "communication": {
            "protocols": ["HTTP/1.1", "WebSocket"],
            "data_format": "JSON",
            "auth": "Bearer Token (可选)",
            "agent_heartbeat_interval": "5s (spde 端默认)",
            "realtime_push": "WebSocket，节点状态/任务进度/速度实时推送",
            "node_deleted_notification": "WebSocket 事件 node.deleted（WsMessage 信封），删除节点时主动通知 spde",
        },
        "node_status_fields": [
            "id (UUID)",
            "hostname",
            "platform",
            "arch",
            "version",
            "status (online/busy/offline/pending)",
            "last_seen",
            "registered_at",
            "labels",
            "active_tasks",
            "max_concurrent",
            "max_bandwidth_bps",
            "bytes_downloaded",
            "capabilities (spde 上报的能力清单)",
        ],
        "status_report": {
            "node_level": [
                "active_tasks",
                "bytes_downloaded",
                "total_speed_bps",
                "last_seen",
                "version",
            ],
            "task_level": [
                "dispatch_id",
                "task_name",
                "percent",
                "speed_bps",
                "downloaded_bytes",
                "total_bytes",
                "active_connections",
                "status",
            ],
        },
        "build_info": {
            "rust_version": rust_version,
            "build_timestamp": build_timestamp,
            "git_commit": git_commit,
            "target_triple": target_triple,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        },
    })
}

/// 输出说明书到 stdout（--manifest 命令使用）
pub fn print_manifest() {
    let manifest = build_capability_manifest();
    println!("{}", serde_json::to_string_pretty(&manifest).unwrap()); // panda-allow: cli-output
}
