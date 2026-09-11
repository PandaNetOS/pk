# PK

PandaNetOS 生态主控：生成、下发并控制 [SPDE](https://github.com/pandamelive/spde) 节点。Rust 实现，内置 Web UI。

## 功能特性

- **多节点调度**：支持多个 spde 下载节点统一管理和任务分发
- **智能负载均衡**：根据节点负载和带宽自动分配下载任务
- **实时进度监控**：WebSocket 实时推送下载进度、速度、连接数
- **任务队列管理**：支持任务优先级、暂停、恢复、取消
- **HTTP API**：完整的 RESTful API 供第三方系统集成
- **节点审批机制**：新节点接入需要管理员审批
- **配置热更新**：全局配置和节点配置支持运行时修改
- **PandaNetOS 标准库**：基于 PandaNetOS 标准库构建，统一协议和规范

## 生态标准

本项目属于 **PandaNetOS 生态项目群**，遵循全系统权威标准仓库 [PandaNetOS](https://github.com/PandaNetOS/PandaNetOS) 的规范。

### 标准库路径约定

**生态唯一标准库**：PandaNetOS 生态只允许使用 `pnos`（[pnos-spec](https://github.com/PandaNetOS/pnos-spec)）标准库——**有且只有它**。旧的 `pandanetos` 已废弃，任何项目不得再依赖。

`Cargo.toml` 中的依赖声明（git 依赖，无需同级目录）：

```toml
[dependencies]
pnos = { git = "https://github.com/PandaNetOS/pnos-spec.git", branch = "main" }
```

> 无需把标准库克隆到同级目录：`pnos` 通过 git 依赖自动拉取。

### 规范要求

- **只允许 `pnos`**：统一协议路径常量（`pnos::protocol`）、响应格式（`pnos::response::ApiResponse`）、错误码（`pnos::error::ErrorCode`）、事件协议（`pnos::events::WsMessage`）与配置标准；**禁止**依赖已废弃的 `pandanetos`，**禁止**维护私有协议与常量。
- **标准一致性**：API 路径、响应格式、文件布局与文档规范均以 PandaNetOS《标准规范》为准。
- 一行导入所有常用类型：`use pnos::prelude::*;`

- 节点注册 / 心跳 / 在线状态
- 下载任务创建与调度（任一节点 / 全部节点 / 指定节点）
- 运行记录与流量统计
- 按平台下发 SPDE 二进制与生成 `config.yaml`
- 节点 `spde agent --master` 拉任务并回报告

## 快速开始

```bash
cargo build --release
./target/release/pk serve
```

浏览器打开 `http://127.0.0.1:5566`。

工作目录在二进制同级的 `pk-controlcenter/`：

```
pk-controlcenter/
├── config.yaml          # 主控配置
└── pk-data/
    ├── state.json       # 节点 / 任务 / 调度 / 运行记录
    └── artifacts/       # 放入各平台 spde 二进制后即可下发
```

## 节点接入

在已编译的 SPDE 上：

```bash
spde agent --master http://<pk-host>:5566
```

或把对应平台二进制放到 `pk-data/artifacts/` 后，在节点上执行：

```bash
curl -fsSL http://<pk-host>:5566/install.sh | sh
```

Windows：

```powershell
irm http://<pk-host>:5566/install.ps1 | iex
```

## 调度策略

| target | 行为 |
|---|---|
| `any` | 下发到当前负载最低的一个在线节点 |
| `all` | 每个在线节点各执行一份 |
| `nodes` | 仅下发到指定 `node_ids` |

节点通过心跳领取 pending dispatch，完成后 `POST /api/v1/agent/report`。

## HTTP API（节选）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/overview` | 汇总 |
| GET | `/api/v1/nodes` | 节点列表 |
| POST | `/api/v1/tasks` | 创建任务 |
| POST | `/api/v1/tasks/{id}/dispatch` | 立即调度 |
| GET | `/api/v1/runs` | 运行记录 |
| GET | `/api/v1/artifacts/{platform}` | 下载 SPDE 二进制 |
| GET | `/api/v1/nodes/{id}/config.yaml` | 生成该节点配置 |
| POST | `/api/v1/agent/register` | 节点注册 |
| POST | `/api/v1/agent/heartbeat` | 心跳 + 领任务 |
| POST | `/api/v1/agent/report` | 回写结果 |

`config.yaml` 里 `token` 非空时，上述 API 需要 `Authorization: Bearer <token>`。

## 配置

```yaml
listen: "0.0.0.0:5566"
heartbeat_timeout_secs: 45
token: ""
spde_defaults:
  max_concurrent: 4
  connections_per_file: 8
  save_path: "./download"
```

## 开发指南

### 环境要求

- Rust 1.75+
- PandaNetOS 标准库（同级目录）

### 构建

```bash
cargo build --release
```

### 测试

```bash
cargo test --all
```

### 合规检查

```bash
bash ../PandaNetOS/scripts/check_compliance.sh .
```

## 贡献指南

欢迎提交 Issue 和 Pull Request。提交前请确保通过合规检查。


## License

MIT
