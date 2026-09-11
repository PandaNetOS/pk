use anyhow::Result;
use clap::{Parser, Subcommand};
use pk::{api, comm, manifest, web, workflow_scheduler, ws, AppState, PkConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "pk", version, about = "PK — SPDE 主控")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 输出自描述能力清单（说明书）
    Manifest,
    /// 启动主控 HTTP + Web UI
    Serve {
        /// 配置文件路径（默认：可执行文件同级 pk-controlcenter/config.yaml）
        #[arg(long)]
        config: Option<PathBuf>,
        /// 覆盖监听地址
        #[arg(long)]
        listen: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Commands::Manifest => {
            manifest::print_manifest();
            Ok(())
        }
        Commands::Serve { config, listen } => run_serve(config, listen).await,
    }
}

async fn run_serve(config: Option<PathBuf>, listen: Option<String>) -> Result<()> {
    let exe = std::env::current_exe()?;
    let exe_dir = exe
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let work_root = exe_dir.join("pk-controlcenter");
    tokio::fs::create_dir_all(&work_root).await?;

    let cfg_path = config.unwrap_or_else(|| work_root.join("config.yaml"));
    let mut cfg = PkConfig::load_or_init(&cfg_path)?;
    if let Some(l) = listen {
        cfg.listen = l;
    }

    let state = AppState::open(cfg.clone(), work_root.clone()).await?;
    tracing::info!("work root {:?}", work_root);
    tracing::info!("data dir  {:?}", state.data_dir);

    // 接入 pnos-comm 通信 SDK：注册到 pnos-runtime（runtime 不可达时优雅降级）
    if let Some(app) = comm::init_comm(&cfg, state.clone()).await {
        *state.pk_app.write().await = Some(app);
    }

    // 启动工作流后台调度器
    workflow_scheduler::start(state.clone()).await;
    // 启动前端 WebSocket 实时状态广播（每秒推送一次）
    ws::spawn_realtime_broadcaster(state.clone());

    let app = api::router(state.clone());
    let app = web::mount(app);

    let addr: SocketAddr = cfg.listen.parse()?;
    tracing::info!("PK listening on http://{addr}");
    tracing::info!("Web UI: http://{addr}/");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown(state.clone()))
    .await?;
    Ok(())
}

async fn shutdown(state: Arc<AppState>) {
    // 先等待终止信号；收到后才注销 pk 并关闭与 pnos-runtime 的连接。
    // 注意：axum 的 with_graceful_shutdown 会在启动时就轮询该 future，
    // 若把 app.shutdown() 放在前面，会导致 pk 一注册就立刻注销。
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal");
    if let Some(app) = state.pk_app.write().await.take() {
        app.shutdown().await;
    }
}
