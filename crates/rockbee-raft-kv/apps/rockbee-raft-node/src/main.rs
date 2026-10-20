//! Read this file first: parse settings, start a node, optionally bootstrap, close.
use std::{process::ExitCode, time::Duration};

use rockbee_raft_node::cluster::{NodeError, NodeRuntime};
use tokio::time::Instant;

mod cli;

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log::error!("node failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    #[cfg(feature = "diagnostics")]
    let _diagnostics = rockbee_raft_node::diagnostics::Session::from_environment();
    let Some(arguments) = cli::parse(std::env::args().skip(1))? else {
        println!("{}", cli::USAGE);
        return Ok(());
    };
    // Install both signal handlers before creating listeners or Raft tasks.
    let mut shutdown = ShutdownSignals::install().map_err(|error| error.to_string())?;
    let (node_id, mut node) = match arguments.config {
        cli::Startup::Bootstrap(config) => {
            let id = config.node_id;
            (
                id,
                NodeRuntime::start(config)
                    .await
                    .map_err(|error| error.to_string())?,
            )
        }
        cli::Startup::Join(config) => {
            let id = config.local_endpoint.node_id;
            (
                id,
                NodeRuntime::start_join(config)
                    .await
                    .map_err(|error| error.to_string())?,
            )
        }
    };
    let result = serve(&node, arguments.initialize, &mut shutdown).await;
    // Even bootstrap/signal errors take the same owned shutdown path.
    node.close().await;
    log::info!("node stopped node={}", node_id.get());
    result
}

async fn serve(
    node: &NodeRuntime,
    initialize: bool,
    shutdown: &mut ShutdownSignals,
) -> Result<(), String> {
    let initialized = tokio::select! {
        result = initialize_cluster(node, initialize) => {
            result.map_err(|error| error.to_string())?;
            true
        },
        result = shutdown.wait() => {
            result.map_err(|error| error.to_string())?;
            false
        },
    };
    if initialized {
        log::info!("node listeners ready; requests still require Raft authority");
        shutdown.wait().await.map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn initialize_cluster(node: &NodeRuntime, initialize: bool) -> Result<(), NodeError> {
    if initialize {
        node.initialize_groups().await?;
        node.bootstrap_directory(Instant::now() + Duration::from_secs(30))
            .await?;
    }
    Ok(())
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn install() -> Result<Self, std::io::Error> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) -> Result<(), std::io::Error> {
        tokio::select! {
            _ = self.interrupt.recv() => Ok(()),
            _ = self.terminate.recv() => Ok(()),
        }
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn install() -> Result<Self, std::io::Error> {
        Ok(Self)
    }
    async fn wait(&mut self) -> Result<(), std::io::Error> {
        tokio::signal::ctrl_c().await
    }
}
