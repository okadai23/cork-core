//! CORK Daemon (corkd) - The main CORK gRPC server binary.
//!
//! This binary starts the CORK Core gRPC server and listens for incoming requests.

use std::time::Duration;

use cork_core::api::{CorkCoreService, CorkCoreServiceConfig, DEFAULT_GRPC_MAX_MESSAGE_BYTES};
use cork_core::engine::watchdog::WatchdogSupervisor;
use cork_core::shutdown::{ShutdownMode, ShutdownPolicy};
use cork_proto::cork::v1::cork_core_server::CorkCoreServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let shutdown_policy = load_shutdown_policy();
    let service = CorkCoreService::with_config(CorkCoreServiceConfig {
        shutdown_policy,
        ..CorkCoreServiceConfig::default()
    });
    let watchdog_service = service.clone();
    let shutdown_service = service.clone();

    tokio::spawn(async move {
        let mut supervisor = WatchdogSupervisor::new(Duration::from_secs(5));
        loop {
            supervisor.tick().await;
            watchdog_service.tick_watchdog().await;
        }
    });

    println!("corkd starting on {}", addr);

    Server::builder()
        .add_service(
            CorkCoreServer::new(service).max_decoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES),
        )
        .serve_with_shutdown(addr, async move {
            wait_for_shutdown_signal().await;
            shutdown_service.handle_shutdown().await;
        })
        .await?;

    Ok(())
}

fn load_shutdown_policy() -> ShutdownPolicy {
    let mode = std::env::var("CORK_SHUTDOWN_MODE")
        .unwrap_or_else(|_| "drain".to_string())
        .to_lowercase();
    let mode = match mode.as_str() {
        "cancel" => ShutdownMode::Cancel,
        _ => ShutdownMode::Drain,
    };
    let drain_timeout_secs = std::env::var("CORK_SHUTDOWN_DRAIN_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    ShutdownPolicy {
        mode,
        drain_timeout: Duration::from_secs(drain_timeout_secs),
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut term = signal(SignalKind::terminate()).expect("signal handler");
        let mut interrupt = signal(SignalKind::interrupt()).expect("signal handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
            _ = interrupt.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
