//! CORK Daemon (corkd) - The main CORK gRPC server binary.
//!
//! This binary starts the CORK Core gRPC server and listens for incoming requests.

use std::time::Duration;

use cork_core::api::{CorkCoreService, DEFAULT_GRPC_MAX_MESSAGE_BYTES};
use cork_core::engine::watchdog::WatchdogSupervisor;
use cork_proto::cork::v1::cork_core_server::CorkCoreServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = CorkCoreService::new();
    let watchdog_service = service.clone();

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
        .serve(addr)
        .await?;

    Ok(())
}
