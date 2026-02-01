//! CORK Daemon (corkd) - The main CORK gRPC server binary.
//!
//! This binary starts the CORK Core gRPC server and listens for incoming requests.

use cork_core::api::CorkCoreService;
use cork_proto::cork::v1::cork_core_server::CorkCoreServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = CorkCoreService::new();

    println!("corkd starting on {}", addr);

    Server::builder()
        .add_service(CorkCoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
