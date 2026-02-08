use clap::Parser;
use cork_core::api::{CorkCoreService, DEFAULT_GRPC_MAX_MESSAGE_BYTES};
use cork_core::cli::{
    CanonicalKind, Cli, Command, apply_patch_request, canonicalize_json, get_run_request,
    payload_from_file, stream_events_request, submit_run_request,
};
use cork_proto::cork::v1::cork_core_client::CorkCoreClient;
use cork_proto::cork::v1::cork_core_server::CorkCoreServer;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve { addr } => {
            let addr = addr.parse()?;
            let service = CorkCoreService::new();
            println!("corkd starting on {}", addr);
            Server::builder()
                .add_service(
                    CorkCoreServer::new(service)
                        .max_decoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES),
                )
                .serve(addr)
                .await?;
        }
        Command::SubmitRun {
            addr,
            contract,
            policy,
            input,
            input_content_type,
            experiment_id,
            variant_id,
        } => {
            let contract_doc = canonicalize_json(&contract, CanonicalKind::Contract)?;
            let policy_doc = canonicalize_json(&policy, CanonicalKind::Policy)?;
            let payload = match input {
                Some(path) => Some(payload_from_file(&path, &input_content_type)?),
                None => None,
            };
            let request =
                submit_run_request(contract_doc, policy_doc, payload, experiment_id, variant_id);
            let mut client = CorkCoreClient::connect(addr).await?;
            let response = client.submit_run(Request::new(request)).await?.into_inner();
            println!(
                "run_id={}",
                response.handle.map(|h| h.run_id).unwrap_or_default()
            );
        }
        Command::ApplyPatch {
            addr,
            run_id,
            patch,
            actor_id,
        } => {
            let patch_doc = canonicalize_json(&patch, CanonicalKind::Patch)?;
            let request = apply_patch_request(run_id, patch_doc, actor_id);
            let mut client = CorkCoreClient::connect(addr).await?;
            let response = client
                .apply_graph_patch(Request::new(request))
                .await?
                .into_inner();
            if response.accepted {
                println!("patch accepted");
            } else {
                println!("patch rejected: {}", response.rejection_reason);
            }
        }
        Command::StreamEvents {
            addr,
            run_id,
            since_event_seq,
        } => {
            let request = stream_events_request(run_id, since_event_seq);
            let mut client = CorkCoreClient::connect(addr).await?;
            let mut stream = client
                .stream_run_events(Request::new(request))
                .await?
                .into_inner();
            while let Some(event) = stream.next().await {
                println!("{:?}", event?);
            }
        }
        Command::GetRun { addr, run_id } => {
            let request = get_run_request(run_id);
            let mut client = CorkCoreClient::connect(addr).await?;
            let response = client.get_run(Request::new(request)).await?.into_inner();
            println!("{:?}", response);
        }
    }
    Ok(())
}
