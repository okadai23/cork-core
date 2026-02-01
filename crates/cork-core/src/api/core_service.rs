//! CorkCore gRPC service implementation.
//!
//! This module contains the implementation of the CorkCore gRPC service
//! as defined in `proto/cork/v1/core.proto`.

use cork_proto::cork::v1::{
    ApplyGraphPatchRequest, ApplyGraphPatchResponse, CancelRunRequest, CancelRunResponse,
    GetCompositeGraphRequest, GetCompositeGraphResponse, GetLogsRequest, GetLogsResponse,
    GetRunRequest, GetRunResponse, ListRunsRequest, ListRunsResponse, RunEvent,
    StreamRunEventsRequest, SubmitRunRequest, SubmitRunResponse, cork_core_server::CorkCore,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// The CorkCore service implementation.
#[derive(Debug, Default)]
pub struct CorkCoreService {}

impl CorkCoreService {
    /// Creates a new CorkCoreService instance.
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl CorkCore for CorkCoreService {
    async fn submit_run(
        &self,
        _request: Request<SubmitRunRequest>,
    ) -> Result<Response<SubmitRunResponse>, Status> {
        Err(Status::unimplemented("SubmitRun not yet implemented"))
    }

    async fn cancel_run(
        &self,
        _request: Request<CancelRunRequest>,
    ) -> Result<Response<CancelRunResponse>, Status> {
        Err(Status::unimplemented("CancelRun not yet implemented"))
    }

    async fn get_run(
        &self,
        _request: Request<GetRunRequest>,
    ) -> Result<Response<GetRunResponse>, Status> {
        Err(Status::unimplemented("GetRun not yet implemented"))
    }

    async fn list_runs(
        &self,
        _request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        Err(Status::unimplemented("ListRuns not yet implemented"))
    }

    type StreamRunEventsStream = ReceiverStream<Result<RunEvent, Status>>;

    async fn stream_run_events(
        &self,
        _request: Request<StreamRunEventsRequest>,
    ) -> Result<Response<Self::StreamRunEventsStream>, Status> {
        Err(Status::unimplemented("StreamRunEvents not yet implemented"))
    }

    async fn apply_graph_patch(
        &self,
        _request: Request<ApplyGraphPatchRequest>,
    ) -> Result<Response<ApplyGraphPatchResponse>, Status> {
        Err(Status::unimplemented("ApplyGraphPatch not yet implemented"))
    }

    async fn get_composite_graph(
        &self,
        _request: Request<GetCompositeGraphRequest>,
    ) -> Result<Response<GetCompositeGraphResponse>, Status> {
        Err(Status::unimplemented(
            "GetCompositeGraph not yet implemented",
        ))
    }

    async fn get_logs(
        &self,
        _request: Request<GetLogsRequest>,
    ) -> Result<Response<GetLogsResponse>, Status> {
        Err(Status::unimplemented("GetLogs not yet implemented"))
    }
}
