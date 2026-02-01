//! CorkCore gRPC service implementation.
//!
//! This module contains the implementation of the CorkCore gRPC service
//! as defined in `proto/cork/v1/core.proto`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cork_proto::cork::v1::{
    ApplyGraphPatchRequest, ApplyGraphPatchResponse, CancelRunRequest, CancelRunResponse,
    GetCompositeGraphRequest, GetCompositeGraphResponse, GetLogsRequest, GetLogsResponse,
    GetRunRequest, GetRunResponse, ListRunsRequest, ListRunsResponse, RunEvent,
    StreamRunEventsRequest, SubmitRunRequest, SubmitRunResponse, cork_core_server::CorkCore,
};
use cork_store::{EventLog, InMemoryEventLog};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::{Request, Response, Status};

/// The CorkCore service implementation.
#[derive(Debug, Default)]
pub struct CorkCoreService {
    event_logs: Arc<RwLock<HashMap<String, Arc<InMemoryEventLog>>>>,
}

impl CorkCoreService {
    /// Creates a new CorkCoreService instance.
    pub fn new() -> Self {
        Self::default()
    }

    fn event_log_for_run(&self, run_id: &str) -> Arc<InMemoryEventLog> {
        let mut logs = self
            .event_logs
            .write()
            .expect("event log store lock poisoned");
        logs.entry(run_id.to_string())
            .or_insert_with(|| Arc::new(InMemoryEventLog::new()))
            .clone()
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
        request: Request<StreamRunEventsRequest>,
    ) -> Result<Response<Self::StreamRunEventsStream>, Status> {
        let request = request.into_inner();
        let handle = request
            .handle
            .ok_or_else(|| Status::invalid_argument("missing run handle"))?;
        if handle.run_id.is_empty() {
            return Err(Status::invalid_argument("missing run_id"));
        }
        if request.since_event_seq < 0 {
            return Err(Status::invalid_argument(
                "since_event_seq must be non-negative",
            ));
        }
        let since_seq = request.since_event_seq as u64;
        let log = self.event_log_for_run(&handle.run_id);
        let subscription = log.subscribe(since_seq);

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            for event in subscription.backlog {
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
            }

            let mut live = BroadcastStream::new(subscription.receiver);
            while let Some(item) = live.next().await {
                match item {
                    Ok(event) => {
                        if tx.send(Ok(event)).await.is_err() {
                            break;
                        }
                    }
                    Err(BroadcastStreamRecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::unavailable("event stream lagged")))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use cork_proto::cork::v1::{RunHandle, StreamRunEventsRequest};

    #[tokio::test]
    async fn stream_run_events_yields_backlog_and_live_updates() {
        let service = CorkCoreService::new();
        let run_id = "run-1";
        let log = service.event_log_for_run(run_id);
        log.append(RunEvent::default());

        let request = StreamRunEventsRequest {
            handle: Some(RunHandle {
                run_id: run_id.to_string(),
            }),
            since_event_seq: 0,
        };
        let response = service
            .stream_run_events(Request::new(request))
            .await
            .expect("stream response");
        let mut stream = response.into_inner();

        let first = stream
            .next()
            .await
            .expect("stream item")
            .expect("stream event");
        assert_eq!(first.event_seq, 0);

        log.append(RunEvent::default());
        let second = stream
            .next()
            .await
            .expect("stream item")
            .expect("stream event");
        assert_eq!(second.event_seq, 1);
    }
}
