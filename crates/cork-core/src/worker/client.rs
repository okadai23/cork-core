use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::cork_worker_client::CorkWorkerClient;
use cork_proto::cork::v1::invoke_tool_stream_chunk::Chunk;
use cork_proto::cork::v1::run_event;
use cork_proto::cork::v1::{
    ArtifactRef as ProtoArtifactRef, InvokeToolRequest, InvokeToolResponse, InvokeToolStreamChunk,
    LogRecord, NodeStateChanged, NodeStatus, Payload, RunEvent, TraceContext,
};
use cork_store::{ArtifactRef, EventLog, NodeOutput, NodePayload, StateStore};
use prost_types::Timestamp;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetWindow {
    pub started_at: SystemTime,
    pub wall_ms: u64,
}

impl BudgetWindow {
    fn deadline(self) -> SystemTime {
        self.started_at
            .checked_add(Duration::from_millis(self.wall_ms))
            .unwrap_or(self.started_at)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InvocationBudget {
    pub run: Option<BudgetWindow>,
    pub stage: Option<BudgetWindow>,
    pub node: Option<BudgetWindow>,
}

impl InvocationBudget {
    fn earliest_deadline(&self) -> Option<SystemTime> {
        [self.run, self.stage, self.node]
            .into_iter()
            .flatten()
            .map(BudgetWindow::deadline)
            .min()
    }
}

#[derive(Clone)]
pub struct InvocationContext<'a> {
    pub run_id: &'a str,
    pub stage_id: &'a str,
    pub node_id: &'a str,
    pub event_log: &'a dyn EventLog,
    pub state_store: &'a dyn StateStore,
    pub budget: InvocationBudget,
    pub trace_context: Option<TraceContext>,
}

pub struct WorkerClient {
    client: CorkWorkerClient<Channel>,
}

impl WorkerClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            client: CorkWorkerClient::new(channel),
        }
    }

    pub fn from_client(client: CorkWorkerClient<Channel>) -> Self {
        Self { client }
    }

    pub async fn invoke_tool(
        &mut self,
        request: InvokeToolRequest,
        context: &InvocationContext<'_>,
    ) -> Result<InvokeToolResponse, Status> {
        let request = build_request(request, context);
        match self.client.invoke_tool(request).await {
            Ok(response) => {
                let response = response.into_inner();
                handle_success(context, &response);
                Ok(response)
            }
            Err(status) => {
                handle_failure(context, &status);
                Err(status)
            }
        }
    }

    pub async fn invoke_tool_stream(
        &mut self,
        request: InvokeToolRequest,
        context: &InvocationContext<'_>,
    ) -> Result<InvokeToolResponse, Status> {
        let request = build_request(request, context);
        let mut stream = match self.client.invoke_tool_stream(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                handle_failure(context, &status);
                return Err(status);
            }
        };

        let mut final_response = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    if let Some(action) = handle_chunk(context, &chunk) {
                        final_response = Some(action);
                    }
                }
                Err(status) => {
                    handle_failure(context, &status);
                    return Err(status);
                }
            }
        }

        let Some(final_response) = final_response else {
            let status = Status::data_loss("invoke tool stream ended without final response");
            handle_failure(context, &status);
            return Err(status);
        };

        handle_success(context, &final_response);
        Ok(final_response)
    }
}

fn build_request(
    mut request: InvokeToolRequest,
    context: &InvocationContext<'_>,
) -> Request<InvokeToolRequest> {
    if request.trace_context.is_none() {
        request.trace_context = context.trace_context.clone();
    }
    if let Some(deadline) = merge_deadline(request.deadline, context.budget.earliest_deadline()) {
        request.deadline = Some(system_time_to_timestamp(deadline));
    }
    let mut request = Request::new(request);
    if let Some(timeout) = timeout_from_budget(SystemTime::now(), &context.budget) {
        request.set_timeout(timeout);
    }
    request
}

fn merge_deadline(
    request_deadline: Option<Timestamp>,
    budget_deadline: Option<SystemTime>,
) -> Option<SystemTime> {
    let request_deadline = request_deadline.and_then(timestamp_to_system_time);
    match (request_deadline, budget_deadline) {
        (Some(request), Some(budget)) => Some(request.min(budget)),
        (Some(request), None) => Some(request),
        (None, Some(budget)) => Some(budget),
        (None, None) => None,
    }
}

fn handle_chunk(
    context: &InvocationContext<'_>,
    chunk: &InvokeToolStreamChunk,
) -> Option<InvokeToolResponse> {
    match &chunk.chunk {
        Some(Chunk::Log(log)) => {
            append_log(context.event_log, log.clone(), chunk.ts);
            None
        }
        Some(Chunk::Final(response)) => Some(response.clone()),
        Some(Chunk::Heartbeat(_)) | Some(Chunk::PartialOutput(_)) | None => None,
    }
}

fn handle_success(context: &InvocationContext<'_>, response: &InvokeToolResponse) {
    let output = node_output_from_response(response);
    context
        .state_store
        .set_node_output(context.run_id, context.node_id, output);
    append_node_state(
        context.event_log,
        context.stage_id,
        context.node_id,
        NodeStatus::NodeRunning,
        NodeStatus::NodeSucceeded,
        None,
    );
}

fn handle_failure(context: &InvocationContext<'_>, status: &Status) {
    append_node_state(
        context.event_log,
        context.stage_id,
        context.node_id,
        NodeStatus::NodeRunning,
        NodeStatus::NodeFailed,
        Some(format!("gRPC {:?}: {}", status.code(), status.message())),
    );
}

fn node_output_from_response(response: &InvokeToolResponse) -> NodeOutput {
    let payload = response.output.clone().unwrap_or_default();
    let artifacts = response.artifacts.iter().map(artifact_from_proto).collect();
    NodeOutput::from_payload(payload_from_proto(&payload), artifacts)
}

fn payload_from_proto(payload: &Payload) -> NodePayload {
    NodePayload {
        content_type: payload.content_type.clone(),
        data: payload.data.clone(),
    }
}

fn artifact_from_proto(artifact: &ProtoArtifactRef) -> ArtifactRef {
    let sha256_hex = artifact
        .sha256
        .as_ref()
        .map(|sha| bytes_to_hex(&sha.bytes32))
        .unwrap_or_default();
    ArtifactRef {
        uri: artifact.uri.clone(),
        sha256_hex,
        content_type: if artifact.content_type.is_empty() {
            None
        } else {
            Some(artifact.content_type.clone())
        },
        size_bytes: if artifact.size_bytes > 0 {
            Some(artifact.size_bytes as u64)
        } else {
            None
        },
    }
}

fn append_log(event_log: &dyn EventLog, log: LogRecord, ts: Option<Timestamp>) {
    let timestamp = ts.or(log.ts).unwrap_or_else(now_timestamp);
    let event = RunEvent {
        event_seq: 0,
        ts: Some(timestamp),
        event: Some(run_event::Event::Log(log)),
    };
    event_log.append(event);
}

fn append_node_state(
    event_log: &dyn EventLog,
    stage_id: &str,
    node_id: &str,
    old_status: NodeStatus,
    new_status: NodeStatus,
    reason: Option<String>,
) {
    let event = RunEvent {
        event_seq: 0,
        ts: Some(now_timestamp()),
        event: Some(run_event::Event::NodeState(NodeStateChanged {
            stage_id: stage_id.to_string(),
            node_id: node_id.to_string(),
            old_status: old_status as i32,
            new_status: new_status as i32,
            reason: reason.unwrap_or_default(),
        })),
    };
    event_log.append(event);
}

fn timeout_from_budget(now: SystemTime, budget: &InvocationBudget) -> Option<Duration> {
    let deadline = budget.earliest_deadline()?;
    Some(
        deadline
            .duration_since(now)
            .unwrap_or_else(|_| Duration::from_millis(0)),
    )
}

fn now_timestamp() -> Timestamp {
    system_time_to_timestamp(SystemTime::now())
}

fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn timestamp_to_system_time(timestamp: Timestamp) -> Option<SystemTime> {
    let seconds = u64::try_from(timestamp.seconds).ok()?;
    let nanos = u32::try_from(timestamp.nanos).ok()?;
    Some(UNIX_EPOCH + Duration::new(seconds, nanos))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cork_proto::cork::v1::cork_worker_server::{CorkWorker, CorkWorkerServer};
    use cork_proto::cork::v1::{InvokeToolRequest, InvokeToolResponse};
    use cork_store::{InMemoryEventLog, InMemoryStateStore};
    use std::pin::Pin;
    use tokio::net::TcpListener;
    use tokio_stream::Stream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Response, Status};

    struct TestWorker;

    #[tonic::async_trait]
    impl CorkWorker for TestWorker {
        async fn health(
            &self,
            _request: Request<cork_proto::cork::v1::HealthRequest>,
        ) -> Result<Response<cork_proto::cork::v1::HealthResponse>, Status> {
            Ok(Response::new(cork_proto::cork::v1::HealthResponse {
                status: "ok".to_string(),
                worker_id: "worker-1".to_string(),
            }))
        }

        async fn invoke_tool(
            &self,
            _request: Request<InvokeToolRequest>,
        ) -> Result<Response<InvokeToolResponse>, Status> {
            Ok(Response::new(InvokeToolResponse {
                output: Some(Payload {
                    content_type: "text/plain".to_string(),
                    data: b"ok".to_vec(),
                    encoding: "".to_string(),
                    sha256: None,
                }),
                artifacts: Vec::new(),
                error_code: "".to_string(),
                error_message: "".to_string(),
            }))
        }

        type InvokeToolStreamStream =
            Pin<Box<dyn Stream<Item = Result<InvokeToolStreamChunk, Status>> + Send>>;

        async fn invoke_tool_stream(
            &self,
            _request: Request<InvokeToolRequest>,
        ) -> Result<Response<Self::InvokeToolStreamStream>, Status> {
            let log = LogRecord {
                ts: Some(system_time_to_timestamp(SystemTime::now())),
                level: "INFO".to_string(),
                message: "stream-log".to_string(),
                trace_id_hex: "".to_string(),
                span_id_hex: "".to_string(),
                scope_id: "".to_string(),
                scope_seq: 0,
                attrs: Default::default(),
            };
            let chunks = vec![
                Ok(InvokeToolStreamChunk {
                    ts: log.ts,
                    chunk: Some(Chunk::Log(log)),
                }),
                Ok(InvokeToolStreamChunk {
                    ts: Some(system_time_to_timestamp(SystemTime::now())),
                    chunk: Some(Chunk::Final(InvokeToolResponse {
                        output: Some(Payload {
                            content_type: "application/json".to_string(),
                            data: br#"{"ok":true}"#.to_vec(),
                            encoding: "".to_string(),
                            sha256: None,
                        }),
                        artifacts: Vec::new(),
                        error_code: "".to_string(),
                        error_message: "".to_string(),
                    })),
                }),
            ];
            let stream = tokio_stream::iter(chunks);
            Ok(Response::new(Box::pin(stream)))
        }
    }

    async fn start_test_server() -> (Channel, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let incoming = TcpListenerStream::new(listener);
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CorkWorkerServer::new(TestWorker))
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("channel")
            .connect()
            .await
            .expect("connect");
        (channel, handle)
    }

    #[test]
    fn budget_uses_earliest_deadline() {
        let base = UNIX_EPOCH + Duration::from_secs(10);
        let budget = InvocationBudget {
            run: Some(BudgetWindow {
                started_at: base,
                wall_ms: 10_000,
            }),
            stage: Some(BudgetWindow {
                started_at: base,
                wall_ms: 2_000,
            }),
            node: Some(BudgetWindow {
                started_at: base,
                wall_ms: 5_000,
            }),
        };
        let deadline = budget.earliest_deadline().expect("deadline");
        assert_eq!(deadline, base + Duration::from_secs(2));
        let timeout = timeout_from_budget(base, &budget).expect("timeout");
        assert_eq!(timeout, Duration::from_secs(2));
    }

    #[test]
    fn merge_deadline_prefers_earlier_request_deadline() {
        let budget_deadline = UNIX_EPOCH + Duration::from_secs(20);
        let request_deadline = system_time_to_timestamp(UNIX_EPOCH + Duration::from_secs(5));

        let merged = merge_deadline(Some(request_deadline), Some(budget_deadline)).expect("merged");
        assert_eq!(merged, UNIX_EPOCH + Duration::from_secs(5));
    }

    #[test]
    fn merge_deadline_uses_budget_when_request_missing() {
        let budget_deadline = UNIX_EPOCH + Duration::from_secs(20);
        let merged = merge_deadline(None, Some(budget_deadline)).expect("merged");
        assert_eq!(merged, budget_deadline);
    }

    #[test]
    fn merge_deadline_uses_request_when_budget_missing() {
        let request_deadline = system_time_to_timestamp(UNIX_EPOCH + Duration::from_secs(5));
        let merged = merge_deadline(Some(request_deadline), None).expect("merged");
        assert_eq!(merged, UNIX_EPOCH + Duration::from_secs(5));
    }

    #[tokio::test]
    async fn invoke_tool_sets_output_and_event() {
        let (channel, handle) = start_test_server().await;
        let mut client = WorkerClient::new(channel);
        let event_log = InMemoryEventLog::new();
        let state_store = InMemoryStateStore::new();
        let context = InvocationContext {
            run_id: "run-1",
            stage_id: "stage-a",
            node_id: "stage-a/node-a",
            event_log: &event_log,
            state_store: &state_store,
            budget: InvocationBudget::default(),
            trace_context: None,
        };

        let request = InvokeToolRequest {
            invocation_id: "inv-1".to_string(),
            tool_name: "tool".to_string(),
            tool_version: "v1".to_string(),
            input: None,
            deadline: None,
            idempotency_key: "".to_string(),
            trace_context: None,
        };

        let response = client.invoke_tool(request, &context).await.expect("invoke");
        assert_eq!(response.output.unwrap().data, b"ok");

        let output = state_store
            .node_output("run-1", "stage-a/node-a")
            .expect("output");
        assert_eq!(output.payload.data, b"ok");

        let events = event_log.subscribe(0).backlog;
        assert!(events.iter().any(|event| matches!(
            event.event.as_ref(),
            Some(run_event::Event::NodeState(NodeStateChanged { new_status, .. }))
                if *new_status == NodeStatus::NodeSucceeded as i32
        )));

        handle.abort();
    }

    #[tokio::test]
    async fn invoke_tool_stream_appends_logs() {
        let (channel, handle) = start_test_server().await;
        let mut client = WorkerClient::new(channel);
        let event_log = InMemoryEventLog::new();
        let state_store = InMemoryStateStore::new();
        let context = InvocationContext {
            run_id: "run-2",
            stage_id: "stage-b",
            node_id: "stage-b/node-b",
            event_log: &event_log,
            state_store: &state_store,
            budget: InvocationBudget::default(),
            trace_context: None,
        };

        let request = InvokeToolRequest {
            invocation_id: "inv-2".to_string(),
            tool_name: "tool".to_string(),
            tool_version: "v1".to_string(),
            input: None,
            deadline: None,
            idempotency_key: "".to_string(),
            trace_context: None,
        };

        let response = client
            .invoke_tool_stream(request, &context)
            .await
            .expect("invoke stream");
        assert!(response.output.is_some());

        let events = event_log.subscribe(0).backlog;
        assert!(events.iter().any(|event| matches!(
            event.event.as_ref(),
            Some(run_event::Event::Log(LogRecord { message, .. }))
                if message == "stream-log"
        )));

        handle.abort();
    }
}
