use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::cork_worker_client::CorkWorkerClient;
use cork_proto::cork::v1::invoke_tool_stream_chunk::Chunk;
use cork_proto::cork::v1::run_event;
use cork_proto::cork::v1::{
    ArtifactRef as ProtoArtifactRef, InvokeToolRequest, InvokeToolResponse, InvokeToolStreamChunk,
    LogRecord, NodeStateChanged, NodeStatus, Payload, PolicyEvent, RunEvent, TraceContext,
};
use cork_store::{
    ArtifactRef, CircuitBreakerPolicy, EventLog, LogStore, NodeOutput, NodePayload, RetryBackoff,
    RetryBackoffPolicy, RetryJitter, RunCtx, StateStore, WorkerPolicy, WorkerRetryCondition,
    WorkerRetryPolicy,
};
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
    pub run_ctx: Option<&'a RunCtx>,
    pub event_log: &'a dyn EventLog,
    pub log_store: &'a dyn LogStore,
    pub state_store: &'a dyn StateStore,
    pub budget: InvocationBudget,
    pub trace_context: Option<TraceContext>,
    pub worker_policy: Option<WorkerPolicy>,
}

pub struct WorkerClient {
    client: CorkWorkerClient<Channel>,
    circuit_breaker: CircuitBreakerState,
}

impl WorkerClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            client: CorkWorkerClient::new(channel),
            circuit_breaker: CircuitBreakerState::Closed { failures: 0 },
        }
    }

    pub fn from_client(client: CorkWorkerClient<Channel>) -> Self {
        Self {
            client,
            circuit_breaker: CircuitBreakerState::Closed { failures: 0 },
        }
    }

    pub async fn invoke_tool(
        &mut self,
        request: InvokeToolRequest,
        context: &InvocationContext<'_>,
    ) -> Result<InvokeToolResponse, Status> {
        let result = self.invoke_tool_with_retry(request, context).await;
        match result {
            Ok(response) => {
                handle_success(context, &response).await;
                Ok(response)
            }
            Err(status) => {
                handle_failure(context, &status).await;
                Err(status)
            }
        }
    }

    pub async fn invoke_tool_stream(
        &mut self,
        request: InvokeToolRequest,
        context: &InvocationContext<'_>,
    ) -> Result<InvokeToolResponse, Status> {
        let result = self.invoke_tool_stream_with_retry(request, context).await;
        match result {
            Ok(response) => {
                handle_success(context, &response).await;
                Ok(response)
            }
            Err(status) => {
                handle_failure(context, &status).await;
                Err(status)
            }
        }
    }
}

#[derive(Debug, Clone)]
enum CircuitBreakerState {
    Closed { failures: u32 },
    Open { open_until: SystemTime },
    HalfOpen,
}

impl CircuitBreakerState {}

enum CircuitDecision {
    Allow,
    FailFast(Status),
}

impl WorkerClient {
    async fn invoke_tool_with_retry(
        &mut self,
        request: InvokeToolRequest,
        context: &InvocationContext<'_>,
    ) -> Result<InvokeToolResponse, Status> {
        let policy = context.worker_policy.as_ref().cloned().unwrap_or_default();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let decision = self
                .circuit_allow(&policy.retry.circuit_breaker, context)
                .await;
            if let CircuitDecision::FailFast(status) = decision {
                return Err(status);
            }
            let request = build_request(request.clone(), context);
            let result = self
                .client
                .invoke_tool(request)
                .await
                .map(|response| response.into_inner());
            match result {
                Ok(value) => {
                    self.circuit_on_success(&policy.retry.circuit_breaker, context)
                        .await;
                    return Ok(value);
                }
                Err(status) => {
                    self.circuit_on_failure(&policy.retry.circuit_breaker, context)
                        .await;
                    if !should_retry(&status, attempt, &policy.retry) {
                        return Err(status);
                    }
                    let delay = retry_delay(attempt, &policy.retry.backoff);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn invoke_tool_stream_with_retry(
        &mut self,
        request: InvokeToolRequest,
        context: &InvocationContext<'_>,
    ) -> Result<InvokeToolResponse, Status> {
        let policy = context.worker_policy.as_ref().cloned().unwrap_or_default();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let decision = self
                .circuit_allow(&policy.retry.circuit_breaker, context)
                .await;
            if let CircuitDecision::FailFast(status) = decision {
                return Err(status);
            }
            let request = build_request(request.clone(), context);
            let result = match self.client.invoke_tool_stream(request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let mut final_response = None;
                    let mut stream_error = None;
                    while let Some(chunk) = stream.next().await {
                        match chunk {
                            Ok(chunk) => {
                                if let Some(action) = handle_chunk(context, &chunk).await {
                                    final_response = Some(action);
                                }
                            }
                            Err(status) => {
                                stream_error = Some(status);
                                break;
                            }
                        }
                    }
                    if let Some(status) = stream_error {
                        Err(status)
                    } else if let Some(final_response) = final_response {
                        Ok(final_response)
                    } else {
                        Err(Status::data_loss(
                            "invoke tool stream ended without final response",
                        ))
                    }
                }
                Err(status) => Err(status),
            };
            match result {
                Ok(value) => {
                    self.circuit_on_success(&policy.retry.circuit_breaker, context)
                        .await;
                    return Ok(value);
                }
                Err(status) => {
                    self.circuit_on_failure(&policy.retry.circuit_breaker, context)
                        .await;
                    if !should_retry(&status, attempt, &policy.retry) {
                        return Err(status);
                    }
                    let delay = retry_delay(attempt, &policy.retry.backoff);
                    tokio::time::sleep(delay).await;
                }
            }
        }
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
    if let Some(timeout) = effective_timeout(SystemTime::now(), &context.budget, context) {
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

async fn handle_chunk(
    context: &InvocationContext<'_>,
    chunk: &InvokeToolStreamChunk,
) -> Option<InvokeToolResponse> {
    match &chunk.chunk {
        Some(Chunk::Log(log)) => {
            append_log(
                context.event_log,
                context.log_store,
                context.run_id,
                context.stage_id,
                context.node_id,
                log.clone(),
                chunk.ts,
            )
            .await;
            None
        }
        Some(Chunk::Final(response)) => Some(response.clone()),
        Some(Chunk::Heartbeat(_)) | Some(Chunk::PartialOutput(_)) | None => None,
    }
}

async fn handle_success(context: &InvocationContext<'_>, response: &InvokeToolResponse) {
    let output = node_output_from_response(response);
    context
        .state_store
        .set_node_output(context.run_id, context.node_id, output);
    append_node_state(
        context.run_ctx,
        context.event_log,
        context.stage_id,
        context.node_id,
        NodeStatus::NodeRunning,
        NodeStatus::NodeSucceeded,
        None,
    )
    .await;
}

async fn handle_failure(context: &InvocationContext<'_>, status: &Status) {
    append_node_state(
        context.run_ctx,
        context.event_log,
        context.stage_id,
        context.node_id,
        NodeStatus::NodeRunning,
        NodeStatus::NodeFailed,
        Some(format!("gRPC {:?}: {}", status.code(), status.message())),
    )
    .await;
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

async fn append_log(
    event_log: &dyn EventLog,
    log_store: &dyn LogStore,
    run_id: &str,
    stage_id: &str,
    node_id: &str,
    mut log: LogRecord,
    ts: Option<Timestamp>,
) {
    let timestamp = ts.or(log.ts).unwrap_or_else(now_timestamp);
    log.ts = Some(timestamp);
    let log = log_store.append_log(run_id, stage_id, node_id, log);
    let event = RunEvent {
        event_seq: 0,
        ts: Some(timestamp),
        event: Some(run_event::Event::Log(log)),
    };
    event_log.append(event).await;
}

async fn append_node_state(
    run_ctx: Option<&RunCtx>,
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
    event_log.append(event).await;
    if let Some(run_ctx) = run_ctx {
        run_ctx.touch_progress().await;
    }
}

fn timeout_from_budget(now: SystemTime, budget: &InvocationBudget) -> Option<Duration> {
    let deadline = budget.earliest_deadline()?;
    Some(
        deadline
            .duration_since(now)
            .unwrap_or_else(|_| Duration::from_millis(0)),
    )
}

fn effective_timeout(
    now: SystemTime,
    budget: &InvocationBudget,
    context: &InvocationContext<'_>,
) -> Option<Duration> {
    let budget_timeout = timeout_from_budget(now, budget);
    let policy_timeout = context
        .worker_policy
        .as_ref()
        .and_then(|policy| policy.timeout_ms)
        .or_else(|| WorkerPolicy::default().timeout_ms)
        .map(Duration::from_millis);
    merge_timeout(budget_timeout, policy_timeout)
}

fn merge_timeout(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
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

fn should_retry(status: &Status, attempt: u32, policy: &WorkerRetryPolicy) -> bool {
    if policy.max_attempts == 0 {
        return false;
    }
    if attempt >= policy.max_attempts {
        return false;
    }
    is_retryable(status, policy)
}

fn is_retryable(status: &Status, policy: &WorkerRetryPolicy) -> bool {
    policy
        .retry_on
        .iter()
        .any(|condition| status_matches_condition(status, condition))
}

fn status_matches_condition(status: &Status, condition: &WorkerRetryCondition) -> bool {
    match condition {
        WorkerRetryCondition::Timeout => status.code() == tonic::Code::DeadlineExceeded,
        WorkerRetryCondition::TransientNetwork => {
            matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::Unknown
            )
        }
        WorkerRetryCondition::GrpcUnavailable => status.code() == tonic::Code::Unavailable,
        WorkerRetryCondition::GrpcDeadlineExceeded => {
            status.code() == tonic::Code::DeadlineExceeded
        }
        WorkerRetryCondition::GrpcResourceExhausted => {
            status.code() == tonic::Code::ResourceExhausted
        }
        WorkerRetryCondition::GrpcAborted => status.code() == tonic::Code::Aborted,
        WorkerRetryCondition::GrpcCancelled => status.code() == tonic::Code::Cancelled,
        WorkerRetryCondition::GrpcUnknown => status.code() == tonic::Code::Unknown,
        WorkerRetryCondition::GrpcInternal => status.code() == tonic::Code::Internal,
    }
}

fn retry_delay(attempt: u32, backoff: &RetryBackoff) -> Duration {
    let attempt = attempt.max(1);
    let base = backoff.base_delay_ms;
    let raw_delay = match backoff.policy {
        RetryBackoffPolicy::Exponential => {
            let shift = attempt.saturating_sub(1);
            let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
            base.saturating_mul(multiplier)
        }
        RetryBackoffPolicy::Linear => base.saturating_mul(attempt as u64),
        RetryBackoffPolicy::Constant => base,
    };
    let capped = raw_delay.min(backoff.max_delay_ms);
    let jittered = apply_jitter(capped, backoff.jitter.clone());
    Duration::from_millis(jittered)
}

fn apply_jitter(delay_ms: u64, jitter: RetryJitter) -> u64 {
    if delay_ms == 0 {
        return 0;
    }
    match jitter {
        RetryJitter::None => delay_ms,
        RetryJitter::Full => random_bounded(delay_ms),
        RetryJitter::Equal => {
            let half = delay_ms / 2;
            half + random_bounded(delay_ms - half)
        }
    }
}

fn random_bounded(max_exclusive: u64) -> u64 {
    if max_exclusive <= 1 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .subsec_nanos() as u64;
    nanos % max_exclusive
}

impl WorkerClient {
    async fn circuit_allow(
        &mut self,
        policy: &CircuitBreakerPolicy,
        context: &InvocationContext<'_>,
    ) -> CircuitDecision {
        if !policy.enabled {
            return CircuitDecision::Allow;
        }
        let now = SystemTime::now();
        match self.circuit_breaker.clone() {
            CircuitBreakerState::Open { open_until } if now < open_until => {
                return CircuitDecision::FailFast(Status::unavailable(
                    "worker circuit breaker open",
                ));
            }
            CircuitBreakerState::Open { .. } => {
                self.circuit_breaker = CircuitBreakerState::HalfOpen;
                emit_circuit_event(context, "worker_circuit_half_open", policy, None).await;
            }
            _ => {}
        }
        CircuitDecision::Allow
    }

    async fn circuit_on_failure(
        &mut self,
        policy: &CircuitBreakerPolicy,
        context: &InvocationContext<'_>,
    ) {
        if !policy.enabled {
            return;
        }
        let now = SystemTime::now();
        match &mut self.circuit_breaker {
            CircuitBreakerState::Closed { failures } => {
                *failures = failures.saturating_add(1);
                let failure_count = *failures;
                if failure_count >= policy.open_after_failures {
                    let open_until = now
                        .checked_add(Duration::from_millis(policy.half_open_after_ms))
                        .unwrap_or(now);
                    self.circuit_breaker = CircuitBreakerState::Open { open_until };
                    emit_circuit_event(context, "worker_circuit_open", policy, Some(failure_count))
                        .await;
                }
            }
            CircuitBreakerState::HalfOpen => {
                let open_until = now
                    .checked_add(Duration::from_millis(policy.half_open_after_ms))
                    .unwrap_or(now);
                self.circuit_breaker = CircuitBreakerState::Open { open_until };
                emit_circuit_event(context, "worker_circuit_open", policy, Some(1)).await;
            }
            CircuitBreakerState::Open { .. } => {}
        }
    }

    async fn circuit_on_success(
        &mut self,
        policy: &CircuitBreakerPolicy,
        context: &InvocationContext<'_>,
    ) {
        if !policy.enabled {
            return;
        }
        match self.circuit_breaker {
            CircuitBreakerState::Closed { ref mut failures } => {
                *failures = 0;
            }
            _ => {
                self.circuit_breaker = CircuitBreakerState::Closed { failures: 0 };
                emit_circuit_event(context, "worker_circuit_closed", policy, Some(0)).await;
            }
        }
    }
}

async fn emit_circuit_event(
    context: &InvocationContext<'_>,
    kind: &str,
    policy: &CircuitBreakerPolicy,
    failures: Option<u32>,
) {
    let mut attrs = std::collections::HashMap::new();
    attrs.insert(
        "open_after_failures".to_string(),
        policy.open_after_failures.to_string(),
    );
    attrs.insert(
        "half_open_after_ms".to_string(),
        policy.half_open_after_ms.to_string(),
    );
    if let Some(failures) = failures {
        attrs.insert("failures".to_string(), failures.to_string());
    }
    let event = RunEvent {
        event_seq: 0,
        ts: Some(now_timestamp()),
        event: Some(run_event::Event::Policy(PolicyEvent {
            kind: kind.to_string(),
            attrs,
        })),
    };
    context.event_log.append(event).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use cork_proto::cork::v1::cork_worker_server::{CorkWorker, CorkWorkerServer};
    use cork_proto::cork::v1::{InvokeToolRequest, InvokeToolResponse};
    use cork_store::{
        CircuitBreakerPolicy, InMemoryEventLog, InMemoryLogStore, InMemoryStateStore, LogStore,
        RetryBackoff, RetryBackoffPolicy, RetryJitter, WorkerPolicy, WorkerRetryCondition,
        WorkerRetryPolicy,
    };
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
                .add_service(
                    CorkWorkerServer::new(TestWorker)
                        .max_decoding_message_size(crate::api::DEFAULT_GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(crate::api::DEFAULT_GRPC_MAX_MESSAGE_BYTES),
                )
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

    struct FlakyStreamWorker {
        remaining_failures: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl CorkWorker for FlakyStreamWorker {
        async fn health(
            &self,
            _request: Request<cork_proto::cork::v1::HealthRequest>,
        ) -> Result<Response<cork_proto::cork::v1::HealthResponse>, Status> {
            Ok(Response::new(cork_proto::cork::v1::HealthResponse {
                status: "ok".to_string(),
                worker_id: "worker-flaky".to_string(),
            }))
        }

        async fn invoke_tool(
            &self,
            _request: Request<InvokeToolRequest>,
        ) -> Result<Response<InvokeToolResponse>, Status> {
            Err(Status::unimplemented("invoke_tool not used"))
        }

        type InvokeToolStreamStream =
            Pin<Box<dyn Stream<Item = Result<InvokeToolStreamChunk, Status>> + Send>>;

        async fn invoke_tool_stream(
            &self,
            _request: Request<InvokeToolRequest>,
        ) -> Result<Response<Self::InvokeToolStreamStream>, Status> {
            let remaining = self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    Some(value.saturating_sub(1))
                })
                .unwrap_or(0);
            if remaining > 0 {
                let log = LogRecord {
                    ts: None,
                    level: "INFO".to_string(),
                    message: "flaky-log".to_string(),
                    trace_id_hex: "".to_string(),
                    span_id_hex: "".to_string(),
                    scope_id: "".to_string(),
                    scope_seq: 0,
                    attrs: Default::default(),
                };
                let chunks = vec![
                    Ok(InvokeToolStreamChunk {
                        ts: None,
                        chunk: Some(Chunk::Log(log)),
                    }),
                    Err(Status::unavailable("transient stream failure")),
                ];
                return Ok(Response::new(Box::pin(tokio_stream::iter(chunks))));
            }
            let response = InvokeToolResponse {
                output: Some(Payload {
                    content_type: "application/json".to_string(),
                    data: br#"{"ok":true}"#.to_vec(),
                    encoding: "".to_string(),
                    sha256: None,
                }),
                artifacts: Vec::new(),
                error_code: "".to_string(),
                error_message: "".to_string(),
            };
            let chunks = vec![Ok(InvokeToolStreamChunk {
                ts: None,
                chunk: Some(Chunk::Final(response)),
            })];
            Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
        }
    }

    async fn start_flaky_server(failures: usize) -> (Channel, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let incoming = TcpListenerStream::new(listener);
        let worker = FlakyStreamWorker {
            remaining_failures: Arc::new(AtomicUsize::new(failures)),
        };
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    CorkWorkerServer::new(worker)
                        .max_decoding_message_size(crate::api::DEFAULT_GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(crate::api::DEFAULT_GRPC_MAX_MESSAGE_BYTES),
                )
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

    fn test_retry_policy(max_attempts: u32) -> WorkerPolicy {
        WorkerPolicy {
            timeout_ms: Some(1_000),
            retry: WorkerRetryPolicy {
                max_attempts,
                backoff: RetryBackoff {
                    policy: RetryBackoffPolicy::Constant,
                    base_delay_ms: 1,
                    max_delay_ms: 1,
                    jitter: RetryJitter::None,
                },
                retry_on: vec![WorkerRetryCondition::GrpcUnavailable],
                circuit_breaker: CircuitBreakerPolicy {
                    enabled: false,
                    open_after_failures: 3,
                    half_open_after_ms: 1_000,
                },
            },
        }
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
        let log_store = InMemoryLogStore::new();
        let state_store = InMemoryStateStore::new();
        let context = InvocationContext {
            run_id: "run-1",
            stage_id: "stage-a",
            node_id: "stage-a/node-a",
            run_ctx: None,
            event_log: &event_log,
            log_store: &log_store,
            state_store: &state_store,
            budget: InvocationBudget::default(),
            trace_context: None,
            worker_policy: None,
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

        let events = event_log.subscribe(0).await.expect("subscribe").backlog;
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
        let log_store = InMemoryLogStore::new();
        let state_store = InMemoryStateStore::new();
        let context = InvocationContext {
            run_id: "run-2",
            stage_id: "stage-b",
            node_id: "stage-b/node-b",
            run_ctx: None,
            event_log: &event_log,
            log_store: &log_store,
            state_store: &state_store,
            budget: InvocationBudget::default(),
            trace_context: None,
            worker_policy: None,
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

        let events = event_log.subscribe(0).await.expect("subscribe").backlog;
        assert!(events.iter().any(|event| matches!(
            event.event.as_ref(),
            Some(run_event::Event::Log(LogRecord { message, .. }))
                if message == "stream-log"
        )));

        let page = log_store.list_logs("run-2", None, cork_store::LogFilters::default(), 10);
        assert_eq!(page.logs.len(), 1);
        assert!(page.logs[0].ts.is_some());

        handle.abort();
    }

    #[tokio::test]
    async fn invoke_tool_stream_retries_on_stream_error() {
        let (channel, handle) = start_flaky_server(1).await;
        let mut client = WorkerClient::new(channel);
        let event_log = InMemoryEventLog::new();
        let log_store = InMemoryLogStore::new();
        let state_store = InMemoryStateStore::new();
        let context = InvocationContext {
            run_id: "run-3",
            stage_id: "stage-c",
            node_id: "stage-c/node-c",
            run_ctx: None,
            event_log: &event_log,
            log_store: &log_store,
            state_store: &state_store,
            budget: InvocationBudget::default(),
            trace_context: None,
            worker_policy: Some(test_retry_policy(2)),
        };

        let request = InvokeToolRequest {
            invocation_id: "inv-3".to_string(),
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
            .expect("retry success");
        assert!(response.output.is_some());

        handle.abort();
    }
}
