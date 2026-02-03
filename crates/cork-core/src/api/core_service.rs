//! CorkCore gRPC service implementation.
//!
//! This module contains the implementation of the CorkCore gRPC service
//! as defined in `proto/cork/v1/core.proto`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::patch::apply_graph_patch_ops;
use crate::engine::run::node_stage_id;
use cork_hash::sha256;
use cork_proto::cork::v1::run_event;
use cork_proto::cork::v1::{
    ApplyGraphPatchRequest, ApplyGraphPatchResponse, CancelRunRequest, CancelRunResponse,
    CanonicalJsonDocument, GetCompositeGraphRequest, GetCompositeGraphResponse, GetLogsRequest,
    GetLogsResponse, GetRunRequest, GetRunResponse, ListRunsRequest, ListRunsResponse, RunEvent,
    StreamRunEventsRequest, SubmitRunRequest, SubmitRunResponse, cork_core_server::CorkCore,
};
use cork_store::{
    CreateRunInput, EventLog, InMemoryEventLog, InMemoryGraphStore, InMemoryRunRegistry,
    InMemoryStateStore, RunCtx, RunRegistry,
};
use prost_types::Timestamp;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::{Request, Response, Status};

/// The CorkCore service implementation.
pub struct CorkCoreService {
    event_logs: Arc<RwLock<HashMap<String, Arc<InMemoryEventLog>>>>,
    graph_store: Arc<InMemoryGraphStore>,
    run_registry: Arc<dyn RunRegistry>,
    state_store: Arc<InMemoryStateStore>,
}

impl CorkCoreService {
    /// Creates a new CorkCoreService instance.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_run_registry(run_registry: Arc<dyn RunRegistry>) -> Self {
        Self {
            event_logs: Arc::new(RwLock::new(HashMap::new())),
            graph_store: Arc::new(InMemoryGraphStore::new()),
            run_registry,
            state_store: Arc::new(InMemoryStateStore::new()),
        }
    }

    pub fn create_run(&self, input: CreateRunInput) -> Arc<RunCtx> {
        self.run_registry.create_run(input)
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

impl Default for CorkCoreService {
    fn default() -> Self {
        Self {
            event_logs: Arc::new(RwLock::new(HashMap::new())),
            graph_store: Arc::new(InMemoryGraphStore::new()),
            run_registry: Arc::new(InMemoryRunRegistry::new()),
            state_store: Arc::new(InMemoryStateStore::new()),
        }
    }
}

enum Sha256Verification {
    MatchOrMissing,
    Mismatch {
        expected: [u8; 32],
        provided: Vec<u8>,
    },
}

#[derive(Debug)]
enum GraphPatchRejectionReason {
    Sha256Mismatch {
        expected: String,
        provided: String,
    },
    PatchSeqMismatch {
        expected: u64,
        provided: u64,
    },
    StageNotActive {
        active_stage_id: Option<String>,
        provided_stage_id: String,
    },
    MissingExpansionPolicy,
    DynamicNodesNotAllowed,
    NodeKindNotAllowed {
        kind: String,
    },
    InvalidPatch {
        message: String,
    },
}

impl GraphPatchRejectionReason {
    fn message(&self) -> String {
        match self {
            GraphPatchRejectionReason::Sha256Mismatch { expected, provided } => {
                format!("sha256 mismatch (expected {expected}, provided {provided})")
            }
            GraphPatchRejectionReason::PatchSeqMismatch { expected, provided } => {
                format!("patch_seq mismatch (expected {expected}, provided {provided})")
            }
            GraphPatchRejectionReason::StageNotActive {
                active_stage_id,
                provided_stage_id,
            } => format!(
                "stage_id not active (active {}, provided {provided_stage_id})",
                active_stage_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string())
            ),
            GraphPatchRejectionReason::MissingExpansionPolicy => {
                "expansion_policy missing".to_string()
            }
            GraphPatchRejectionReason::DynamicNodesNotAllowed => {
                "dynamic nodes not allowed in active stage".to_string()
            }
            GraphPatchRejectionReason::NodeKindNotAllowed { kind } => {
                format!("node kind not allowed: {kind}")
            }
            GraphPatchRejectionReason::InvalidPatch { message } => {
                format!("invalid patch: {message}")
            }
        }
    }
}

#[derive(Debug)]
struct GraphPatchMetadata {
    patch_seq: u64,
    stage_id: String,
    node_kinds: Vec<String>,
    has_node_added: bool,
}

#[derive(Debug)]
struct StageTouch {
    touches_active_stage: bool,
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn verify_canonical_sha256(doc: &CanonicalJsonDocument) -> Result<Sha256Verification, Box<Status>> {
    let Some(sha) = doc.sha256.as_ref() else {
        return Ok(Sha256Verification::MatchOrMissing);
    };
    if sha.bytes32.is_empty() {
        return Ok(Sha256Verification::MatchOrMissing);
    }
    if sha.bytes32.len() != 32 {
        return Err(Box::new(Status::invalid_argument(
            "sha256.bytes32 must be 32 bytes",
        )));
    }
    let computed = sha256(&doc.canonical_json_utf8);
    if sha.bytes32.as_slice() == computed {
        Ok(Sha256Verification::MatchOrMissing)
    } else {
        Ok(Sha256Verification::Mismatch {
            expected: computed,
            provided: sha.bytes32.clone(),
        })
    }
}

fn parse_graph_patch_metadata(
    patch: &CanonicalJsonDocument,
) -> Result<GraphPatchMetadata, GraphPatchRejectionReason> {
    let value: Value = serde_json::from_slice(&patch.canonical_json_utf8).map_err(|err| {
        GraphPatchRejectionReason::InvalidPatch {
            message: err.to_string(),
        }
    })?;
    let patch_seq = value
        .get("patch_seq")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
            message: "patch_seq is missing or not an integer".to_string(),
        })?;
    let stage_id = value
        .get("stage_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
            message: "stage_id is missing or not a string".to_string(),
        })?
        .to_string();
    let ops = value
        .get("ops")
        .and_then(|value| value.as_array())
        .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
            message: "ops is missing or not an array".to_string(),
        })?;

    let mut node_kinds = Vec::new();
    let mut has_node_added = false;
    for op in ops {
        let op_type = op.get("op_type").and_then(|value| value.as_str());
        if op_type != Some("NODE_ADDED") {
            continue;
        }
        has_node_added = true;
        let node = op
            .get("node_added")
            .and_then(|value| value.get("node"))
            .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                message: "node_added.node is missing".to_string(),
            })?;
        let kind = node
            .get("kind")
            .and_then(|value| value.as_str())
            .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                message: "node_added.node.kind is missing".to_string(),
            })?;
        node_kinds.push(kind.to_string());
        let exec = node
            .get("exec")
            .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                message: "node_added.node.exec is missing".to_string(),
            })?;
        if let Some(tool) = exec.get("tool") {
            let side_effect = tool
                .get("side_effect")
                .and_then(|value| value.as_str())
                .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                    message: "tool.side_effect is missing".to_string(),
                })?;
            if side_effect != "NONE" {
                let idempotency_key = tool
                    .get("idempotency_key")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty());
                if idempotency_key.is_none() {
                    return Err(GraphPatchRejectionReason::InvalidPatch {
                        message: "tool.idempotency_key is required when side_effect != NONE"
                            .to_string(),
                    });
                }
            }
        }
    }

    Ok(GraphPatchMetadata {
        patch_seq,
        stage_id,
        node_kinds,
        has_node_added,
    })
}

fn parse_stage_touch(
    patch: &CanonicalJsonDocument,
    active_stage_id: &str,
) -> Result<StageTouch, GraphPatchRejectionReason> {
    let value: Value = serde_json::from_slice(&patch.canonical_json_utf8).map_err(|err| {
        GraphPatchRejectionReason::InvalidPatch {
            message: err.to_string(),
        }
    })?;
    let ops = value
        .get("ops")
        .and_then(|value| value.as_array())
        .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
            message: "ops is missing or not an array".to_string(),
        })?;
    for op in ops {
        let op_type = op.get("op_type").and_then(|value| value.as_str());
        match op_type {
            Some("NODE_ADDED") => {
                let node_id = op
                    .get("node_added")
                    .and_then(|value| value.get("node"))
                    .and_then(|value| value.get("node_id"))
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                        message: "node_added.node.node_id is missing".to_string(),
                    })?;
                if node_stage_id(node_id) == Some(active_stage_id) {
                    return Ok(StageTouch {
                        touches_active_stage: true,
                    });
                }
            }
            Some("EDGE_ADDED") => {
                let edge = op.get("edge_added").ok_or_else(|| {
                    GraphPatchRejectionReason::InvalidPatch {
                        message: "edge_added is missing".to_string(),
                    }
                })?;
                let from = edge
                    .get("from")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                        message: "edge_added.from is missing".to_string(),
                    })?;
                let to = edge
                    .get("to")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                        message: "edge_added.to is missing".to_string(),
                    })?;
                if node_stage_id(from) == Some(active_stage_id)
                    || node_stage_id(to) == Some(active_stage_id)
                {
                    return Ok(StageTouch {
                        touches_active_stage: true,
                    });
                }
            }
            Some("STATE_PUT") => {
                let state_put =
                    op.get("state_put")
                        .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                            message: "state_put is missing".to_string(),
                        })?;
                let scope = state_put
                    .get("scope")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                        message: "state_put.scope is missing".to_string(),
                    })?;
                if scope == "STAGE" {
                    let stage_id = state_put
                        .get("stage_id")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                            message: "state_put.stage_id is missing".to_string(),
                        })?;
                    if stage_id == active_stage_id {
                        return Ok(StageTouch {
                            touches_active_stage: true,
                        });
                    }
                }
            }
            Some("NODE_UPDATED") => {
                let node_id = op
                    .get("node_updated")
                    .and_then(|value| value.get("node_id"))
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| GraphPatchRejectionReason::InvalidPatch {
                        message: "node_updated.node_id is missing".to_string(),
                    })?;
                if node_stage_id(node_id) == Some(active_stage_id) {
                    return Ok(StageTouch {
                        touches_active_stage: true,
                    });
                }
            }
            Some(other) => {
                return Err(GraphPatchRejectionReason::InvalidPatch {
                    message: format!("unsupported op_type {other}"),
                });
            }
            None => {
                return Err(GraphPatchRejectionReason::InvalidPatch {
                    message: "op_type is missing".to_string(),
                });
            }
        }
    }
    Ok(StageTouch {
        touches_active_stage: false,
    })
}

fn reject_response(reason: GraphPatchRejectionReason) -> ApplyGraphPatchResponse {
    ApplyGraphPatchResponse {
        accepted: false,
        rejection_reason: reason.message(),
    }
}

#[tonic::async_trait]
impl CorkCore for CorkCoreService {
    async fn submit_run(
        &self,
        request: Request<SubmitRunRequest>,
    ) -> Result<Response<SubmitRunResponse>, Status> {
        let request = request.into_inner();
        if let Some(contract) = request.contract_manifest.as_ref()
            && let Sha256Verification::Mismatch { .. } =
                verify_canonical_sha256(contract).map_err(|status| *status)?
        {
            return Err(Status::invalid_argument(
                "contract_manifest sha256 mismatch",
            ));
        }
        if let Some(policy) = request.policy.as_ref()
            && let Sha256Verification::Mismatch { .. } =
                verify_canonical_sha256(policy).map_err(|status| *status)?
        {
            return Err(Status::invalid_argument("policy sha256 mismatch"));
        }
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
        request: Request<ApplyGraphPatchRequest>,
    ) -> Result<Response<ApplyGraphPatchResponse>, Status> {
        let request = request.into_inner();
        let handle = request
            .handle
            .ok_or_else(|| Status::invalid_argument("missing run handle"))?;
        if handle.run_id.is_empty() {
            return Err(Status::invalid_argument("missing run_id"));
        }
        let run_ctx = self
            .run_registry
            .get_run(&handle.run_id)
            .ok_or_else(|| Status::not_found("run not found"))?;
        let patch = request
            .patch
            .ok_or_else(|| Status::invalid_argument("missing patch"))?;
        let verification = verify_canonical_sha256(&patch).map_err(|status| *status)?;
        if let Sha256Verification::Mismatch { expected, provided } = verification {
            let rejection_reason = GraphPatchRejectionReason::Sha256Mismatch {
                expected: bytes_to_hex(&expected),
                provided: bytes_to_hex(&provided),
            };
            return Ok(Response::new(reject_response(rejection_reason)));
        }
        let metadata = match parse_graph_patch_metadata(&patch) {
            Ok(metadata) => metadata,
            Err(reason) => return Ok(Response::new(reject_response(reason))),
        };
        if run_ctx.active_stage_id().as_deref() != Some(&metadata.stage_id) {
            return Ok(Response::new(reject_response(
                GraphPatchRejectionReason::StageNotActive {
                    active_stage_id: run_ctx.active_stage_id(),
                    provided_stage_id: metadata.stage_id,
                },
            )));
        }
        let Some(expansion_policy) = run_ctx.active_stage_expansion_policy() else {
            return Ok(Response::new(reject_response(
                GraphPatchRejectionReason::MissingExpansionPolicy,
            )));
        };
        if metadata.has_node_added && !expansion_policy.allow_dynamic {
            return Ok(Response::new(reject_response(
                GraphPatchRejectionReason::DynamicNodesNotAllowed,
            )));
        }
        if metadata.has_node_added {
            for kind in metadata.node_kinds {
                if !expansion_policy
                    .allow_kinds
                    .iter()
                    .any(|allowed| allowed == &kind)
                {
                    return Ok(Response::new(reject_response(
                        GraphPatchRejectionReason::NodeKindNotAllowed { kind },
                    )));
                }
            }
        }
        if let Err(expected) = run_ctx.check_patch_seq(metadata.patch_seq) {
            return Ok(Response::new(reject_response(
                GraphPatchRejectionReason::PatchSeqMismatch {
                    expected,
                    provided: metadata.patch_seq,
                },
            )));
        }
        let stage_touch = match parse_stage_touch(&patch, &metadata.stage_id) {
            Ok(stage_touch) => stage_touch,
            Err(reason) => return Ok(Response::new(reject_response(reason))),
        };
        if let Err(err) = apply_graph_patch_ops(
            run_ctx.run_id(),
            &patch,
            self.graph_store.as_ref(),
            self.state_store.as_ref(),
        ) {
            return Ok(Response::new(reject_response(
                GraphPatchRejectionReason::InvalidPatch {
                    message: err.message(),
                },
            )));
        }
        run_ctx.advance_patch_seq();
        if stage_touch.touches_active_stage {
            run_ctx.touch_stage_patch();
        }
        let event = RunEvent {
            event_seq: 0,
            ts: Some(system_time_to_timestamp(SystemTime::now())),
            event: Some(run_event::Event::GraphPatch(patch.clone())),
        };
        self.event_log_for_run(run_ctx.run_id()).append(event);
        Ok(Response::new(ApplyGraphPatchResponse {
            accepted: true,
            rejection_reason: String::new(),
        }))
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
    use cork_proto::cork::v1::{
        ApplyGraphPatchRequest, CanonicalJsonDocument, RunHandle, Sha256, StreamRunEventsRequest,
    };
    use cork_store::{
        CreateRunInput, ExpansionPolicy, GraphStore, InMemoryRunRegistry, RunRegistry, StateStore,
    };
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    fn build_sha256(bytes: &[u8]) -> Sha256 {
        Sha256 {
            bytes32: bytes.to_vec(),
        }
    }

    fn build_patch_document(
        run_id: &str,
        patch_seq: u64,
        stage_id: &str,
        ops: Value,
    ) -> CanonicalJsonDocument {
        let payload = json!({
            "schema_version": "cork.graph_patch.v0.1",
            "run_id": run_id,
            "patch_seq": patch_seq,
            "stage_id": stage_id,
            "ops": ops,
        });
        let canonical_json_utf8 = serde_json::to_vec(&payload).expect("serialize patch payload");
        let digest = sha256(&canonical_json_utf8);
        CanonicalJsonDocument {
            canonical_json_utf8,
            sha256: Some(build_sha256(&digest)),
            schema_id: "cork.graph_patch.v0.1".to_string(),
        }
    }

    fn node_added_op(kind: &str) -> Value {
        node_added_op_with_id("node-1", kind)
    }

    fn node_added_op_with_id(node_id: &str, kind: &str) -> Value {
        json!({
            "op_type": "NODE_ADDED",
            "node_added": {
                "node": {
                    "node_id": node_id,
                    "kind": kind,
                    "anchor_position": "WITHIN",
                    "deps": [],
                    "exec": { "llm": { "provider": "test", "model": "test", "messages": [ { "role": "user", "parts": [ { "text": "hi" } ] } ] } }
                }
            }
        })
    }

    fn edge_added_op(from: &str, to: &str) -> Value {
        json!({
            "op_type": "EDGE_ADDED",
            "edge_added": { "from": from, "to": to }
        })
    }

    fn state_put_op(
        scope: &str,
        stage_id: Option<&str>,
        json_pointer: &str,
        value: Value,
    ) -> Value {
        let mut state_put = json!({
            "scope": scope,
            "json_pointer": json_pointer,
            "value": value
        });
        if let Some(stage_id) = stage_id {
            state_put
                .as_object_mut()
                .expect("state_put object")
                .insert("stage_id".to_string(), json!(stage_id));
        }
        json!({
            "op_type": "STATE_PUT",
            "state_put": state_put
        })
    }

    fn node_updated_op(node_id: &str, patch: Value) -> Value {
        json!({
            "op_type": "NODE_UPDATED",
            "node_updated": { "node_id": node_id, "patch": patch }
        })
    }

    fn node_added_tool_op(side_effect: &str, idempotency_key: Option<&str>) -> Value {
        let mut tool_exec = json!({
            "tool_name": "tool-a",
            "tool_version": "v1",
            "input": {
                "literal": {
                    "content_type": "application/json",
                    "data_base64": "e30="
                }
            },
            "side_effect": side_effect
        });
        if let Some(key) = idempotency_key {
            tool_exec
                .as_object_mut()
                .expect("tool exec object")
                .insert("idempotency_key".to_string(), json!(key));
        }
        json!({
            "op_type": "NODE_ADDED",
            "node_added": {
                "node": {
                    "node_id": "node-tool-1",
                    "kind": "TOOL",
                    "anchor_position": "WITHIN",
                    "deps": [],
                    "exec": { "tool": tool_exec }
                }
            }
        })
    }

    fn setup_service_with_run(
        active_stage_id: &str,
        allow_dynamic: bool,
        allow_kinds: Vec<&str>,
        next_patch_seq: u64,
    ) -> (CorkCoreService, RunHandle) {
        let registry: Arc<dyn RunRegistry> = Arc::new(InMemoryRunRegistry::new());
        let service = CorkCoreService::with_run_registry(Arc::clone(&registry));
        let run = registry.create_run(CreateRunInput {
            active_stage_id: Some(active_stage_id.to_string()),
            active_stage_expansion_policy: Some(ExpansionPolicy {
                allow_dynamic,
                allow_kinds: allow_kinds.into_iter().map(str::to_string).collect(),
            }),
            next_patch_seq: Some(next_patch_seq),
            ..Default::default()
        });
        (
            service,
            RunHandle {
                run_id: run.run_id().to_string(),
            },
        )
    }

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

    #[tokio::test]
    async fn apply_graph_patch_rejects_sha256_mismatch() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["LLM"], 0);
        let patch =
            build_patch_document(&handle.run_id, 0, "stage-a", json!([node_added_op("LLM")]));
        let patch = CanonicalJsonDocument {
            sha256: Some(Sha256 {
                bytes32: vec![0u8; 32],
            }),
            ..patch
        };
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };
        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(!response.accepted);
        assert!(
            response.rejection_reason.contains("sha256 mismatch"),
            "unexpected rejection reason: {}",
            response.rejection_reason
        );
    }

    #[tokio::test]
    async fn apply_graph_patch_accepts_matching_sha256() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["LLM"], 0);
        let patch =
            build_patch_document(&handle.run_id, 0, "stage-a", json!([node_added_op("LLM")]));
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };
        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        if !response.accepted {
            panic!("rejection: {}", response.rejection_reason);
        }
        assert!(response.rejection_reason.is_empty());
    }

    #[tokio::test]
    async fn apply_graph_patch_rejects_patch_seq_gap() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["LLM"], 0);
        let patch =
            build_patch_document(&handle.run_id, 2, "stage-a", json!([node_added_op("LLM")]));
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };

        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(!response.accepted);
        assert!(
            response.rejection_reason.contains("patch_seq mismatch"),
            "unexpected rejection reason: {}",
            response.rejection_reason
        );
    }

    #[tokio::test]
    async fn apply_graph_patch_rejects_inactive_stage() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["LLM"], 0);
        let patch =
            build_patch_document(&handle.run_id, 0, "stage-b", json!([node_added_op("LLM")]));
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };

        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(!response.accepted);
        assert!(
            response.rejection_reason.contains("stage_id not active"),
            "unexpected rejection reason: {}",
            response.rejection_reason
        );
    }

    #[tokio::test]
    async fn apply_graph_patch_rejects_disallowed_node_kind() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["LLM"], 0);
        let patch =
            build_patch_document(&handle.run_id, 0, "stage-a", json!([node_added_op("TOOL")]));
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };

        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(!response.accepted);
        assert!(
            response.rejection_reason.contains("node kind not allowed"),
            "unexpected rejection reason: {}",
            response.rejection_reason
        );
    }

    #[tokio::test]
    async fn apply_graph_patch_rejects_side_effect_tool_without_idempotency_key() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["TOOL"], 0);
        let patch = build_patch_document(
            &handle.run_id,
            0,
            "stage-a",
            json!([node_added_tool_op("EXTERNAL_WRITE", None)]),
        );
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };

        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(!response.accepted);
        assert!(
            response.rejection_reason.contains("idempotency_key"),
            "unexpected rejection reason: {}",
            response.rejection_reason
        );
    }

    #[tokio::test]
    async fn apply_graph_patch_accepts_side_effect_none_without_idempotency_key() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["TOOL"], 0);
        let patch = build_patch_document(
            &handle.run_id,
            0,
            "stage-a",
            json!([node_added_tool_op("NONE", None)]),
        );
        let request = ApplyGraphPatchRequest {
            handle: Some(handle),
            patch: Some(patch),
            actor_id: String::new(),
        };

        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(
            response.accepted,
            "rejection: {}",
            response.rejection_reason
        );
        assert!(response.rejection_reason.is_empty());
    }

    #[tokio::test]
    async fn apply_graph_patch_updates_graph_state_and_logs_event() {
        let (service, handle) = setup_service_with_run("stage-a", true, vec!["LLM"], 0);
        let ops = json!([
            node_added_op_with_id("node-a", "LLM"),
            node_added_op_with_id("node-b", "LLM"),
            edge_added_op("node-a", "node-b"),
            state_put_op("RUN", None, "/state/value", json!({"ok": true})),
            node_updated_op("node-b", json!({"ttl_ms": 1234}))
        ]);
        let patch = build_patch_document(&handle.run_id, 0, "stage-a", ops);
        let request = ApplyGraphPatchRequest {
            handle: Some(handle.clone()),
            patch: Some(patch.clone()),
            actor_id: String::new(),
        };

        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        if !response.accepted {
            panic!("rejection: {}", response.rejection_reason);
        }

        let node_b = service
            .graph_store
            .node(&handle.run_id, "node-b")
            .expect("node-b");
        assert_eq!(node_b.ttl_ms, Some(1234));
        let deps = service
            .graph_store
            .deps(&handle.run_id, "node-b")
            .expect("deps");
        assert_eq!(deps, vec!["node-a".to_string()]);

        let run_state = service
            .state_store
            .run_state(&handle.run_id)
            .expect("run state");
        assert_eq!(run_state.pointer("/state/value/ok"), Some(&json!(true)));

        let log = service.event_log_for_run(&handle.run_id);
        let subscription = log.subscribe(0);
        assert_eq!(subscription.backlog.len(), 1);
        let event = &subscription.backlog[0];
        match event.event.as_ref() {
            Some(run_event::Event::GraphPatch(doc)) => {
                assert_eq!(doc.schema_id, patch.schema_id);
            }
            _ => panic!("expected graph_patch event"),
        }
    }

    #[tokio::test]
    async fn apply_graph_patch_updates_stage_patch_only_when_active_stage_touched() {
        let registry: Arc<dyn RunRegistry> = Arc::new(InMemoryRunRegistry::new());
        let service = CorkCoreService::with_run_registry(Arc::clone(&registry));
        let run = registry.create_run(CreateRunInput {
            active_stage_id: Some("stage-a".to_string()),
            active_stage_expansion_policy: Some(ExpansionPolicy {
                allow_dynamic: true,
                allow_kinds: vec!["LLM".to_string()],
            }),
            next_patch_seq: Some(0),
            ..Default::default()
        });
        let old = SystemTime::now() - Duration::from_secs(30);
        run.set_last_patch_at(Some(old));
        let patch = build_patch_document(
            run.run_id(),
            0,
            "stage-a",
            json!([node_added_op_with_id("stage-b/node-1", "LLM")]),
        );
        let request = ApplyGraphPatchRequest {
            handle: Some(RunHandle {
                run_id: run.run_id().to_string(),
            }),
            patch: Some(patch),
            actor_id: String::new(),
        };
        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(response.accepted);
        assert_eq!(run.last_patch_at(), Some(old));

        let patch = build_patch_document(
            run.run_id(),
            1,
            "stage-a",
            json!([node_added_op_with_id("stage-a/node-2", "LLM")]),
        );
        let request = ApplyGraphPatchRequest {
            handle: Some(RunHandle {
                run_id: run.run_id().to_string(),
            }),
            patch: Some(patch),
            actor_id: String::new(),
        };
        let response = service
            .apply_graph_patch(Request::new(request))
            .await
            .expect("apply response")
            .into_inner();
        assert!(response.accepted);
        let updated = run.last_patch_at().expect("last_patch_at");
        assert!(updated > old);
    }
}
