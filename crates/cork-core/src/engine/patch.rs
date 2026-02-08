use std::collections::{HashMap, HashSet};
use std::fmt;

use cork_hash::{composite_graph_hash, patch_hash};
use cork_proto::cork::v1::{CanonicalJsonDocument, HashBundle, Sha256};
use cork_store::{
    GraphStore, GraphStoreError, NodeSpec, RunCtx, StateStore, StateStoreError,
    apply_state_put_value,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatchRejectReason {
    Sha256Mismatch {
        expected: String,
        provided: String,
    },
    PatchSeqGap {
        expected: u64,
        provided: u64,
    },
    StageNotActive {
        active_stage_id: Option<String>,
        provided_stage_id: String,
    },
    ExpansionPolicyMissing,
    DynamicNodesNotAllowed,
    KindNotAllowed {
        kind: String,
    },
    IdempotencyRequired,
    InvalidJsonPointer {
        pointer: String,
    },
    UnknownNodeId {
        node_id: String,
    },
    CycleDetected {
        from: String,
        to: String,
    },
    LimitExceeded {
        limit: String,
        actual: usize,
    },
    InvalidPatch {
        message: String,
    },
}

impl fmt::Display for PatchRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatchRejectReason::Sha256Mismatch { expected, provided } => write!(
                f,
                "sha256 mismatch (expected {expected}, provided {provided})"
            ),
            PatchRejectReason::PatchSeqGap { expected, provided } => {
                write!(
                    f,
                    "patch_seq gap (expected {expected}, provided {provided})"
                )
            }
            PatchRejectReason::StageNotActive {
                active_stage_id,
                provided_stage_id,
            } => write!(
                f,
                "stage not active (active {}, provided {provided_stage_id})",
                active_stage_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string())
            ),
            PatchRejectReason::ExpansionPolicyMissing => write!(f, "expansion_policy missing"),
            PatchRejectReason::DynamicNodesNotAllowed => {
                write!(f, "dynamic nodes not allowed in active stage")
            }
            PatchRejectReason::KindNotAllowed { kind } => {
                write!(f, "node kind not allowed: {kind}")
            }
            PatchRejectReason::IdempotencyRequired => {
                write!(f, "idempotency_key required for side_effect")
            }
            PatchRejectReason::InvalidJsonPointer { pointer } => {
                write!(f, "invalid json pointer: {pointer}")
            }
            PatchRejectReason::UnknownNodeId { node_id } => write!(f, "unknown node_id: {node_id}"),
            PatchRejectReason::CycleDetected { from, to } => {
                write!(f, "cycle detected: {from} -> {to}")
            }
            PatchRejectReason::LimitExceeded { limit, actual } => {
                write!(f, "limit exceeded: {limit} (actual {actual})")
            }
            PatchRejectReason::InvalidPatch { message } => write!(f, "invalid patch: {message}"),
        }
    }
}

#[derive(Debug)]
pub enum GraphPatchApplyError {
    InvalidPatch(String),
    GraphStore(GraphStoreError),
    StateStore(StateStoreError),
}

impl GraphPatchApplyError {
    pub fn message(&self) -> String {
        match self {
            GraphPatchApplyError::InvalidPatch(message) => message.clone(),
            GraphPatchApplyError::GraphStore(err) => format!("graph store error: {err:?}"),
            GraphPatchApplyError::StateStore(err) => format!("state store error: {err:?}"),
        }
    }

    pub fn reject_reason(&self) -> PatchRejectReason {
        match self {
            GraphPatchApplyError::InvalidPatch(message) => PatchRejectReason::InvalidPatch {
                message: message.clone(),
            },
            GraphPatchApplyError::GraphStore(err) => match err {
                GraphStoreError::NodeAlreadyExists(node_id) => PatchRejectReason::InvalidPatch {
                    message: format!("node already exists: {node_id}"),
                },
                GraphStoreError::NodeNotFound(node_id) => PatchRejectReason::UnknownNodeId {
                    node_id: node_id.clone(),
                },
                GraphStoreError::CycleDetected { from, to } => PatchRejectReason::CycleDetected {
                    from: from.clone(),
                    to: to.clone(),
                },
            },
            GraphPatchApplyError::StateStore(err) => match err {
                StateStoreError::InvalidJsonPointer(pointer) => {
                    PatchRejectReason::InvalidJsonPointer {
                        pointer: pointer.clone(),
                    }
                }
                StateStoreError::MissingStageId => PatchRejectReason::InvalidPatch {
                    message: "stage_id is required for STAGE scope".to_string(),
                },
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct PatchTracker {
    contract_hash: [u8; 32],
    patch_hashes: Vec<[u8; 32]>,
}

#[derive(Debug, Clone)]
pub struct ValidatedGraphPatch {
    run_id: String,
    ops: Vec<GraphPatchOp>,
}

#[derive(Debug, Clone)]
enum GraphPatchOp {
    NodeAdded(NodeSpec),
    EdgeAdded {
        from: String,
        to: String,
    },
    StatePut {
        scope: String,
        stage_id: Option<String>,
        json_pointer: String,
        value: Value,
    },
    NodeUpdated {
        node_id: String,
        ttl_ms: Option<u64>,
        scheduling: Option<Value>,
        htn: Option<Value>,
    },
}

impl PatchTracker {
    pub fn new(contract_hash: [u8; 32]) -> Self {
        Self {
            contract_hash,
            patch_hashes: Vec::new(),
        }
    }

    pub fn patch_hashes(&self) -> &[[u8; 32]] {
        &self.patch_hashes
    }

    pub async fn apply_patch(&mut self, run_ctx: &RunCtx, patch_bytes: &[u8]) -> HashBundle {
        let digest = patch_hash(patch_bytes);
        self.patch_hashes.push(digest);
        run_ctx.advance_patch_seq().await;
        self.update_hash_bundle(run_ctx).await
    }

    async fn update_hash_bundle(&self, run_ctx: &RunCtx) -> HashBundle {
        let composite = composite_graph_hash(&self.contract_hash, &self.patch_hashes);
        let existing = run_ctx.metadata().await.hash_bundle;
        let (policy_hash, run_config_hash) = existing
            .as_ref()
            .map(|bundle| (bundle.policy_hash.clone(), bundle.run_config_hash.clone()))
            .unwrap_or((None, None));
        let bundle = HashBundle {
            contract_manifest_hash: Some(bytes_to_sha256(&self.contract_hash)),
            policy_hash,
            run_config_hash,
            composite_graph_hash: Some(bytes_to_sha256(&composite)),
        };
        run_ctx.set_hash_bundle(Some(bundle.clone())).await;
        bundle
    }
}

fn bytes_to_sha256(bytes: &[u8; 32]) -> Sha256 {
    Sha256 {
        bytes32: bytes.to_vec(),
    }
}

struct GraphOverlay<'a> {
    graph_store: &'a dyn GraphStore,
    existing_nodes: HashMap<String, NodeSpec>,
    existing_deps: HashMap<String, Vec<String>>,
    added_nodes: HashMap<String, NodeSpec>,
    added_edges: HashMap<String, Vec<String>>,
}

impl<'a> GraphOverlay<'a> {
    fn new(graph_store: &'a dyn GraphStore) -> Self {
        Self {
            graph_store,
            existing_nodes: HashMap::new(),
            existing_deps: HashMap::new(),
            added_nodes: HashMap::new(),
            added_edges: HashMap::new(),
        }
    }

    fn node_exists(&mut self, run_id: &str, node_id: &str) -> bool {
        if self.added_nodes.contains_key(node_id) {
            return true;
        }
        if self.existing_nodes.contains_key(node_id) {
            return true;
        }
        if let Some(node) = self.graph_store.node(run_id, node_id) {
            self.existing_nodes.insert(node_id.to_string(), node);
            return true;
        }
        false
    }

    fn ensure_node_exists(
        &mut self,
        run_id: &str,
        node_id: &str,
    ) -> Result<(), GraphPatchApplyError> {
        if self.node_exists(run_id, node_id) {
            Ok(())
        } else {
            Err(GraphPatchApplyError::GraphStore(
                GraphStoreError::NodeNotFound(node_id.to_string()),
            ))
        }
    }

    fn get_deps(&mut self, run_id: &str, node_id: &str) -> Vec<String> {
        let mut deps = if let Some(node) = self.added_nodes.get(node_id) {
            node.deps.clone()
        } else {
            if let Some(existing) = self.existing_deps.get(node_id) {
                existing.clone()
            } else {
                let fetched = self.graph_store.deps(run_id, node_id).unwrap_or_default();
                self.existing_deps
                    .insert(node_id.to_string(), fetched.clone());
                fetched
            }
        };
        if let Some(extra) = self.added_edges.get(node_id) {
            for dep in extra {
                if !deps.iter().any(|existing| existing == dep) {
                    deps.push(dep.clone());
                }
            }
        }
        deps
    }

    fn would_create_cycle(&mut self, run_id: &str, from: &str, to: &str) -> bool {
        if from == to {
            return true;
        }
        let mut stack = vec![from.to_string()];
        let mut visited: HashSet<String> = HashSet::new();
        while let Some(node) = stack.pop() {
            if node == to {
                return true;
            }
            if !visited.insert(node.clone()) {
                continue;
            }
            for dep in self.get_deps(run_id, &node) {
                stack.push(dep);
            }
        }
        false
    }

    fn add_node(&mut self, node: NodeSpec) {
        self.added_nodes.insert(node.node_id.clone(), node);
    }

    fn add_edge(&mut self, from: &str, to: &str) {
        let deps = self.added_edges.entry(to.to_string()).or_default();
        if !deps.iter().any(|dep| dep == from) {
            deps.push(from.to_string());
        }
    }

    fn update_node(
        &mut self,
        node_id: &str,
        ttl_ms: Option<u64>,
        scheduling: Option<Value>,
        htn: Option<Value>,
    ) {
        if let Some(node) = self.added_nodes.get_mut(node_id) {
            if let Some(ttl_ms) = ttl_ms {
                node.ttl_ms = Some(ttl_ms);
            }
            if scheduling.is_some() {
                node.scheduling = scheduling;
            }
            if htn.is_some() {
                node.htn = htn;
            }
        }
    }
}

struct StateOverlay<'a> {
    state_store: &'a dyn StateStore,
    run_id: String,
    run_state: Value,
    stage_state: HashMap<String, Value>,
}

impl<'a> StateOverlay<'a> {
    fn new(run_id: &str, state_store: &'a dyn StateStore) -> Self {
        Self {
            state_store,
            run_id: run_id.to_string(),
            run_state: state_store
                .run_state(run_id)
                .unwrap_or_else(|| Value::Object(serde_json::Map::new())),
            stage_state: HashMap::new(),
        }
    }

    fn stage_entry(&mut self, stage_id: &str) -> &mut Value {
        self.stage_state
            .entry(stage_id.to_string())
            .or_insert_with(|| {
                self.state_store
                    .stage_state(&self.run_id, stage_id)
                    .unwrap_or_else(|| Value::Object(serde_json::Map::new()))
            })
    }

    fn apply_state_put(
        &mut self,
        scope: &str,
        stage_id: Option<&str>,
        json_pointer: &str,
        value: Value,
    ) -> Result<(), GraphPatchApplyError> {
        match scope {
            "RUN" => apply_state_put_value(&mut self.run_state, json_pointer, value)
                .map_err(GraphPatchApplyError::StateStore),
            "STAGE" => {
                let stage_id = stage_id.ok_or(GraphPatchApplyError::StateStore(
                    StateStoreError::MissingStageId,
                ))?;
                let entry = self.stage_entry(stage_id);
                apply_state_put_value(entry, json_pointer, value)
                    .map_err(GraphPatchApplyError::StateStore)
            }
            _ => Err(GraphPatchApplyError::StateStore(
                StateStoreError::InvalidJsonPointer(format!("unknown scope {scope}")),
            )),
        }
    }
}

pub fn validate_graph_patch(
    run_id: &str,
    patch: &CanonicalJsonDocument,
    graph_store: &dyn GraphStore,
    state_store: &dyn StateStore,
) -> Result<ValidatedGraphPatch, GraphPatchApplyError> {
    let value: Value = serde_json::from_slice(&patch.canonical_json_utf8)
        .map_err(|err| GraphPatchApplyError::InvalidPatch(format!("invalid patch json: {err}")))?;
    let ops = value
        .get("ops")
        .and_then(|value| value.as_array())
        .ok_or_else(|| GraphPatchApplyError::InvalidPatch("ops is missing".to_string()))?;
    let mut graph_overlay = GraphOverlay::new(graph_store);
    let mut state_overlay = StateOverlay::new(run_id, state_store);
    let mut validated_ops = Vec::with_capacity(ops.len());
    for op in ops {
        let op_type = op
            .get("op_type")
            .and_then(|value| value.as_str())
            .ok_or_else(|| GraphPatchApplyError::InvalidPatch("op_type is missing".to_string()))?;
        match op_type {
            "NODE_ADDED" => {
                let node = op
                    .get("node_added")
                    .and_then(|value| value.get("node"))
                    .ok_or_else(|| {
                        GraphPatchApplyError::InvalidPatch("node_added.node is missing".to_string())
                    })?;
                let node_spec = parse_node_spec(node)?;
                if graph_overlay.node_exists(run_id, &node_spec.node_id) {
                    return Err(GraphPatchApplyError::GraphStore(
                        GraphStoreError::NodeAlreadyExists(node_spec.node_id),
                    ));
                }
                graph_overlay.add_node(node_spec.clone());
                validated_ops.push(GraphPatchOp::NodeAdded(node_spec));
            }
            "EDGE_ADDED" => {
                let edge = op.get("edge_added").ok_or_else(|| {
                    GraphPatchApplyError::InvalidPatch("edge_added is missing".to_string())
                })?;
                let from = edge
                    .get("from")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        GraphPatchApplyError::InvalidPatch("edge_added.from is missing".to_string())
                    })?;
                let to = edge
                    .get("to")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        GraphPatchApplyError::InvalidPatch("edge_added.to is missing".to_string())
                    })?;
                graph_overlay.ensure_node_exists(run_id, from)?;
                graph_overlay.ensure_node_exists(run_id, to)?;
                if graph_overlay.would_create_cycle(run_id, from, to) {
                    return Err(GraphPatchApplyError::GraphStore(
                        GraphStoreError::CycleDetected {
                            from: from.to_string(),
                            to: to.to_string(),
                        },
                    ));
                }
                graph_overlay.add_edge(from, to);
                validated_ops.push(GraphPatchOp::EdgeAdded {
                    from: from.to_string(),
                    to: to.to_string(),
                });
            }
            "STATE_PUT" => {
                let state_put = op.get("state_put").ok_or_else(|| {
                    GraphPatchApplyError::InvalidPatch("state_put is missing".to_string())
                })?;
                let scope = state_put
                    .get("scope")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        GraphPatchApplyError::InvalidPatch("state_put.scope is missing".to_string())
                    })?;
                let stage_id = state_put.get("stage_id").and_then(|value| value.as_str());
                let json_pointer = state_put
                    .get("json_pointer")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        GraphPatchApplyError::InvalidPatch(
                            "state_put.json_pointer is missing".to_string(),
                        )
                    })?;
                let value = state_put.get("value").cloned().ok_or_else(|| {
                    GraphPatchApplyError::InvalidPatch("state_put.value is missing".to_string())
                })?;
                state_overlay.apply_state_put(scope, stage_id, json_pointer, value.clone())?;
                validated_ops.push(GraphPatchOp::StatePut {
                    scope: scope.to_string(),
                    stage_id: stage_id.map(str::to_string),
                    json_pointer: json_pointer.to_string(),
                    value,
                });
            }
            "NODE_UPDATED" => {
                let node_updated = op.get("node_updated").ok_or_else(|| {
                    GraphPatchApplyError::InvalidPatch("node_updated is missing".to_string())
                })?;
                let node_id = node_updated
                    .get("node_id")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        GraphPatchApplyError::InvalidPatch(
                            "node_updated.node_id is missing".to_string(),
                        )
                    })?;
                let patch = node_updated.get("patch").ok_or_else(|| {
                    GraphPatchApplyError::InvalidPatch("node_updated.patch is missing".to_string())
                })?;
                let ttl_ms = patch.get("ttl_ms").and_then(|value| value.as_u64());
                let scheduling = patch.get("scheduling").cloned();
                let htn = patch.get("htn").cloned();
                graph_overlay.ensure_node_exists(run_id, node_id)?;
                graph_overlay.update_node(node_id, ttl_ms, scheduling.clone(), htn.clone());
                validated_ops.push(GraphPatchOp::NodeUpdated {
                    node_id: node_id.to_string(),
                    ttl_ms,
                    scheduling,
                    htn,
                });
            }
            other => {
                return Err(GraphPatchApplyError::InvalidPatch(format!(
                    "unsupported op_type {other}"
                )));
            }
        }
    }
    Ok(ValidatedGraphPatch {
        run_id: run_id.to_string(),
        ops: validated_ops,
    })
}

pub fn commit_graph_patch(
    validated: ValidatedGraphPatch,
    graph_store: &dyn GraphStore,
    state_store: &dyn StateStore,
) -> Result<(), GraphPatchApplyError> {
    for op in validated.ops {
        match op {
            GraphPatchOp::NodeAdded(node_spec) => graph_store
                .add_node(&validated.run_id, node_spec)
                .map_err(GraphPatchApplyError::GraphStore)?,
            GraphPatchOp::EdgeAdded { from, to } => graph_store
                .add_edge(&validated.run_id, &from, &to)
                .map_err(GraphPatchApplyError::GraphStore)?,
            GraphPatchOp::StatePut {
                scope,
                stage_id,
                json_pointer,
                value,
            } => state_store
                .apply_state_put(
                    &validated.run_id,
                    &scope,
                    stage_id.as_deref(),
                    &json_pointer,
                    value,
                )
                .map_err(GraphPatchApplyError::StateStore)?,
            GraphPatchOp::NodeUpdated {
                node_id,
                ttl_ms,
                scheduling,
                htn,
            } => graph_store
                .update_node(&validated.run_id, &node_id, ttl_ms, scheduling, htn)
                .map_err(GraphPatchApplyError::GraphStore)?,
        }
    }
    Ok(())
}

fn parse_node_spec(node: &Value) -> Result<NodeSpec, GraphPatchApplyError> {
    let node_id = node
        .get("node_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| GraphPatchApplyError::InvalidPatch("node.node_id is missing".to_string()))?;
    let kind = node
        .get("kind")
        .and_then(|value| value.as_str())
        .ok_or_else(|| GraphPatchApplyError::InvalidPatch("node.kind is missing".to_string()))?;
    let anchor_position = node
        .get("anchor_position")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            GraphPatchApplyError::InvalidPatch("node.anchor_position is missing".to_string())
        })?;
    let exec = node
        .get("exec")
        .cloned()
        .ok_or_else(|| GraphPatchApplyError::InvalidPatch("node.exec is missing".to_string()))?;
    let deps = node
        .get("deps")
        .and_then(|value| value.as_array())
        .ok_or_else(|| GraphPatchApplyError::InvalidPatch("node.deps is missing".to_string()))?
        .iter()
        .map(|value| {
            value.as_str().map(String::from).ok_or_else(|| {
                GraphPatchApplyError::InvalidPatch("node.deps contains non-string".to_string())
            })
        })
        .collect::<Result<Vec<String>, GraphPatchApplyError>>()?;
    let scheduling = node.get("scheduling").cloned();
    let htn = node.get("htn").cloned();
    let ttl_ms = node.get("ttl_ms").and_then(|value| value.as_u64());
    Ok(NodeSpec {
        node_id: node_id.to_string(),
        kind: kind.to_string(),
        anchor_position: anchor_position.to_string(),
        exec,
        deps,
        scheduling,
        htn,
        ttl_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::core_service::parse_graph_patch_metadata;
    use cork_hash::contract_hash;
    use cork_proto::cork::v1::CanonicalJsonDocument;
    use cork_store::{
        CreateRunInput, InMemoryGraphStore, InMemoryRunRegistry, InMemoryStateStore, RunRegistry,
    };
    use serde_json::json;

    fn build_patch_document(
        run_id: &str,
        patch_seq: u64,
        stage_id: &str,
        ops: serde_json::Value,
    ) -> CanonicalJsonDocument {
        let payload = json!({
            "schema_version": "cork.graph_patch.v0.1",
            "run_id": run_id,
            "patch_seq": patch_seq,
            "stage_id": stage_id,
            "ops": ops,
        });
        CanonicalJsonDocument {
            canonical_json_utf8: serde_json::to_vec(&payload).expect("serialize patch document"),
            sha256: None,
            schema_id: "cork.graph_patch.v0.1".to_string(),
        }
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
                    "exec": {
                        "llm": {
                            "provider": "test",
                            "model": "test",
                            "messages": [ { "role": "user", "parts": [ { "text": "hi" } ] } ]
                        }
                    }
                }
            }
        })
    }

    fn state_put_op(
        scope: &str,
        stage_id: Option<&str>,
        json_pointer: &str,
        value: serde_json::Value,
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

    #[tokio::test]
    async fn apply_patch_updates_hash_bundle() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry.create_run(CreateRunInput::default()).await;
        let contract_digest = contract_hash(b"contract");
        let mut tracker = PatchTracker::new(contract_digest);

        let bundle = tracker.apply_patch(&run_ctx, br#"{"op":"noop"}"#).await;
        let stored = run_ctx.metadata().await.hash_bundle.expect("hash bundle");

        assert_eq!(
            stored
                .contract_manifest_hash
                .as_ref()
                .expect("contract hash")
                .bytes32,
            contract_digest.to_vec()
        );
        assert_eq!(
            stored
                .composite_graph_hash
                .as_ref()
                .expect("composite hash")
                .bytes32,
            bundle
                .composite_graph_hash
                .as_ref()
                .expect("composite hash")
                .bytes32
        );
    }

    #[tokio::test]
    async fn composite_graph_hash_tracks_patch_sequence() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry.create_run(CreateRunInput::default()).await;
        let contract_digest = contract_hash(b"contract");
        let mut tracker = PatchTracker::new(contract_digest);

        tracker.apply_patch(&run_ctx, br#"{"op":"first"}"#).await;
        tracker.apply_patch(&run_ctx, br#"{"op":"second"}"#).await;

        let expected = composite_graph_hash(&contract_digest, tracker.patch_hashes());
        let stored = run_ctx.metadata().await.hash_bundle.expect("hash bundle");
        let stored_composite = stored
            .composite_graph_hash
            .as_ref()
            .expect("composite hash")
            .bytes32
            .clone();

        assert_eq!(stored_composite, expected.to_vec());
    }

    #[tokio::test]
    async fn rejects_patch_seq_gap() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry
            .create_run(CreateRunInput {
                next_patch_seq: Some(1),
                ..Default::default()
            })
            .await;

        let result =
            run_ctx
                .check_patch_seq(0)
                .await
                .map_err(|expected| PatchRejectReason::PatchSeqGap {
                    expected,
                    provided: 0,
                });

        assert_eq!(
            result,
            Err(PatchRejectReason::PatchSeqGap {
                expected: 1,
                provided: 0
            })
        );
    }

    #[test]
    fn rejects_side_effect_without_idempotency_key() {
        let patch = build_patch_document(
            "run-1",
            0,
            "stage-a",
            json!([{
                "op_type": "NODE_ADDED",
                "node_added": {
                    "node": {
                        "node_id": "node-tool-1",
                        "kind": "TOOL",
                        "anchor_position": "WITHIN",
                        "deps": [],
                        "exec": {
                            "tool": {
                                "tool_name": "tool-a",
                                "tool_version": "v1",
                                "input": {
                                    "literal": {
                                        "content_type": "application/json",
                                        "data_base64": "e30="
                                    }
                                },
                                "side_effect": "EXTERNAL_WRITE"
                            }
                        }
                    }
                }
            }]),
        );

        let result = parse_graph_patch_metadata(&patch);
        assert!(matches!(
            result,
            Err(PatchRejectReason::IdempotencyRequired)
        ));
    }

    #[test]
    fn apply_patch_atomicity() {
        let graph_store = InMemoryGraphStore::new();
        let state_store = InMemoryStateStore::new();
        let patch = build_patch_document(
            "run-1",
            0,
            "stage-a",
            json!([
                node_added_op_with_id("node-a", "LLM"),
                state_put_op("RUN", None, "invalid", json!(1))
            ]),
        );

        let result = validate_graph_patch("run-1", &patch, &graph_store, &state_store);

        assert!(result.is_err());
        assert!(graph_store.node("run-1", "node-a").is_none());
        assert!(state_store.run_state("run-1").is_none());
    }
}
