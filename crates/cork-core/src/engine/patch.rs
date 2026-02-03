use cork_hash::{composite_graph_hash, patch_hash};
use cork_proto::cork::v1::{CanonicalJsonDocument, HashBundle, Sha256};
use cork_store::{GraphStore, GraphStoreError, NodeSpec, RunCtx, StateStore, StateStoreError};
use serde_json::Value;

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
}

#[derive(Debug, Clone)]
pub struct PatchTracker {
    contract_hash: [u8; 32],
    patch_hashes: Vec<[u8; 32]>,
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

    pub fn apply_patch(&mut self, run_ctx: &RunCtx, patch_bytes: &[u8]) -> HashBundle {
        let digest = patch_hash(patch_bytes);
        self.patch_hashes.push(digest);
        run_ctx.advance_patch_seq();
        self.update_hash_bundle(run_ctx)
    }

    fn update_hash_bundle(&self, run_ctx: &RunCtx) -> HashBundle {
        let composite = composite_graph_hash(&self.contract_hash, &self.patch_hashes);
        let existing = run_ctx.metadata().hash_bundle;
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
        run_ctx.set_hash_bundle(Some(bundle.clone()));
        bundle
    }
}

fn bytes_to_sha256(bytes: &[u8; 32]) -> Sha256 {
    Sha256 {
        bytes32: bytes.to_vec(),
    }
}

pub fn apply_graph_patch_ops(
    run_id: &str,
    patch: &CanonicalJsonDocument,
    graph_store: &dyn GraphStore,
    state_store: &dyn StateStore,
) -> Result<(), GraphPatchApplyError> {
    let value: Value = serde_json::from_slice(&patch.canonical_json_utf8)
        .map_err(|err| GraphPatchApplyError::InvalidPatch(format!("invalid patch json: {err}")))?;
    let ops = value
        .get("ops")
        .and_then(|value| value.as_array())
        .ok_or_else(|| GraphPatchApplyError::InvalidPatch("ops is missing".to_string()))?;
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
                graph_store
                    .add_node(run_id, node_spec)
                    .map_err(GraphPatchApplyError::GraphStore)?;
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
                graph_store
                    .add_edge(run_id, from, to)
                    .map_err(GraphPatchApplyError::GraphStore)?;
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
                state_store
                    .apply_state_put(run_id, scope, stage_id, json_pointer, value)
                    .map_err(GraphPatchApplyError::StateStore)?;
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
                graph_store
                    .update_node(run_id, node_id, ttl_ms, scheduling, htn)
                    .map_err(GraphPatchApplyError::GraphStore)?;
            }
            other => {
                return Err(GraphPatchApplyError::InvalidPatch(format!(
                    "unsupported op_type {other}"
                )));
            }
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
    use cork_hash::contract_hash;
    use cork_store::{CreateRunInput, InMemoryRunRegistry, RunRegistry};

    #[test]
    fn apply_patch_updates_hash_bundle() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry.create_run(CreateRunInput::default());
        let contract_digest = contract_hash(b"contract");
        let mut tracker = PatchTracker::new(contract_digest);

        let bundle = tracker.apply_patch(&run_ctx, br#"{"op":"noop"}"#);
        let stored = run_ctx.metadata().hash_bundle.expect("hash bundle");

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

    #[test]
    fn composite_graph_hash_tracks_patch_sequence() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry.create_run(CreateRunInput::default());
        let contract_digest = contract_hash(b"contract");
        let mut tracker = PatchTracker::new(contract_digest);

        tracker.apply_patch(&run_ctx, br#"{"op":"first"}"#);
        tracker.apply_patch(&run_ctx, br#"{"op":"second"}"#);

        let expected = composite_graph_hash(&contract_digest, tracker.patch_hashes());
        let stored = run_ctx.metadata().hash_bundle.expect("hash bundle");
        let stored_composite = stored
            .composite_graph_hash
            .as_ref()
            .expect("composite hash")
            .bytes32
            .clone();

        assert_eq!(stored_composite, expected.to_vec());
    }
}
