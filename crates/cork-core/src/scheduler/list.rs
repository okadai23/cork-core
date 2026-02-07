use std::cmp::Reverse;
use std::collections::HashMap;

use cork_store::{NodeSpec, StateStore};
use serde_json::Value;

use crate::engine::run::{
    NodeRuntimeState, NodeRuntimeStatus, SchedulerTieBreak, StageRuntimeState,
    ValidatedContractManifest, evaluate_node_readiness,
};
use crate::scheduler::resource::{ResourceAlternative, ResourceRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulingProfile {
    pub priority: i64,
    pub estimated_duration_ms: u64,
    pub resource_requests: Vec<ResourceRequest>,
    pub resource_alternatives: Vec<ResourceAlternative>,
}

impl SchedulingProfile {
    pub fn from_node(node: &NodeSpec) -> Self {
        let mut profile = SchedulingProfile {
            priority: 0,
            estimated_duration_ms: 0,
            resource_requests: Vec::new(),
            resource_alternatives: Vec::new(),
        };
        let Some(scheduling) = node.scheduling.as_ref() else {
            return profile;
        };
        if let Some(priority) = scheduling.get("priority").and_then(|value| value.as_i64()) {
            profile.priority = priority;
        }
        if let Some(duration) = scheduling
            .get("estimated_duration_ms")
            .and_then(|value| value.as_u64())
        {
            profile.estimated_duration_ms = duration;
        }
        if let Some(resources) = scheduling
            .get("resources")
            .and_then(|value| value.as_array())
        {
            profile.resource_requests = resources
                .iter()
                .filter_map(parse_resource_request)
                .collect();
        }
        if let Some(alternatives) = scheduling
            .get("alternatives")
            .and_then(|value| value.as_array())
        {
            profile.resource_alternatives = alternatives
                .iter()
                .filter_map(parse_resource_alternative)
                .collect();
        }
        profile
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadyNode {
    pub node: NodeSpec,
    pub scheduling: SchedulingProfile,
}

pub fn collect_ready_nodes(
    run_id: &str,
    nodes: &[NodeSpec],
    node_states: &mut HashMap<String, NodeRuntimeState>,
    stage_states: &HashMap<String, StageRuntimeState>,
    contract: &ValidatedContractManifest,
    state_store: &dyn StateStore,
) -> Vec<ReadyNode> {
    let mut ready = Vec::new();
    for node in nodes {
        let current = node_states
            .get(&node.node_id)
            .cloned()
            .unwrap_or(NodeRuntimeState {
                status: NodeRuntimeStatus::Pending,
                last_error: None,
            });
        let next = evaluate_node_readiness(
            run_id,
            node,
            &current,
            node_states,
            stage_states,
            contract,
            state_store,
        );
        if next != current {
            node_states.insert(node.node_id.clone(), next.clone());
        }
        if next.status == NodeRuntimeStatus::Ready {
            ready.push(ReadyNode {
                node: node.clone(),
                scheduling: SchedulingProfile::from_node(node),
            });
        }
    }
    ready
}

pub fn order_ready_nodes(nodes: &mut [ReadyNode], tie_break: SchedulerTieBreak) {
    match tie_break {
        SchedulerTieBreak::Fifo => {
            nodes.sort_by(|a, b| a.node.node_id.cmp(&b.node.node_id));
        }
        SchedulerTieBreak::Priority => {
            nodes.sort_by(|a, b| {
                (Reverse(a.scheduling.priority), a.node.node_id.clone())
                    .cmp(&(Reverse(b.scheduling.priority), b.node.node_id.clone()))
            });
        }
        SchedulerTieBreak::ShortestEstimatedFirst => {
            nodes.sort_by(|a, b| {
                (a.scheduling.estimated_duration_ms, a.node.node_id.clone())
                    .cmp(&(b.scheduling.estimated_duration_ms, b.node.node_id.clone()))
            });
        }
        SchedulerTieBreak::LongestEstimatedFirst => {
            nodes.sort_by(|a, b| {
                (
                    Reverse(a.scheduling.estimated_duration_ms),
                    a.node.node_id.clone(),
                )
                    .cmp(&(
                        Reverse(b.scheduling.estimated_duration_ms),
                        b.node.node_id.clone(),
                    ))
            });
        }
    }
}

fn parse_resource_request(value: &Value) -> Option<ResourceRequest> {
    let resource_id = value.get("resource_id")?.as_str()?.to_string();
    let amount = value.get("amount")?.as_u64()?;
    let amount = u32::try_from(amount).ok()?;
    Some(ResourceRequest {
        resource_id,
        amount,
    })
}

fn parse_resource_alternative(value: &Value) -> Option<ResourceAlternative> {
    let resource_id = value.get("resource_id")?.as_str()?.to_string();
    let estimated_duration_ms = value.get("estimated_duration_ms")?.as_u64()?;
    Some(ResourceAlternative {
        resource_id,
        estimated_duration_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::run::{StageRuntimeState, StageRuntimeStatus, ValidatedContractManifest};
    use cork_store::InMemoryStateStore;
    use serde_json::json;

    fn node_spec(node_id: &str, scheduling: Value) -> NodeSpec {
        NodeSpec {
            node_id: node_id.to_string(),
            kind: "TOOL".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec: json!({
                "tool": {
                    "tool_name": "tool",
                    "tool_version": "v1",
                    "side_effect": "NONE",
                    "input": { "literal": { "content_type": "application/json", "data_base64": "e30=" } }
                }
            }),
            deps: Vec::new(),
            scheduling: Some(scheduling),
            htn: None,
            ttl_ms: None,
        }
    }

    fn ready_nodes_with_tie_break(tie_break: SchedulerTieBreak) -> Vec<String> {
        let nodes = vec![
            node_spec(
                "stage-a/node-a",
                json!({ "priority": 1, "estimated_duration_ms": 50 }),
            ),
            node_spec(
                "stage-a/node-b",
                json!({ "priority": 3, "estimated_duration_ms": 10 }),
            ),
            node_spec(
                "stage-a/node-c",
                json!({ "priority": 2, "estimated_duration_ms": 100 }),
            ),
        ];
        let mut node_states = HashMap::new();
        for node in &nodes {
            node_states.insert(
                node.node_id.clone(),
                NodeRuntimeState {
                    status: NodeRuntimeStatus::Pending,
                    last_error: None,
                },
            );
        }
        let stage_states = HashMap::from([(
            "stage-a".to_string(),
            StageRuntimeState {
                status: StageRuntimeStatus::Committed,
            },
        )]);
        let contract = ValidatedContractManifest {
            stage_order: vec!["stage-a".to_string()],
        };
        let store = InMemoryStateStore::new();
        let mut ready = collect_ready_nodes(
            "run-1",
            &nodes,
            &mut node_states,
            &stage_states,
            &contract,
            &store,
        );
        order_ready_nodes(&mut ready, tie_break);
        ready.into_iter().map(|node| node.node.node_id).collect()
    }

    #[test]
    fn tie_break_changes_ready_node_order() {
        assert_eq!(
            ready_nodes_with_tie_break(SchedulerTieBreak::Fifo),
            vec!["stage-a/node-a", "stage-a/node-b", "stage-a/node-c"]
        );
        assert_eq!(
            ready_nodes_with_tie_break(SchedulerTieBreak::Priority),
            vec!["stage-a/node-b", "stage-a/node-c", "stage-a/node-a"]
        );
        assert_eq!(
            ready_nodes_with_tie_break(SchedulerTieBreak::ShortestEstimatedFirst),
            vec!["stage-a/node-b", "stage-a/node-a", "stage-a/node-c"]
        );
        assert_eq!(
            ready_nodes_with_tie_break(SchedulerTieBreak::LongestEstimatedFirst),
            vec!["stage-a/node-c", "stage-a/node-a", "stage-a/node-b"]
        );
    }
}
