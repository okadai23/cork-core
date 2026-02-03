use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::{GetRunResponse, RunHandle, RunStatus};
use cork_store::{
    ArtifactRef, NodeOutput, NodePayload, RunCtx, RunRegistry, StageAutoCommitPolicy, StateStore,
};
use prost_types::Timestamp;
use serde_json::Value;

pub fn run_ctx_to_response(run_ctx: &Arc<RunCtx>) -> GetRunResponse {
    let metadata = run_ctx.metadata();
    GetRunResponse {
        handle: Some(RunHandle {
            run_id: run_ctx.run_id().to_string(),
        }),
        status: metadata.status as i32,
        created_at: Some(system_time_to_timestamp(metadata.created_at)),
        updated_at: Some(system_time_to_timestamp(metadata.updated_at)),
        hashes: metadata.hash_bundle,
        active_stage_id: metadata.active_stage_id.unwrap_or_default(),
    }
}

pub fn get_run_response(registry: &dyn RunRegistry, run_id: &str) -> Option<GetRunResponse> {
    let run_ctx = registry.get_run(run_id)?;
    Some(run_ctx_to_response(&run_ctx))
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

pub fn map_proto_status(status: i32) -> Option<RunStatus> {
    RunStatus::try_from(status).ok()
}

pub fn record_node_output(
    state_store: &dyn StateStore,
    run_id: &str,
    node_id: &str,
    payload: NodePayload,
    artifacts: Vec<ArtifactRef>,
) -> NodeOutput {
    let output = NodeOutput::from_payload(payload, artifacts);
    state_store.set_node_output(run_id, node_id, output.clone());
    output
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedContractManifest {
    pub stage_order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractManifestError {
    Schema(String),
    DuplicateStageId(String),
    MissingStageDependency {
        stage_id: String,
        dependency_id: String,
    },
    CycleDetected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerMode {
    ListHeuristic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourcePoolKind {
    Cumulative,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourcePoolSpec {
    pub resource_id: String,
    pub capacity: u64,
    pub kind: ResourcePoolKind,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPolicy {
    pub resource_pools: Vec<ResourcePoolSpec>,
    pub scheduler_mode: SchedulerMode,
    pub stage_auto_commit: StageAutoCommitPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    Schema(String),
    DuplicateResourceId(String),
    UnsupportedSchedulerMode(String),
}

pub fn validate_contract_manifest(
    value: &Value,
) -> Result<ValidatedContractManifest, ContractManifestError> {
    cork_schema::validate_contract_manifest(value)
        .map_err(|err| ContractManifestError::Schema(format!("{err:?}")))?;
    let stage_order = validate_contract_graph(value)?;
    Ok(ValidatedContractManifest { stage_order })
}

pub fn validate_policy(value: &Value) -> Result<ValidatedPolicy, PolicyError> {
    cork_schema::validate_policy(value).map_err(|err| PolicyError::Schema(format!("{err:?}")))?;
    let resource_pools = parse_resource_pools(value)?;
    let scheduler_mode = parse_scheduler_mode(value)?;
    let stage_auto_commit = parse_stage_auto_commit(value)?;
    Ok(ValidatedPolicy {
        resource_pools,
        scheduler_mode,
        stage_auto_commit,
    })
}

fn validate_contract_graph(value: &Value) -> Result<Vec<String>, ContractManifestError> {
    let stages = value
        .get("contract_graph")
        .and_then(|graph| graph.get("stages"))
        .and_then(|stages| stages.as_array())
        .ok_or_else(|| {
            ContractManifestError::Schema("contract_graph.stages is missing".to_string())
        })?;

    let mut stage_ids = Vec::with_capacity(stages.len());
    let mut stage_set = HashSet::with_capacity(stages.len());
    for stage in stages {
        let stage_id = stage
            .get("stage_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| ContractManifestError::Schema("stage_id is missing".to_string()))?;
        if !stage_set.insert(stage_id.to_string()) {
            return Err(ContractManifestError::DuplicateStageId(
                stage_id.to_string(),
            ));
        }
        stage_ids.push(stage_id.to_string());
    }

    for stage in stages {
        let stage_id = stage
            .get("stage_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| ContractManifestError::Schema("stage_id is missing".to_string()))?;
        if let Some(dependencies) = stage.get("dependencies").and_then(|deps| deps.as_array()) {
            for dependency in dependencies {
                let dependency_id = dependency
                    .get("stage_id")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        ContractManifestError::Schema("dependency.stage_id is missing".to_string())
                    })?;
                if !stage_set.contains(dependency_id) {
                    return Err(ContractManifestError::MissingStageDependency {
                        stage_id: stage_id.to_string(),
                        dependency_id: dependency_id.to_string(),
                    });
                }
            }
        }
    }

    let mut indegree: HashMap<String, usize> = stage_ids
        .iter()
        .map(|stage_id| (stage_id.clone(), 0))
        .collect();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

    for stage in stages {
        let stage_id = stage
            .get("stage_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| ContractManifestError::Schema("stage_id is missing".to_string()))?;
        if let Some(dependencies) = stage.get("dependencies").and_then(|deps| deps.as_array()) {
            for dependency in dependencies {
                let dependency_id = dependency
                    .get("stage_id")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        ContractManifestError::Schema("dependency.stage_id is missing".to_string())
                    })?;
                adjacency
                    .entry(dependency_id.to_string())
                    .or_default()
                    .push(stage_id.to_string());
                if let Some(count) = indegree.get_mut(stage_id) {
                    *count += 1;
                }
            }
        }
    }

    let mut queue: VecDeque<String> = indegree
        .iter()
        .filter_map(|(stage_id, count)| {
            if *count == 0 {
                Some(stage_id.clone())
            } else {
                None
            }
        })
        .collect();
    let mut order = Vec::with_capacity(stage_ids.len());

    while let Some(stage_id) = queue.pop_front() {
        order.push(stage_id.clone());
        if let Some(neighbors) = adjacency.get(&stage_id) {
            for neighbor in neighbors {
                if let Some(count) = indegree.get_mut(neighbor) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push_back(neighbor.clone());
                    }
                }
            }
        }
    }

    if order.len() != stage_ids.len() {
        return Err(ContractManifestError::CycleDetected);
    }

    Ok(order)
}

fn parse_resource_pools(value: &Value) -> Result<Vec<ResourcePoolSpec>, PolicyError> {
    let pools = value
        .get("resource_pools")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let mut seen = HashSet::new();
    let mut specs = Vec::with_capacity(pools.len());

    for pool in pools {
        let pool_obj = pool.as_object().ok_or_else(|| {
            PolicyError::Schema("resource_pools entry must be object".to_string())
        })?;
        let resource_id = pool_obj
            .get("resource_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| PolicyError::Schema("resource_id is missing".to_string()))?;
        if !seen.insert(resource_id.to_string()) {
            return Err(PolicyError::DuplicateResourceId(resource_id.to_string()));
        }
        let capacity = pool_obj
            .get("capacity")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| PolicyError::Schema("capacity is missing".to_string()))?;
        let kind = match pool_obj
            .get("kind")
            .and_then(|value| value.as_str())
            .ok_or_else(|| PolicyError::Schema("kind is missing".to_string()))?
        {
            "CUMULATIVE" => ResourcePoolKind::Cumulative,
            "EXCLUSIVE" => ResourcePoolKind::Exclusive,
            other => return Err(PolicyError::Schema(format!("unknown kind {other}"))),
        };
        let tags = pool_obj
            .get("tags")
            .and_then(|value| value.as_array())
            .map(|tags| {
                tags.iter()
                    .map(|tag| {
                        tag.as_str().map(str::to_string).ok_or_else(|| {
                            PolicyError::Schema("resource_pools.tags must be strings".to_string())
                        })
                    })
                    .collect::<Result<Vec<String>, PolicyError>>()
            })
            .transpose()?
            .unwrap_or_default();
        specs.push(ResourcePoolSpec {
            resource_id: resource_id.to_string(),
            capacity,
            kind,
            tags,
        });
    }

    specs.sort_by(|a, b| a.resource_id.cmp(&b.resource_id));
    Ok(specs)
}

fn parse_scheduler_mode(value: &Value) -> Result<SchedulerMode, PolicyError> {
    let mode = value
        .get("scheduler")
        .and_then(|scheduler| scheduler.get("mode"))
        .and_then(|mode| mode.as_str())
        .ok_or_else(|| PolicyError::Schema("scheduler.mode is missing".to_string()))?;
    match mode {
        "LIST_HEURISTIC" => Ok(SchedulerMode::ListHeuristic),
        other => Err(PolicyError::UnsupportedSchedulerMode(other.to_string())),
    }
}

fn parse_stage_auto_commit(value: &Value) -> Result<StageAutoCommitPolicy, PolicyError> {
    let auto_commit = value
        .get("stage_auto_commit")
        .and_then(|value| value.as_object())
        .ok_or_else(|| PolicyError::Schema("stage_auto_commit is missing".to_string()))?;
    let enabled = auto_commit
        .get("enabled")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| PolicyError::Schema("stage_auto_commit.enabled is missing".to_string()))?;
    let quiescence_ms = auto_commit
        .get("quiescence_ms")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| {
            PolicyError::Schema("stage_auto_commit.quiescence_ms is missing".to_string())
        })?;
    let max_open_ms = auto_commit
        .get("max_open_ms")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| {
            PolicyError::Schema("stage_auto_commit.max_open_ms is missing".to_string())
        })?;
    let exclude_when_waiting = auto_commit
        .get("exclude_when_waiting")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| {
            PolicyError::Schema("stage_auto_commit.exclude_when_waiting is missing".to_string())
        })?;
    Ok(StageAutoCommitPolicy {
        enabled,
        quiescence_ms,
        max_open_ms,
        exclude_when_waiting,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cork_store::{CreateRunInput, InMemoryRunRegistry, InMemoryStateStore, RunFilters};
    use serde_json::json;

    #[test]
    fn run_ctx_to_response_maps_fields() {
        let registry = InMemoryRunRegistry::new();
        let run = registry.create_run(CreateRunInput {
            experiment_id: Some("exp".to_string()),
            variant_id: Some("var".to_string()),
            status: Some(RunStatus::RunRunning),
            hash_bundle: None,
            stage_auto_commit: None,
            next_patch_seq: None,
            active_stage_id: None,
            active_stage_expansion_policy: None,
        });

        let response = run_ctx_to_response(&run);
        assert_eq!(response.handle.unwrap().run_id, run.run_id());
        assert_eq!(response.status, RunStatus::RunRunning as i32);
        assert!(response.created_at.is_some());
        assert!(response.updated_at.is_some());
    }

    #[test]
    fn get_run_response_returns_none_when_missing() {
        let registry = InMemoryRunRegistry::new();
        assert!(get_run_response(&registry, "missing").is_none());
    }

    #[test]
    fn map_proto_status_accepts_valid_values() {
        assert_eq!(
            map_proto_status(RunStatus::RunPending as i32),
            Some(RunStatus::RunPending)
        );
        assert!(map_proto_status(999).is_none());
    }

    #[test]
    fn record_node_output_stores_payload_and_parsed_json() {
        let store = InMemoryStateStore::new();
        let output = record_node_output(
            &store,
            "run-1",
            "node-1",
            NodePayload {
                content_type: "application/json".to_string(),
                data: br#"{"value": 1}"#.to_vec(),
            },
            Vec::new(),
        );

        assert!(output.is_json());
        assert_eq!(output.parsed_json, Some(json!({ "value": 1 })));
        let stored = store.node_output("run-1", "node-1").expect("stored output");
        assert_eq!(stored, output);
    }

    #[test]
    fn list_runs_filters_in_registry() {
        let registry = InMemoryRunRegistry::new();
        registry.create_run(CreateRunInput {
            experiment_id: Some("exp-1".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunPending),
            hash_bundle: None,
            stage_auto_commit: None,
            next_patch_seq: None,
            active_stage_id: None,
            active_stage_expansion_policy: None,
        });
        registry.create_run(CreateRunInput {
            experiment_id: Some("exp-2".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunPending),
            hash_bundle: None,
            stage_auto_commit: None,
            next_patch_seq: None,
            active_stage_id: None,
            active_stage_expansion_policy: None,
        });

        let page = registry.list_runs(
            None,
            RunFilters {
                experiment_id: Some("exp-1".to_string()),
                variant_id: None,
                status: Some(RunStatus::RunPending),
            },
            10,
        );
        assert_eq!(page.runs.len(), 1);
        assert_eq!(
            page.runs[0].metadata().experiment_id.as_deref(),
            Some("exp-1")
        );
    }

    fn build_manifest(stages: serde_json::Value) -> serde_json::Value {
        json!({
            "schema_version": "cork.contract_manifest.v0.1",
            "manifest_id": "manifest-1234",
            "contract_graph": {
                "stages": stages
            },
            "defaults": {
                "expansion_policy": {
                    "allow_dynamic": true,
                    "allow_kinds": [],
                    "max_dynamic_nodes": 0,
                    "max_steps_in_stage": 0,
                    "allow_cross_stage_deps": "NONE"
                },
                "stage_budget": {},
                "stage_ttl": {},
                "completion_policy": {
                    "fail_on_any_failure": false,
                    "require_commit": true
                }
            }
        })
    }

    #[test]
    fn validate_contract_manifest_builds_topological_order() {
        let manifest = build_manifest(json!([
            {
                "stage_id": "stage_a",
                "dependencies": [
                    { "stage_id": "stage_b", "constraint": "HARD" }
                ]
            },
            { "stage_id": "stage_b" }
        ]));

        let validated = validate_contract_manifest(&manifest).expect("valid manifest");
        assert_eq!(validated.stage_order, vec!["stage_b", "stage_a"]);
    }

    #[test]
    fn validate_contract_manifest_rejects_duplicate_stage_ids() {
        let manifest = build_manifest(json!([
            { "stage_id": "stage_a" },
            { "stage_id": "stage_a" }
        ]));

        let result = validate_contract_manifest(&manifest);
        assert!(matches!(
            result,
            Err(ContractManifestError::DuplicateStageId(id)) if id == "stage_a"
        ));
    }

    #[test]
    fn validate_contract_manifest_rejects_missing_dependencies() {
        let manifest = build_manifest(json!([
            {
                "stage_id": "stage_a",
                "dependencies": [
                    { "stage_id": "missing_stage", "constraint": "HARD" }
                ]
            }
        ]));

        let result = validate_contract_manifest(&manifest);
        assert!(matches!(
            result,
            Err(ContractManifestError::MissingStageDependency { stage_id, dependency_id })
                if stage_id == "stage_a" && dependency_id == "missing_stage"
        ));
    }

    #[test]
    fn validate_contract_manifest_rejects_cycles() {
        let manifest = build_manifest(json!([
            {
                "stage_id": "stage_a",
                "dependencies": [
                    { "stage_id": "stage_b", "constraint": "HARD" }
                ]
            },
            {
                "stage_id": "stage_b",
                "dependencies": [
                    { "stage_id": "stage_a", "constraint": "HARD" }
                ]
            }
        ]));

        let result = validate_contract_manifest(&manifest);
        assert!(matches!(result, Err(ContractManifestError::CycleDetected)));
    }

    fn build_policy() -> serde_json::Value {
        json!({
            "schema_version": "cork.policy.v0.1",
            "policy_id": "policy-1234",
            "run_budget": {
                "wall_ms": 1000,
                "deadline_mode": "RELATIVE",
                "deadline_ms": 1000,
                "tokens_in_max": 100,
                "tokens_out_max": 0,
                "cost_usd_max": 0.0,
                "max_steps": 1,
                "max_dynamic_nodes": 0,
                "history_max_events": 100,
                "history_max_bytes": 1024
            },
            "concurrency": {
                "io_max": 1,
                "cpu_max": 1,
                "per_stage_max": 1,
                "per_provider_max": 1
            },
            "retry": {
                "max_attempts": 0,
                "backoff": {
                    "policy": "CONSTANT",
                    "base_delay_ms": 0,
                    "max_delay_ms": 0,
                    "jitter": "NONE"
                },
                "retry_on": [],
                "respect_retry_after": false,
                "circuit_breaker": {
                    "enabled": false,
                    "open_after_failures": 1,
                    "half_open_after_ms": 100
                }
            },
            "rate_limit": {
                "providers": []
            },
            "idle": {
                "heartbeat_interval_ms": 100,
                "idle_timeout_ms": 1000
            },
            "guardrails": {
                "store_prompts": "none",
                "store_completions": "none",
                "store_tool_io": "none",
                "pre_call": [],
                "during_call": [],
                "post_call": [],
                "on_violation": {
                    "action": "BLOCK",
                    "max_retries": 0
                }
            },
            "context": {
                "token_counter": {
                    "impl": "default",
                    "strict_mode": false
                },
                "compression": {
                    "enabled": false,
                    "steps": []
                }
            },
            "cache": {
                "enabled": false,
                "default_ttl_ms": 0,
                "rules": []
            },
            "loop_detection": {
                "max_steps": 1,
                "max_dynamic_nodes": 0,
                "no_progress": {
                    "enabled": false,
                    "repeat_threshold": 2,
                    "fingerprint_keys": []
                }
            },
            "retention": {
                "event_log_ttl_ms": 0,
                "artifact_ttl_ms": 0,
                "trace_ttl_ms": 0
            },
            "scheduler": {
                "mode": "LIST_HEURISTIC",
                "tie_break": "FIFO"
            },
            "stage_auto_commit": {
                "enabled": true,
                "quiescence_ms": 10,
                "max_open_ms": 20,
                "exclude_when_waiting": false
            },
            "resource_pools": [
                {
                    "resource_id": "pool-2",
                    "capacity": 2,
                    "kind": "EXCLUSIVE",
                    "tags": ["gpu"]
                },
                {
                    "resource_id": "pool-1",
                    "capacity": 1,
                    "kind": "CUMULATIVE"
                }
            ]
        })
    }

    #[test]
    fn validate_policy_normalizes_resource_pools() {
        let policy = build_policy();
        let validated = validate_policy(&policy).expect("valid policy");
        assert_eq!(validated.scheduler_mode, SchedulerMode::ListHeuristic);
        assert_eq!(
            validated.stage_auto_commit,
            StageAutoCommitPolicy {
                enabled: true,
                quiescence_ms: 10,
                max_open_ms: 20,
                exclude_when_waiting: false,
            }
        );
        assert_eq!(
            validated.resource_pools,
            vec![
                ResourcePoolSpec {
                    resource_id: "pool-1".to_string(),
                    capacity: 1,
                    kind: ResourcePoolKind::Cumulative,
                    tags: vec![],
                },
                ResourcePoolSpec {
                    resource_id: "pool-2".to_string(),
                    capacity: 2,
                    kind: ResourcePoolKind::Exclusive,
                    tags: vec!["gpu".to_string()],
                },
            ]
        );
    }

    #[test]
    fn validate_policy_rejects_duplicate_resource_ids() {
        let mut policy = build_policy();
        policy["resource_pools"] = json!([
            { "resource_id": "pool-1", "capacity": 1, "kind": "CUMULATIVE" },
            { "resource_id": "pool-1", "capacity": 2, "kind": "EXCLUSIVE" }
        ]);
        let result = validate_policy(&policy);
        assert!(matches!(
            result,
            Err(PolicyError::DuplicateResourceId(id)) if id == "pool-1"
        ));
    }

    #[test]
    fn validate_policy_rejects_unsupported_scheduler_mode() {
        let mut policy = build_policy();
        policy["scheduler"]["mode"] = json!("OPTIMIZE_CP_SAT");
        let result = validate_policy(&policy);
        assert!(matches!(
            result,
            Err(PolicyError::UnsupportedSchedulerMode(mode)) if mode == "OPTIMIZE_CP_SAT"
        ));
    }
}
