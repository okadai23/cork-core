use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::{GetRunResponse, RunHandle, RunStatus};
use cork_store::{RunCtx, RunRegistry};
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
        active_stage_id: String::new(),
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

pub fn validate_contract_manifest(
    value: &Value,
) -> Result<ValidatedContractManifest, ContractManifestError> {
    cork_schema::validate_contract_manifest(value)
        .map_err(|err| ContractManifestError::Schema(format!("{err:?}")))?;
    let stage_order = validate_contract_graph(value)?;
    Ok(ValidatedContractManifest { stage_order })
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

#[cfg(test)]
mod tests {
    use super::*;
    use cork_store::{CreateRunInput, InMemoryRunRegistry, RunFilters};
    use serde_json::json;

    #[test]
    fn run_ctx_to_response_maps_fields() {
        let registry = InMemoryRunRegistry::new();
        let run = registry.create_run(CreateRunInput {
            experiment_id: Some("exp".to_string()),
            variant_id: Some("var".to_string()),
            status: Some(RunStatus::RunRunning),
            hash_bundle: None,
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
    fn list_runs_filters_in_registry() {
        let registry = InMemoryRunRegistry::new();
        registry.create_run(CreateRunInput {
            experiment_id: Some("exp-1".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunPending),
            hash_bundle: None,
        });
        registry.create_run(CreateRunInput {
            experiment_id: Some("exp-2".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunPending),
            hash_bundle: None,
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
}
