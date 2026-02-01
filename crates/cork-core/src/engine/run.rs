use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::{GetRunResponse, RunHandle, RunStatus};
use cork_store::{RunCtx, RunRegistry};
use prost_types::Timestamp;

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

#[cfg(test)]
mod tests {
    use super::*;
    use cork_store::{CreateRunInput, InMemoryRunRegistry, RunFilters};

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
}
