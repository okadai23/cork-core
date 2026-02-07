use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::{RunEvent, StageLifecycleEvent, run_event};
use cork_store::{EventLog, GraphStore, RunCtx};
use prost_types::Timestamp;

use super::run::{
    NodeRuntimeState, NodeRuntimeStatus, StageRuntimeState, StageRuntimeStatus,
    ValidatedContractManifest, node_stage_id,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoCommitReason {
    Quiescence,
    MaxOpen,
}

impl AutoCommitReason {
    fn as_str(&self) -> &'static str {
        match self {
            AutoCommitReason::Quiescence => "auto_commit_quiescence",
            AutoCommitReason::MaxOpen => "auto_commit_max_open",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoCommitResult {
    pub committed_stage_id: String,
    pub next_stage_id: Option<String>,
    pub reason: AutoCommitReason,
}

pub struct StageAutoCommitSupervisor {
    ticker: tokio::time::Interval,
}

impl StageAutoCommitSupervisor {
    pub fn new(interval: Duration) -> Self {
        Self {
            ticker: tokio::time::interval(interval),
        }
    }

    pub async fn tick(&mut self) {
        self.ticker.tick().await;
    }
}

pub async fn tick_stage_auto_commit(
    run_ctx: &Arc<RunCtx>,
    contract: &ValidatedContractManifest,
    node_states: &HashMap<String, NodeRuntimeState>,
    stage_states: &mut HashMap<String, StageRuntimeState>,
    graph_store: &dyn GraphStore,
    event_log: &dyn EventLog,
    now: SystemTime,
) -> Option<AutoCommitResult> {
    let policy = run_ctx.stage_auto_commit().await?;
    if !policy.enabled {
        return None;
    }
    let active_stage_id = run_ctx.active_stage_id().await?;
    let stage_state = stage_states.get(&active_stage_id)?;
    if stage_state.status != StageRuntimeStatus::Active {
        return None;
    }
    let stage_started_at = ensure_stage_started(run_ctx, now).await;
    let last_patch_at = ensure_last_patch_at(run_ctx, stage_started_at).await;
    let max_open_exceeded =
        elapsed(now, stage_started_at) > Duration::from_millis(policy.max_open_ms);
    let quiescent = elapsed(now, last_patch_at) >= Duration::from_millis(policy.quiescence_ms);
    let has_durable_wait = policy.exclude_when_waiting
        && stage_has_durable_wait(run_ctx, &active_stage_id, node_states, graph_store);
    if has_durable_wait {
        return None;
    }
    let pre_within_terminal =
        stage_pre_within_terminal(run_ctx, &active_stage_id, node_states, graph_store);
    if !max_open_exceeded && (!quiescent || !pre_within_terminal) {
        return None;
    }
    let reason = if max_open_exceeded {
        AutoCommitReason::MaxOpen
    } else {
        AutoCommitReason::Quiescence
    };
    apply_commit_transition(
        run_ctx,
        contract,
        stage_states,
        event_log,
        now,
        &active_stage_id,
        reason.clone(),
    )
    .await
}

async fn ensure_stage_started(run_ctx: &RunCtx, now: SystemTime) -> SystemTime {
    if let Some(started_at) = run_ctx.stage_started_at().await {
        started_at
    } else {
        run_ctx.set_stage_started_at(Some(now)).await;
        now
    }
}

async fn ensure_last_patch_at(run_ctx: &RunCtx, stage_started_at: SystemTime) -> SystemTime {
    if let Some(last_patch_at) = run_ctx.last_patch_at().await {
        last_patch_at
    } else {
        run_ctx.set_last_patch_at(Some(stage_started_at)).await;
        stage_started_at
    }
}

fn elapsed(now: SystemTime, then: SystemTime) -> Duration {
    now.duration_since(then).unwrap_or_default()
}

fn stage_pre_within_terminal(
    run_ctx: &RunCtx,
    stage_id: &str,
    node_states: &HashMap<String, NodeRuntimeState>,
    graph_store: &dyn GraphStore,
) -> bool {
    let run_id = run_ctx.run_id();
    for (node_id, state) in node_states
        .iter()
        .filter(|(node_id, _)| node_stage_id(node_id.as_str()) == Some(stage_id))
    {
        let Some(node) = graph_store.node(run_id, node_id) else {
            return false;
        };
        if is_pre_within(&node.anchor_position) && !is_terminal(&state.status) {
            return false;
        }
    }
    true
}

fn stage_has_durable_wait(
    run_ctx: &RunCtx,
    stage_id: &str,
    node_states: &HashMap<String, NodeRuntimeState>,
    graph_store: &dyn GraphStore,
) -> bool {
    let run_id = run_ctx.run_id();
    node_states
        .keys()
        .filter(|node_id| node_stage_id(node_id.as_str()) == Some(stage_id))
        .filter_map(|node_id| graph_store.node(run_id, node_id))
        .any(|node| node.kind.eq_ignore_ascii_case("durable_wait"))
}

fn is_pre_within(anchor_position: &str) -> bool {
    anchor_position.eq_ignore_ascii_case("PRE") || anchor_position.eq_ignore_ascii_case("WITHIN")
}

fn is_terminal(status: &NodeRuntimeStatus) -> bool {
    matches!(
        status,
        NodeRuntimeStatus::Succeeded
            | NodeRuntimeStatus::Failed
            | NodeRuntimeStatus::Skipped
            | NodeRuntimeStatus::Cancelled
            | NodeRuntimeStatus::TimedOut
            | NodeRuntimeStatus::Expired
            | NodeRuntimeStatus::Stale
    )
}

async fn apply_commit_transition(
    run_ctx: &Arc<RunCtx>,
    contract: &ValidatedContractManifest,
    stage_states: &mut HashMap<String, StageRuntimeState>,
    event_log: &dyn EventLog,
    now: SystemTime,
    active_stage_id: &str,
    reason: AutoCommitReason,
) -> Option<AutoCommitResult> {
    stage_states.insert(
        active_stage_id.to_string(),
        StageRuntimeState {
            status: StageRuntimeStatus::Committed,
        },
    );
    let position = contract
        .stage_order
        .iter()
        .position(|stage| stage == active_stage_id)?;
    let next_stage_id = contract.stage_order.get(position + 1).cloned();
    if let Some(ref next_stage) = next_stage_id {
        stage_states.insert(
            next_stage.to_string(),
            StageRuntimeState {
                status: StageRuntimeStatus::Active,
            },
        );
        run_ctx
            .set_active_stage(
                Some(next_stage.to_string()),
                run_ctx.active_stage_expansion_policy().await,
            )
            .await;
    } else {
        run_ctx.set_active_stage(None, None).await;
    }
    let event = RunEvent {
        event_seq: 0,
        ts: Some(system_time_to_timestamp(now)),
        event: Some(run_event::Event::Stage(StageLifecycleEvent {
            stage_id: active_stage_id.to_string(),
            from_state: "ACTIVE".to_string(),
            to_state: "COMMITTED".to_string(),
            reason: reason.as_str().to_string(),
        })),
    };
    event_log.append(event).await;
    Some(AutoCommitResult {
        committed_stage_id: active_stage_id.to_string(),
        next_stage_id,
        reason,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use cork_store::{
        CreateRunInput, GraphStore, InMemoryEventLog, InMemoryGraphStore, InMemoryRunRegistry,
        RunRegistry, StageAutoCommitPolicy,
    };
    use serde_json::json;

    fn build_node(node_id: &str, anchor_position: &str, kind: &str) -> cork_store::NodeSpec {
        cork_store::NodeSpec {
            node_id: node_id.to_string(),
            kind: kind.to_string(),
            anchor_position: anchor_position.to_string(),
            exec: json!({}),
            deps: Vec::new(),
            scheduling: None,
            htn: None,
            ttl_ms: None,
        }
    }

    #[tokio::test]
    async fn auto_commit_advances_stage_on_quiescence() {
        let registry = InMemoryRunRegistry::new();
        let run = registry
            .create_run(CreateRunInput {
                stage_auto_commit: Some(StageAutoCommitPolicy {
                    enabled: true,
                    quiescence_ms: 10,
                    max_open_ms: 1000,
                    exclude_when_waiting: false,
                }),
                active_stage_id: Some("stage-a".to_string()),
                ..Default::default()
            })
            .await;
        run.set_stage_started_at(Some(SystemTime::now() - Duration::from_millis(50)))
            .await;
        run.set_last_patch_at(Some(SystemTime::now() - Duration::from_millis(20)))
            .await;
        let graph_store = InMemoryGraphStore::new();
        graph_store
            .add_node(run.run_id(), build_node("stage-a/node-a", "PRE", "tool"))
            .expect("add node");
        let node_states = HashMap::from([(
            "stage-a/node-a".to_string(),
            NodeRuntimeState {
                status: NodeRuntimeStatus::Succeeded,
                last_error: None,
            },
        )]);
        let mut stage_states = HashMap::from([(
            "stage-a".to_string(),
            StageRuntimeState {
                status: StageRuntimeStatus::Active,
            },
        )]);
        let contract = ValidatedContractManifest {
            stage_order: vec!["stage-a".to_string(), "stage-b".to_string()],
        };
        let event_log = InMemoryEventLog::new();

        let result = tick_stage_auto_commit(
            &run,
            &contract,
            &node_states,
            &mut stage_states,
            &graph_store,
            &event_log,
            SystemTime::now(),
        )
        .await
        .expect("commit should occur");

        assert_eq!(result.committed_stage_id, "stage-a");
        assert_eq!(result.next_stage_id.as_deref(), Some("stage-b"));
        assert_eq!(run.active_stage_id().await.as_deref(), Some("stage-b"));
        assert_eq!(
            stage_states.get("stage-a").expect("stage-a state").status,
            StageRuntimeStatus::Committed
        );
        assert_eq!(
            stage_states.get("stage-b").expect("stage-b state").status,
            StageRuntimeStatus::Active
        );
        let events = event_log.subscribe(0).await.backlog;
        assert_eq!(events.len(), 1);
        match events[0].event.as_ref().expect("event") {
            run_event::Event::Stage(stage_event) => {
                assert_eq!(stage_event.stage_id, "stage-a");
                assert_eq!(stage_event.reason, "auto_commit_quiescence");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn auto_commit_forces_on_max_open() {
        let registry = InMemoryRunRegistry::new();
        let run = registry
            .create_run(CreateRunInput {
                stage_auto_commit: Some(StageAutoCommitPolicy {
                    enabled: true,
                    quiescence_ms: 1000,
                    max_open_ms: 5,
                    exclude_when_waiting: false,
                }),
                active_stage_id: Some("stage-a".to_string()),
                ..Default::default()
            })
            .await;
        run.set_stage_started_at(Some(SystemTime::now() - Duration::from_millis(20)))
            .await;
        run.set_last_patch_at(Some(SystemTime::now())).await;
        let graph_store = InMemoryGraphStore::new();
        graph_store
            .add_node(run.run_id(), build_node("stage-a/node-a", "WITHIN", "tool"))
            .expect("add node");
        let node_states = HashMap::from([(
            "stage-a/node-a".to_string(),
            NodeRuntimeState {
                status: NodeRuntimeStatus::Running,
                last_error: None,
            },
        )]);
        let mut stage_states = HashMap::from([(
            "stage-a".to_string(),
            StageRuntimeState {
                status: StageRuntimeStatus::Active,
            },
        )]);
        let contract = ValidatedContractManifest {
            stage_order: vec!["stage-a".to_string()],
        };
        let event_log = InMemoryEventLog::new();

        let result = tick_stage_auto_commit(
            &run,
            &contract,
            &node_states,
            &mut stage_states,
            &graph_store,
            &event_log,
            SystemTime::now(),
        )
        .await
        .expect("commit should occur");

        assert_eq!(result.reason, AutoCommitReason::MaxOpen);
        assert_eq!(run.active_stage_id().await, None);
    }

    #[tokio::test]
    async fn auto_commit_respects_exclude_when_waiting() {
        let registry = InMemoryRunRegistry::new();
        let run = registry
            .create_run(CreateRunInput {
                stage_auto_commit: Some(StageAutoCommitPolicy {
                    enabled: true,
                    quiescence_ms: 10,
                    max_open_ms: 1000,
                    exclude_when_waiting: true,
                }),
                active_stage_id: Some("stage-a".to_string()),
                ..Default::default()
            })
            .await;
        run.set_stage_started_at(Some(SystemTime::now() - Duration::from_millis(50)))
            .await;
        run.set_last_patch_at(Some(SystemTime::now() - Duration::from_millis(20)))
            .await;
        let graph_store = InMemoryGraphStore::new();
        graph_store
            .add_node(
                run.run_id(),
                build_node("stage-a/node-a", "PRE", "durable_wait"),
            )
            .expect("add node");
        let node_states = HashMap::from([(
            "stage-a/node-a".to_string(),
            NodeRuntimeState {
                status: NodeRuntimeStatus::Succeeded,
                last_error: None,
            },
        )]);
        let mut stage_states = HashMap::from([(
            "stage-a".to_string(),
            StageRuntimeState {
                status: StageRuntimeStatus::Active,
            },
        )]);
        let contract = ValidatedContractManifest {
            stage_order: vec!["stage-a".to_string()],
        };
        let event_log = InMemoryEventLog::new();

        let result = tick_stage_auto_commit(
            &run,
            &contract,
            &node_states,
            &mut stage_states,
            &graph_store,
            &event_log,
            SystemTime::now(),
        )
        .await;

        assert!(result.is_none());
        assert_eq!(
            stage_states.get("stage-a").expect("stage-a state").status,
            StageRuntimeStatus::Active
        );
        assert!(event_log.subscribe(0).await.backlog.is_empty());
    }
}
