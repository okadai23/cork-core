use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cork_proto::cork::v1::{PolicyEvent, RunEvent, RunStatus, run_event};
use cork_store::{EventLog, RunCtx};
use prost_types::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchdogReason {
    RunTtlExceeded,
    IdleTtlExceeded,
    MaxTotalPatchCountExceeded,
    MaxPatchCountPerStageExceeded,
    StageOpenTooLong,
}

impl WatchdogReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            WatchdogReason::RunTtlExceeded => "watchdog_run_ttl",
            WatchdogReason::IdleTtlExceeded => "watchdog_idle_ttl",
            WatchdogReason::MaxTotalPatchCountExceeded => "watchdog_max_total_patch_count",
            WatchdogReason::MaxPatchCountPerStageExceeded => "watchdog_max_patch_count_per_stage",
            WatchdogReason::StageOpenTooLong => "watchdog_stage_open_max",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchdogViolation {
    pub reason: WatchdogReason,
    pub attrs: HashMap<String, String>,
}

pub struct WatchdogSupervisor {
    ticker: tokio::time::Interval,
}

impl WatchdogSupervisor {
    pub fn new(interval: Duration) -> Self {
        Self {
            ticker: tokio::time::interval(interval),
        }
    }

    pub async fn tick(&mut self) {
        self.ticker.tick().await;
    }
}

pub async fn enforce_watchdog(
    run_ctx: &Arc<RunCtx>,
    event_log: &dyn EventLog,
    now: SystemTime,
) -> Option<WatchdogViolation> {
    let policy = run_ctx.watchdog_policy().await?;
    let metadata = run_ctx.metadata().await;
    if is_terminal(metadata.status) {
        return None;
    }
    let mut violation = None;
    if let Some(run_ttl_ms) = policy.run_ttl_ms
        && elapsed(now, metadata.created_at) > Duration::from_millis(run_ttl_ms)
    {
        let mut attrs = HashMap::new();
        attrs.insert("run_ttl_ms".to_string(), run_ttl_ms.to_string());
        violation = Some(WatchdogViolation {
            reason: WatchdogReason::RunTtlExceeded,
            attrs,
        });
    }
    if violation.is_none()
        && let Some(idle_ttl_ms) = policy.idle_ttl_ms
    {
        let last_progress_at = metadata.last_progress_at.unwrap_or(metadata.created_at);
        if elapsed(now, last_progress_at) > Duration::from_millis(idle_ttl_ms) {
            let mut attrs = HashMap::new();
            attrs.insert("idle_ttl_ms".to_string(), idle_ttl_ms.to_string());
            violation = Some(WatchdogViolation {
                reason: WatchdogReason::IdleTtlExceeded,
                attrs,
            });
        }
    }
    if violation.is_none()
        && let Some(max_total_patch_count) = policy.max_total_patch_count
        && metadata.total_patch_count > max_total_patch_count
    {
        let mut attrs = HashMap::new();
        attrs.insert(
            "max_total_patch_count".to_string(),
            max_total_patch_count.to_string(),
        );
        attrs.insert(
            "total_patch_count".to_string(),
            metadata.total_patch_count.to_string(),
        );
        violation = Some(WatchdogViolation {
            reason: WatchdogReason::MaxTotalPatchCountExceeded,
            attrs,
        });
    }
    if violation.is_none()
        && let Some(max_patch_count_per_stage) = policy.max_patch_count_per_stage
        && let Some((stage_id, count)) = metadata
            .stage_patch_counts
            .iter()
            .find(|(_, count)| **count > max_patch_count_per_stage)
    {
        let mut attrs = HashMap::new();
        attrs.insert(
            "max_patch_count_per_stage".to_string(),
            max_patch_count_per_stage.to_string(),
        );
        attrs.insert("stage_id".to_string(), stage_id.to_string());
        attrs.insert("stage_patch_count".to_string(), count.to_string());
        violation = Some(WatchdogViolation {
            reason: WatchdogReason::MaxPatchCountPerStageExceeded,
            attrs,
        });
    }
    if violation.is_none()
        && let (Some(active_stage_id), Some(stage_started_at), Some(policy)) = (
            metadata.active_stage_id.as_ref(),
            metadata.stage_started_at,
            metadata.stage_auto_commit.as_ref(),
        )
        && policy.enabled
        && policy.max_open_ms > 0
        && elapsed(now, stage_started_at) > Duration::from_millis(policy.max_open_ms)
    {
        let mut attrs = HashMap::new();
        attrs.insert("stage_id".to_string(), active_stage_id.to_string());
        attrs.insert("max_open_ms".to_string(), policy.max_open_ms.to_string());
        violation = Some(WatchdogViolation {
            reason: WatchdogReason::StageOpenTooLong,
            attrs,
        });
    }

    let violation = violation?;
    run_ctx.set_status(RunStatus::RunCancelled).await;
    run_ctx.set_last_progress_at(Some(now)).await;
    let mut attrs = violation.attrs.clone();
    attrs.insert("reason".to_string(), violation.reason.as_str().to_string());
    let event = RunEvent {
        event_seq: 0,
        ts: Some(system_time_to_timestamp(now)),
        event: Some(run_event::Event::Policy(PolicyEvent {
            kind: "watchdog_cancel".to_string(),
            attrs,
        })),
    };
    event_log.append(event).await;
    Some(violation)
}

fn elapsed(now: SystemTime, then: SystemTime) -> Duration {
    now.duration_since(then).unwrap_or_default()
}

fn is_terminal(status: RunStatus) -> bool {
    matches!(
        status,
        RunStatus::RunSucceeded
            | RunStatus::RunFailed
            | RunStatus::RunCancelled
            | RunStatus::RunTimedOut
    )
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
        CreateRunInput, InMemoryEventLog, InMemoryRunRegistry, RunRegistry, WatchdogPolicy,
    };

    #[tokio::test]
    async fn watchdog_cancels_on_idle_ttl() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry
            .create_run(CreateRunInput {
                watchdog_policy: Some(WatchdogPolicy {
                    run_ttl_ms: None,
                    idle_ttl_ms: Some(10),
                    max_total_patch_count: None,
                    max_patch_count_per_stage: None,
                }),
                ..Default::default()
            })
            .await;
        let event_log = InMemoryEventLog::new();
        let now = SystemTime::now();
        run_ctx
            .set_last_progress_at(Some(now - Duration::from_millis(20)))
            .await;
        let violation = enforce_watchdog(&run_ctx, &event_log, now).await;
        assert!(matches!(
            violation,
            Some(WatchdogViolation {
                reason: WatchdogReason::IdleTtlExceeded,
                ..
            })
        ));
        let metadata = run_ctx.metadata().await;
        assert_eq!(metadata.status, RunStatus::RunCancelled);
        let events = event_log.subscribe(0).await.expect("subscribe").backlog;
        let policy_event = events
            .into_iter()
            .find_map(|event| match event.event {
                Some(run_event::Event::Policy(policy)) => Some(policy),
                _ => None,
            })
            .expect("policy event");
        assert_eq!(policy_event.kind, "watchdog_cancel");
        assert_eq!(
            policy_event.attrs.get("reason"),
            Some(&WatchdogReason::IdleTtlExceeded.as_str().to_string())
        );
    }

    #[tokio::test]
    async fn watchdog_cancels_on_patch_storm() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry
            .create_run(CreateRunInput {
                watchdog_policy: Some(WatchdogPolicy {
                    run_ttl_ms: None,
                    idle_ttl_ms: None,
                    max_total_patch_count: Some(2),
                    max_patch_count_per_stage: Some(1),
                }),
                ..Default::default()
            })
            .await;
        run_ctx.record_patch("stage-a").await;
        run_ctx.record_patch("stage-a").await;
        run_ctx.record_patch("stage-a").await;
        let event_log = InMemoryEventLog::new();
        let violation = enforce_watchdog(&run_ctx, &event_log, SystemTime::now()).await;
        assert!(matches!(
            violation,
            Some(WatchdogViolation {
                reason: WatchdogReason::MaxTotalPatchCountExceeded,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn watchdog_cancels_on_stage_open_too_long() {
        let registry = InMemoryRunRegistry::new();
        let now = SystemTime::now();
        let run_ctx = registry
            .create_run(CreateRunInput {
                watchdog_policy: Some(WatchdogPolicy {
                    run_ttl_ms: Some(1000),
                    idle_ttl_ms: None,
                    max_total_patch_count: None,
                    max_patch_count_per_stage: None,
                }),
                stage_auto_commit: Some(cork_store::StageAutoCommitPolicy {
                    enabled: true,
                    quiescence_ms: 0,
                    max_open_ms: 10,
                    exclude_when_waiting: false,
                }),
                active_stage_id: Some("stage-a".to_string()),
                stage_started_at: Some(now - Duration::from_millis(20)),
                last_patch_at: Some(now - Duration::from_millis(20)),
                ..Default::default()
            })
            .await;
        let event_log = InMemoryEventLog::new();
        let violation = enforce_watchdog(&run_ctx, &event_log, now).await;
        assert!(matches!(
            violation,
            Some(WatchdogViolation {
                reason: WatchdogReason::StageOpenTooLong,
                ..
            })
        ));
    }
}
