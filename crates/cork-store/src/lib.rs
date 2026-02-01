//! CORK In-memory Store library.
//!
//! This crate provides in-memory stores for Run, Event, Log, State, and Graph data.
//!
//! # Implementation Status
//!
//! Run registry is implemented for CORE-010. Additional stores are TODO.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use base64::Engine as _;
use base64::engine::general_purpose;
use cork_proto::cork::v1::{HashBundle, RunEvent, RunStatus};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RunMetadata {
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub status: RunStatus,
    pub hash_bundle: Option<HashBundle>,
    pub experiment_id: Option<String>,
    pub variant_id: Option<String>,
}

#[derive(Debug)]
pub struct RunCtx {
    run_id: String,
    metadata: RwLock<RunMetadata>,
}

impl RunCtx {
    pub fn new(run_id: String, metadata: RunMetadata) -> Self {
        Self {
            run_id,
            metadata: RwLock::new(metadata),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn metadata(&self) -> RunMetadata {
        self.metadata
            .read()
            .expect("run metadata lock poisoned")
            .clone()
    }

    pub fn set_status(&self, status: RunStatus) {
        let mut metadata = self.metadata.write().expect("run metadata lock poisoned");
        metadata.status = status;
        metadata.updated_at = SystemTime::now();
    }

    pub fn set_hash_bundle(&self, hash_bundle: Option<HashBundle>) {
        let mut metadata = self.metadata.write().expect("run metadata lock poisoned");
        metadata.hash_bundle = hash_bundle;
        metadata.updated_at = SystemTime::now();
    }
}

#[derive(Debug, Clone, Default)]
pub struct CreateRunInput {
    pub experiment_id: Option<String>,
    pub variant_id: Option<String>,
    pub status: Option<RunStatus>,
    pub hash_bundle: Option<HashBundle>,
}

#[derive(Debug, Clone, Default)]
pub struct RunFilters {
    pub experiment_id: Option<String>,
    pub variant_id: Option<String>,
    pub status: Option<RunStatus>,
}

#[derive(Debug, Clone)]
pub struct RunPage {
    pub runs: Vec<Arc<RunCtx>>,
    pub next_page_token: Option<String>,
}

pub trait RunRegistry: Send + Sync {
    fn create_run(&self, input: CreateRunInput) -> Arc<RunCtx>;
    fn get_run(&self, run_id: &str) -> Option<Arc<RunCtx>>;
    fn list_runs(&self, page_token: Option<&str>, filters: RunFilters, page_size: usize)
    -> RunPage;
}

#[derive(Debug, Default)]
pub struct InMemoryRunRegistry {
    store: RwLock<RunStore>,
}

#[derive(Debug, Default)]
struct RunStore {
    runs_by_id: HashMap<String, Arc<RunCtx>>,
    run_order: Vec<String>,
}

impl InMemoryRunRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RunRegistry for InMemoryRunRegistry {
    fn create_run(&self, input: CreateRunInput) -> Arc<RunCtx> {
        let run_id = generate_run_id();
        let now = SystemTime::now();
        let metadata = RunMetadata {
            created_at: now,
            updated_at: now,
            status: input.status.unwrap_or(RunStatus::RunPending),
            hash_bundle: input.hash_bundle,
            experiment_id: input.experiment_id,
            variant_id: input.variant_id,
        };
        let run_ctx = Arc::new(RunCtx::new(run_id.clone(), metadata));

        let mut store = self.store.write().expect("run store lock poisoned");
        store.run_order.push(run_id.clone());
        store.runs_by_id.insert(run_id, Arc::clone(&run_ctx));

        run_ctx
    }

    fn get_run(&self, run_id: &str) -> Option<Arc<RunCtx>> {
        let store = self.store.read().expect("run store lock poisoned");
        store.runs_by_id.get(run_id).cloned()
    }

    fn list_runs(
        &self,
        page_token: Option<&str>,
        filters: RunFilters,
        page_size: usize,
    ) -> RunPage {
        if page_size == 0 {
            return RunPage {
                runs: Vec::new(),
                next_page_token: None,
            };
        }

        let store = self.store.read().expect("run store lock poisoned");
        let mut filtered = Vec::new();
        for run_id in &store.run_order {
            let Some(run_ctx) = store.runs_by_id.get(run_id) else {
                continue;
            };
            let metadata = run_ctx.metadata();
            if let Some(ref experiment_id) = filters.experiment_id
                && metadata.experiment_id.as_ref() != Some(experiment_id)
            {
                continue;
            }
            if let Some(ref variant_id) = filters.variant_id
                && metadata.variant_id.as_ref() != Some(variant_id)
            {
                continue;
            }
            if let Some(status) = filters.status
                && metadata.status != status
            {
                continue;
            }
            filtered.push(Arc::clone(run_ctx));
        }

        let offset = decode_page_token(page_token).unwrap_or(0);
        let total = filtered.len();
        let start = offset.min(total);
        let end = (start + page_size).min(total);
        let next_page_token = if end < total {
            Some(encode_page_token(end))
        } else {
            None
        };

        RunPage {
            runs: filtered[start..end].to_vec(),
            next_page_token,
        }
    }
}

fn generate_run_id() -> String {
    Uuid::new_v4().to_string()
}

fn encode_page_token(offset: usize) -> String {
    general_purpose::STANDARD.encode(format!("offset:{offset}"))
}

fn decode_page_token(token: Option<&str>) -> Option<usize> {
    let token = token?;
    let decoded = general_purpose::STANDARD.decode(token).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let offset = decoded.strip_prefix("offset:")?;
    offset.parse::<usize>().ok()
}

pub struct EventSubscription {
    pub backlog: Vec<RunEvent>,
    pub receiver: broadcast::Receiver<RunEvent>,
}

pub trait EventLog: Send + Sync {
    fn append(&self, event: RunEvent) -> RunEvent;
    fn subscribe(&self, since_seq: u64) -> EventSubscription;
}

#[derive(Debug)]
pub struct InMemoryEventLog {
    state: RwLock<EventLogState>,
    sender: broadcast::Sender<RunEvent>,
}

#[derive(Debug)]
struct EventLogState {
    next_seq: u64,
    events: Vec<RunEvent>,
}

impl InMemoryEventLog {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            state: RwLock::new(EventLogState {
                next_seq: 0,
                events: Vec::new(),
            }),
            sender,
        }
    }
}

impl Default for InMemoryEventLog {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLog for InMemoryEventLog {
    fn append(&self, mut event: RunEvent) -> RunEvent {
        let mut state = self.state.write().expect("event log lock poisoned");
        let seq = state.next_seq;
        state.next_seq = state.next_seq.saturating_add(1);
        event.event_seq = seq as i64;
        state.events.push(event.clone());
        drop(state);
        let _ = self.sender.send(event.clone());
        event
    }

    fn subscribe(&self, since_seq: u64) -> EventSubscription {
        let receiver = self.sender.subscribe();
        let backlog = {
            let state = self.state.read().expect("event log lock poisoned");
            let start = usize::try_from(since_seq).unwrap_or(state.events.len());
            if start >= state.events.len() {
                Vec::new()
            } else {
                state.events[start..].to_vec()
            }
        };
        EventSubscription { backlog, receiver }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[test]
    fn create_and_get_run() {
        let registry = InMemoryRunRegistry::new();
        let run = registry.create_run(CreateRunInput {
            experiment_id: Some("exp-1".to_string()),
            variant_id: Some("var-1".to_string()),
            status: None,
            hash_bundle: None,
        });

        let fetched = registry.get_run(run.run_id()).expect("run exists");
        assert_eq!(run.run_id(), fetched.run_id());
        let metadata = fetched.metadata();
        assert_eq!(metadata.status, RunStatus::RunPending);
        assert_eq!(metadata.experiment_id.as_deref(), Some("exp-1"));
        assert_eq!(metadata.variant_id.as_deref(), Some("var-1"));
        assert!(
            metadata
                .updated_at
                .duration_since(metadata.created_at)
                .is_ok()
        );
    }

    #[test]
    fn list_runs_with_paging_and_filters() {
        let registry = InMemoryRunRegistry::new();
        let run_a = registry.create_run(CreateRunInput {
            experiment_id: Some("exp-1".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunPending),
            hash_bundle: None,
        });
        let _run_b = registry.create_run(CreateRunInput {
            experiment_id: Some("exp-1".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunRunning),
            hash_bundle: None,
        });
        let run_c = registry.create_run(CreateRunInput {
            experiment_id: Some("exp-2".to_string()),
            variant_id: None,
            status: Some(RunStatus::RunPending),
            hash_bundle: None,
        });

        let page1 = registry.list_runs(
            None,
            RunFilters {
                experiment_id: Some("exp-1".to_string()),
                variant_id: None,
                status: None,
            },
            1,
        );
        assert_eq!(page1.runs.len(), 1);
        assert_eq!(page1.runs[0].run_id(), run_a.run_id());
        let token = page1.next_page_token.expect("next token");

        let page2 = registry.list_runs(
            Some(&token),
            RunFilters {
                experiment_id: Some("exp-1".to_string()),
                variant_id: None,
                status: None,
            },
            1,
        );
        assert_eq!(page2.runs.len(), 1);
        assert!(page2.next_page_token.is_none());

        let status_filtered = registry.list_runs(
            None,
            RunFilters {
                experiment_id: None,
                variant_id: None,
                status: Some(RunStatus::RunPending),
            },
            10,
        );
        let ids: Vec<_> = status_filtered
            .runs
            .iter()
            .map(|run| run.run_id().to_string())
            .collect();
        assert!(ids.contains(&run_a.run_id().to_string()));
        assert!(ids.contains(&run_c.run_id().to_string()));
    }

    #[test]
    fn page_token_round_trip() {
        let token = encode_page_token(42);
        let decoded = decode_page_token(Some(&token));
        assert_eq!(decoded, Some(42));
    }

    #[test]
    fn append_assigns_contiguous_event_seq() {
        let log = InMemoryEventLog::new();
        let first = log.append(RunEvent::default());
        let second = log.append(RunEvent::default());
        assert_eq!(first.event_seq, 0);
        assert_eq!(second.event_seq, 1);
    }

    #[tokio::test]
    async fn subscribe_returns_backlog_and_live_events() {
        let log = InMemoryEventLog::new();
        log.append(RunEvent::default());

        let mut subscription = log.subscribe(0);
        assert_eq!(subscription.backlog.len(), 1);
        assert_eq!(subscription.backlog[0].event_seq, 0);

        log.append(RunEvent::default());
        let received = timeout(Duration::from_secs(1), subscription.receiver.recv())
            .await
            .expect("recv timeout")
            .expect("recv failed");
        assert_eq!(received.event_seq, 1);

        let empty = log.subscribe(2);
        assert!(empty.backlog.is_empty());
    }
}
