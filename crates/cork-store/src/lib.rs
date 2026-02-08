//! CORK In-memory Store library.
//!
//! This crate provides in-memory stores for Run, Event, Log, State, and Graph data.
//!
//! # Implementation Status
//!
//! Run registry is implemented for CORE-010. Additional stores are TODO.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose;
use cork_proto::cork::v1::{CanonicalJsonDocument, HashBundle, LogRecord, RunEvent, RunStatus};
use dashmap::DashMap;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpansionPolicy {
    pub allow_dynamic: bool,
    pub allow_kinds: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RunMetadata {
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub status: RunStatus,
    pub hash_bundle: Option<HashBundle>,
    pub experiment_id: Option<String>,
    pub variant_id: Option<String>,
    pub stage_auto_commit: Option<StageAutoCommitPolicy>,
    pub next_patch_seq: u64,
    pub active_stage_id: Option<String>,
    pub active_stage_expansion_policy: Option<ExpansionPolicy>,
    pub stage_started_at: Option<SystemTime>,
    pub last_patch_at: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageAutoCommitPolicy {
    pub enabled: bool,
    pub quiescence_ms: u64,
    pub max_open_ms: u64,
    pub exclude_when_waiting: bool,
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

    pub async fn metadata(&self) -> RunMetadata {
        self.metadata.read().await.clone()
    }

    pub async fn stage_auto_commit(&self) -> Option<StageAutoCommitPolicy> {
        self.metadata.read().await.stage_auto_commit.clone()
    }

    pub async fn next_patch_seq(&self) -> u64 {
        self.metadata.read().await.next_patch_seq
    }

    pub async fn set_next_patch_seq(&self, next_patch_seq: u64) {
        let mut metadata = self.metadata.write().await;
        metadata.next_patch_seq = next_patch_seq;
        metadata.updated_at = SystemTime::now();
    }

    pub async fn try_advance_patch_seq(&self, expected: u64) -> Result<u64, u64> {
        let mut metadata = self.metadata.write().await;
        if metadata.next_patch_seq != expected {
            return Err(metadata.next_patch_seq);
        }
        metadata.next_patch_seq = metadata.next_patch_seq.saturating_add(1);
        metadata.updated_at = SystemTime::now();
        Ok(metadata.next_patch_seq)
    }

    pub async fn check_patch_seq(&self, expected: u64) -> Result<(), u64> {
        let metadata = self.metadata.read().await;
        if metadata.next_patch_seq != expected {
            return Err(metadata.next_patch_seq);
        }
        Ok(())
    }

    pub async fn advance_patch_seq(&self) -> u64 {
        let mut metadata = self.metadata.write().await;
        metadata.next_patch_seq = metadata.next_patch_seq.saturating_add(1);
        metadata.updated_at = SystemTime::now();
        metadata.next_patch_seq
    }

    pub async fn active_stage_id(&self) -> Option<String> {
        self.metadata.read().await.active_stage_id.clone()
    }

    pub async fn active_stage_expansion_policy(&self) -> Option<ExpansionPolicy> {
        self.metadata
            .read()
            .await
            .active_stage_expansion_policy
            .clone()
    }

    pub async fn stage_started_at(&self) -> Option<SystemTime> {
        self.metadata.read().await.stage_started_at
    }

    pub async fn last_patch_at(&self) -> Option<SystemTime> {
        self.metadata.read().await.last_patch_at
    }

    pub async fn set_stage_started_at(&self, stage_started_at: Option<SystemTime>) {
        let mut metadata = self.metadata.write().await;
        metadata.stage_started_at = stage_started_at;
        metadata.updated_at = SystemTime::now();
    }

    pub async fn set_last_patch_at(&self, last_patch_at: Option<SystemTime>) {
        let mut metadata = self.metadata.write().await;
        metadata.last_patch_at = last_patch_at;
        metadata.updated_at = SystemTime::now();
    }

    pub async fn touch_stage_patch(&self) {
        let mut metadata = self.metadata.write().await;
        metadata.last_patch_at = Some(SystemTime::now());
        metadata.updated_at = SystemTime::now();
    }

    pub async fn set_active_stage(
        &self,
        active_stage_id: Option<String>,
        active_stage_expansion_policy: Option<ExpansionPolicy>,
    ) {
        let mut metadata = self.metadata.write().await;
        metadata.active_stage_id = active_stage_id;
        metadata.active_stage_expansion_policy = active_stage_expansion_policy;
        if metadata.active_stage_id.is_some() {
            let now = SystemTime::now();
            metadata.stage_started_at = Some(now);
            metadata.last_patch_at = Some(now);
        } else {
            metadata.stage_started_at = None;
            metadata.last_patch_at = None;
        }
        metadata.updated_at = SystemTime::now();
    }

    pub async fn set_status(&self, status: RunStatus) {
        let mut metadata = self.metadata.write().await;
        metadata.status = status;
        metadata.updated_at = SystemTime::now();
    }

    pub async fn set_hash_bundle(&self, hash_bundle: Option<HashBundle>) {
        let mut metadata = self.metadata.write().await;
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
    pub stage_auto_commit: Option<StageAutoCommitPolicy>,
    pub next_patch_seq: Option<u64>,
    pub active_stage_id: Option<String>,
    pub active_stage_expansion_policy: Option<ExpansionPolicy>,
    pub stage_started_at: Option<SystemTime>,
    pub last_patch_at: Option<SystemTime>,
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

#[async_trait]
pub trait RunRegistry: Send + Sync {
    async fn create_run(&self, input: CreateRunInput) -> Arc<RunCtx>;
    async fn get_run(&self, run_id: &str) -> Option<Arc<RunCtx>>;
    async fn list_runs(
        &self,
        page_token: Option<&str>,
        filters: RunFilters,
        page_size: usize,
    ) -> RunPage;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatchStoreError {
    PatchSeqMismatch { expected: u64, provided: u64 },
}

pub trait PatchStore: Send + Sync {
    fn set_contract_manifest(&self, run_id: &str, contract: CanonicalJsonDocument);
    fn contract_manifest(&self, run_id: &str) -> Option<CanonicalJsonDocument>;
    fn append_patch(
        &self,
        run_id: &str,
        patch_seq: u64,
        patch: CanonicalJsonDocument,
    ) -> Result<(), PatchStoreError>;
    fn patches_in_order(&self, run_id: &str) -> Vec<CanonicalJsonDocument>;
}

#[derive(Debug, Default)]
pub struct InMemoryPatchStore {
    runs: DashMap<String, PatchState>,
}

#[derive(Debug, Default)]
struct PatchState {
    contract_manifest: Option<CanonicalJsonDocument>,
    patches: Vec<CanonicalJsonDocument>,
}

impl InMemoryPatchStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PatchStore for InMemoryPatchStore {
    fn set_contract_manifest(&self, run_id: &str, contract: CanonicalJsonDocument) {
        let mut state = self.runs.entry(run_id.to_string()).or_default();
        state.contract_manifest = Some(contract);
    }

    fn contract_manifest(&self, run_id: &str) -> Option<CanonicalJsonDocument> {
        self.runs
            .get(run_id)
            .and_then(|state| state.contract_manifest.clone())
    }

    fn append_patch(
        &self,
        run_id: &str,
        patch_seq: u64,
        patch: CanonicalJsonDocument,
    ) -> Result<(), PatchStoreError> {
        let mut state = self.runs.entry(run_id.to_string()).or_default();
        let expected = state.patches.len() as u64;
        if patch_seq != expected {
            return Err(PatchStoreError::PatchSeqMismatch {
                expected,
                provided: patch_seq,
            });
        }
        state.patches.push(patch);
        Ok(())
    }

    fn patches_in_order(&self, run_id: &str) -> Vec<CanonicalJsonDocument> {
        self.runs
            .get(run_id)
            .map(|state| state.patches.clone())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeSpec {
    pub node_id: String,
    pub kind: String,
    pub anchor_position: String,
    pub exec: Value,
    pub deps: Vec<String>,
    pub scheduling: Option<Value>,
    pub htn: Option<Value>,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphStoreError {
    NodeAlreadyExists(String),
    NodeNotFound(String),
    CycleDetected { from: String, to: String },
}

pub trait GraphStore: Send + Sync {
    fn add_node(&self, run_id: &str, node: NodeSpec) -> Result<(), GraphStoreError>;
    fn add_edge(&self, run_id: &str, from: &str, to: &str) -> Result<(), GraphStoreError>;
    fn update_node(
        &self,
        run_id: &str,
        node_id: &str,
        ttl_ms: Option<u64>,
        scheduling: Option<Value>,
        htn: Option<Value>,
    ) -> Result<(), GraphStoreError>;
    fn node(&self, run_id: &str, node_id: &str) -> Option<NodeSpec>;
    fn deps(&self, run_id: &str, node_id: &str) -> Option<Vec<String>>;
}

#[derive(Debug, Default)]
pub struct InMemoryGraphStore {
    graphs: DashMap<String, GraphState>,
}

#[derive(Debug, Default)]
struct GraphState {
    nodes: HashMap<String, NodeSpec>,
    edges: HashMap<String, Vec<String>>,
}

impl InMemoryGraphStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl GraphStore for InMemoryGraphStore {
    fn add_node(&self, run_id: &str, node: NodeSpec) -> Result<(), GraphStoreError> {
        let mut graph = self.graphs.entry(run_id.to_string()).or_default();
        if graph.nodes.contains_key(&node.node_id) {
            return Err(GraphStoreError::NodeAlreadyExists(node.node_id));
        }
        let deps = node.deps.clone();
        graph.edges.entry(node.node_id.clone()).or_insert(deps);
        graph.nodes.insert(node.node_id.clone(), node);
        Ok(())
    }

    fn add_edge(&self, run_id: &str, from: &str, to: &str) -> Result<(), GraphStoreError> {
        let mut graph = self.graphs.entry(run_id.to_string()).or_default();
        if !graph.nodes.contains_key(from) {
            return Err(GraphStoreError::NodeNotFound(from.to_string()));
        }
        if !graph.nodes.contains_key(to) {
            return Err(GraphStoreError::NodeNotFound(to.to_string()));
        }
        if detects_cycle(&graph.edges, from, to) {
            return Err(GraphStoreError::CycleDetected {
                from: from.to_string(),
                to: to.to_string(),
            });
        }
        let deps_snapshot = {
            let deps = graph.edges.entry(to.to_string()).or_default();
            if !deps.iter().any(|dep| dep == from) {
                deps.push(from.to_string());
            }
            deps.clone()
        };
        if let Some(node) = graph.nodes.get_mut(to) {
            node.deps = deps_snapshot;
        }
        Ok(())
    }

    fn update_node(
        &self,
        run_id: &str,
        node_id: &str,
        ttl_ms: Option<u64>,
        scheduling: Option<Value>,
        htn: Option<Value>,
    ) -> Result<(), GraphStoreError> {
        let mut graph = self.graphs.entry(run_id.to_string()).or_default();
        let Some(node) = graph.nodes.get_mut(node_id) else {
            return Err(GraphStoreError::NodeNotFound(node_id.to_string()));
        };
        if let Some(ttl_ms) = ttl_ms {
            node.ttl_ms = Some(ttl_ms);
        }
        if scheduling.is_some() {
            node.scheduling = scheduling;
        }
        if htn.is_some() {
            node.htn = htn;
        }
        Ok(())
    }

    fn node(&self, run_id: &str, node_id: &str) -> Option<NodeSpec> {
        self.graphs
            .get(run_id)
            .and_then(|graph| graph.nodes.get(node_id).cloned())
    }

    fn deps(&self, run_id: &str, node_id: &str) -> Option<Vec<String>> {
        self.graphs
            .get(run_id)
            .and_then(|graph| graph.edges.get(node_id).cloned())
    }
}

fn detects_cycle(edges: &HashMap<String, Vec<String>>, from: &str, to: &str) -> bool {
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
        if let Some(deps) = edges.get(&node) {
            for dep in deps {
                stack.push(dep.clone());
            }
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateStoreError {
    InvalidJsonPointer(String),
    MissingStageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePayload {
    pub content_type: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRef {
    pub uri: String,
    pub sha256_hex: String,
    pub content_type: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeOutput {
    pub payload: NodePayload,
    pub parsed_json: Option<Value>,
    pub artifacts: Vec<ArtifactRef>,
}

impl NodeOutput {
    pub fn from_payload(payload: NodePayload, artifacts: Vec<ArtifactRef>) -> Self {
        let parsed_json = if is_json_content_type(&payload.content_type) {
            serde_json::from_slice(&payload.data).ok()
        } else {
            None
        };
        Self {
            payload,
            parsed_json,
            artifacts,
        }
    }

    pub fn is_json(&self) -> bool {
        is_json_content_type(&self.payload.content_type)
    }
}

fn is_json_content_type(content_type: &str) -> bool {
    let content_type = content_type.split(';').next().unwrap_or("").trim();
    let content_type = content_type.to_ascii_lowercase();
    content_type == "application/json" || content_type.ends_with("+json")
}

pub trait StateStore: Send + Sync {
    fn apply_state_put(
        &self,
        run_id: &str,
        scope: &str,
        stage_id: Option<&str>,
        json_pointer: &str,
        value: Value,
    ) -> Result<(), StateStoreError>;
    fn run_state(&self, run_id: &str) -> Option<Value>;
    fn stage_state(&self, run_id: &str, stage_id: &str) -> Option<Value>;
    fn set_node_output(&self, run_id: &str, node_id: &str, output: NodeOutput);
    fn node_output(&self, run_id: &str, node_id: &str) -> Option<NodeOutput>;
}

#[derive(Debug, Default)]
pub struct InMemoryStateStore {
    states: DashMap<String, RunStateStore>,
}

#[derive(Debug)]
struct RunStateStore {
    run_state: Value,
    stage_state: HashMap<String, Value>,
    node_outputs: HashMap<String, NodeOutput>,
}

impl Default for RunStateStore {
    fn default() -> Self {
        Self {
            run_state: Value::Object(serde_json::Map::new()),
            stage_state: HashMap::new(),
            node_outputs: HashMap::new(),
        }
    }
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateStore for InMemoryStateStore {
    fn apply_state_put(
        &self,
        run_id: &str,
        scope: &str,
        stage_id: Option<&str>,
        json_pointer: &str,
        value: Value,
    ) -> Result<(), StateStoreError> {
        let mut state = self.states.entry(run_id.to_string()).or_default();
        match scope {
            "RUN" => apply_state_put_value(&mut state.run_state, json_pointer, value),
            "STAGE" => {
                let stage_id = stage_id.ok_or(StateStoreError::MissingStageId)?;
                let entry = state
                    .stage_state
                    .entry(stage_id.to_string())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                apply_state_put_value(entry, json_pointer, value)
            }
            _ => Err(StateStoreError::InvalidJsonPointer(format!(
                "unknown scope {scope}"
            ))),
        }
    }

    fn run_state(&self, run_id: &str) -> Option<Value> {
        self.states.get(run_id).map(|state| state.run_state.clone())
    }

    fn stage_state(&self, run_id: &str, stage_id: &str) -> Option<Value> {
        self.states
            .get(run_id)
            .and_then(|state| state.stage_state.get(stage_id).cloned())
    }

    fn set_node_output(&self, run_id: &str, node_id: &str, output: NodeOutput) {
        let mut state = self.states.entry(run_id.to_string()).or_default();
        state.node_outputs.insert(node_id.to_string(), output);
    }

    fn node_output(&self, run_id: &str, node_id: &str) -> Option<NodeOutput> {
        self.states
            .get(run_id)
            .and_then(|state| state.node_outputs.get(node_id).cloned())
    }
}

fn apply_pointer_mut(
    root: &mut Value,
    json_pointer: &str,
    value: Value,
) -> Result<(), StateStoreError> {
    if json_pointer.is_empty() {
        *root = value;
        return Ok(());
    }
    ensure_pointer_path(root, json_pointer)?;
    let Some(target) = root.pointer_mut(json_pointer) else {
        return Err(StateStoreError::InvalidJsonPointer(
            json_pointer.to_string(),
        ));
    };
    *target = value;
    Ok(())
}

pub fn apply_state_put_value(
    root: &mut Value,
    json_pointer: &str,
    value: Value,
) -> Result<(), StateStoreError> {
    apply_pointer_mut(root, json_pointer, value)
}

fn ensure_pointer_path(root: &mut Value, json_pointer: &str) -> Result<(), StateStoreError> {
    if !json_pointer.starts_with('/') {
        return Err(StateStoreError::InvalidJsonPointer(
            json_pointer.to_string(),
        ));
    }
    let tokens: Vec<String> = json_pointer
        .split('/')
        .skip(1)
        .map(unescape_pointer_token)
        .collect();
    if tokens.is_empty() {
        return Ok(());
    }
    ensure_pointer_path_recursive(root, json_pointer, &tokens)
}

fn unescape_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn ensure_pointer_path_recursive(
    current: &mut Value,
    json_pointer: &str,
    tokens: &[String],
) -> Result<(), StateStoreError> {
    if tokens.is_empty() {
        return Ok(());
    }
    let token = &tokens[0];
    let is_last = tokens.len() == 1;
    match current {
        Value::Object(map) => {
            if is_last {
                map.entry(token.to_string()).or_insert(Value::Null);
                Ok(())
            } else {
                let entry = map
                    .entry(token.to_string())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                ensure_pointer_path_recursive(entry, json_pointer, &tokens[1..])
            }
        }
        Value::Array(items) => {
            let idx = token
                .parse::<usize>()
                .map_err(|_| StateStoreError::InvalidJsonPointer(json_pointer.to_string()))?;
            if idx >= items.len() {
                items.resize(idx + 1, Value::Null);
            }
            if is_last {
                if items[idx].is_null() {
                    items[idx] = Value::Null;
                }
                Ok(())
            } else {
                ensure_pointer_path_recursive(&mut items[idx], json_pointer, &tokens[1..])
            }
        }
        _ => {
            *current = Value::Object(serde_json::Map::new());
            ensure_pointer_path_recursive(current, json_pointer, tokens)
        }
    }
}

#[derive(Debug)]
pub struct InMemoryRunRegistry {
    runs_by_id: DashMap<String, Arc<RunCtx>>,
    run_order: RwLock<Vec<String>>,
}

impl InMemoryRunRegistry {
    pub fn new() -> Self {
        Self {
            runs_by_id: DashMap::new(),
            run_order: RwLock::new(Vec::new()),
        }
    }
}

impl Default for InMemoryRunRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RunRegistry for InMemoryRunRegistry {
    async fn create_run(&self, input: CreateRunInput) -> Arc<RunCtx> {
        let run_id = generate_run_id();
        let now = SystemTime::now();
        let mut stage_started_at = input.stage_started_at;
        let mut last_patch_at = input.last_patch_at;
        if input.active_stage_id.is_some() {
            if stage_started_at.is_none() {
                stage_started_at = Some(now);
            }
            if last_patch_at.is_none() {
                last_patch_at = stage_started_at;
            }
        }
        let metadata = RunMetadata {
            created_at: now,
            updated_at: now,
            status: input.status.unwrap_or(RunStatus::RunPending),
            hash_bundle: input.hash_bundle,
            experiment_id: input.experiment_id,
            variant_id: input.variant_id,
            stage_auto_commit: input.stage_auto_commit,
            next_patch_seq: input.next_patch_seq.unwrap_or(0),
            active_stage_id: input.active_stage_id,
            active_stage_expansion_policy: input.active_stage_expansion_policy,
            stage_started_at,
            last_patch_at,
        };
        let run_ctx = Arc::new(RunCtx::new(run_id.clone(), metadata));

        self.runs_by_id.insert(run_id.clone(), Arc::clone(&run_ctx));
        self.run_order.write().await.push(run_id);

        run_ctx
    }

    async fn get_run(&self, run_id: &str) -> Option<Arc<RunCtx>> {
        self.runs_by_id.get(run_id).map(|run| Arc::clone(&*run))
    }

    async fn list_runs(
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

        let run_order = self.run_order.read().await.clone();
        let mut filtered = Vec::new();
        for run_id in &run_order {
            let Some(run_ctx) = self.runs_by_id.get(run_id) else {
                continue;
            };
            let metadata = run_ctx.metadata().await;
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
            filtered.push(Arc::clone(&*run_ctx));
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

#[async_trait]
pub trait EventLog: Send + Sync {
    async fn append(&self, event: RunEvent) -> RunEvent;
    async fn subscribe(&self, since_seq: u64) -> EventSubscription;
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

#[async_trait]
impl EventLog for InMemoryEventLog {
    async fn append(&self, mut event: RunEvent) -> RunEvent {
        let mut state = self.state.write().await;
        let seq = state.next_seq;
        state.next_seq = state.next_seq.saturating_add(1);
        event.event_seq = seq as i64;
        state.events.push(event.clone());
        drop(state);
        let _ = self.sender.send(event.clone());
        event
    }

    async fn subscribe(&self, since_seq: u64) -> EventSubscription {
        let receiver = self.sender.subscribe();
        let backlog = {
            let state = self.state.read().await;
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

#[derive(Debug, Clone, Default)]
pub struct LogFilters {
    pub scope_id: Option<String>,
    pub span_id_hex: Option<String>,
    pub stage_id: Option<String>,
    pub node_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LogPage {
    pub logs: Vec<LogRecord>,
    pub next_page_token: Option<String>,
}

pub trait LogStore: Send + Sync {
    fn append_log(&self, run_id: &str, stage_id: &str, node_id: &str, log: LogRecord) -> LogRecord;
    fn list_logs(
        &self,
        run_id: &str,
        page_token: Option<&str>,
        filters: LogFilters,
        page_size: usize,
    ) -> LogPage;
}

#[derive(Debug)]
pub struct InMemoryLogStore {
    runs: DashMap<String, RunLogState>,
}

#[derive(Debug)]
struct RunLogState {
    logs: Vec<LogRecord>,
    next_scope_seq: HashMap<String, u64>,
}

impl InMemoryLogStore {
    pub fn new() -> Self {
        Self {
            runs: DashMap::new(),
        }
    }

    fn ensure_scope_id(run_id: &str, stage_id: &str, node_id: &str, log: &LogRecord) -> String {
        if !log.scope_id.is_empty() {
            return log.scope_id.clone();
        }
        if !stage_id.is_empty() || !node_id.is_empty() {
            return format!("{stage_id}:{node_id}");
        }
        run_id.to_string()
    }

    fn ensure_attrs(attrs: &mut HashMap<String, String>, stage_id: &str, node_id: &str) {
        if !stage_id.is_empty() {
            attrs
                .entry("stage_id".to_string())
                .or_insert_with(|| stage_id.to_string());
        }
        if !node_id.is_empty() {
            attrs
                .entry("node_id".to_string())
                .or_insert_with(|| node_id.to_string());
        }
    }
}

impl Default for InMemoryLogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore for InMemoryLogStore {
    fn append_log(
        &self,
        run_id: &str,
        stage_id: &str,
        node_id: &str,
        mut log: LogRecord,
    ) -> LogRecord {
        let mut state = self
            .runs
            .entry(run_id.to_string())
            .or_insert_with(|| RunLogState {
                logs: Vec::new(),
                next_scope_seq: HashMap::new(),
            });

        let scope_id = Self::ensure_scope_id(run_id, stage_id, node_id, &log);
        let seq = state.next_scope_seq.entry(scope_id.clone()).or_insert(0);
        log.scope_id = scope_id;
        log.scope_seq = *seq as i64;
        *seq = seq.saturating_add(1);
        Self::ensure_attrs(&mut log.attrs, stage_id, node_id);
        state.logs.push(log.clone());
        log
    }

    fn list_logs(
        &self,
        run_id: &str,
        page_token: Option<&str>,
        filters: LogFilters,
        page_size: usize,
    ) -> LogPage {
        let state = match self.runs.get(run_id) {
            Some(state) => state,
            None => {
                return LogPage {
                    logs: Vec::new(),
                    next_page_token: None,
                };
            }
        };

        let mut filtered = Vec::new();
        for log in &state.logs {
            if let Some(ref scope_id) = filters.scope_id
                && &log.scope_id != scope_id
            {
                continue;
            }
            if let Some(ref span_id) = filters.span_id_hex
                && &log.span_id_hex != span_id
            {
                continue;
            }
            if let Some(ref stage_id) = filters.stage_id
                && log.attrs.get("stage_id") != Some(stage_id)
            {
                continue;
            }
            if let Some(ref node_id) = filters.node_id
                && log.attrs.get("node_id") != Some(node_id)
            {
                continue;
            }
            filtered.push(log.clone());
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

        LogPage {
            logs: filtered[start..end].to_vec(),
            next_page_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn create_and_get_run() {
        let registry = InMemoryRunRegistry::new();
        let run = registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp-1".to_string()),
                variant_id: Some("var-1".to_string()),
                stage_auto_commit: Some(StageAutoCommitPolicy {
                    enabled: true,
                    quiescence_ms: 10,
                    max_open_ms: 20,
                    exclude_when_waiting: false,
                }),
                ..Default::default()
            })
            .await;

        let fetched = registry.get_run(run.run_id()).await.expect("run exists");
        assert_eq!(run.run_id(), fetched.run_id());
        let metadata = fetched.metadata().await;
        assert_eq!(metadata.status, RunStatus::RunPending);
        assert_eq!(metadata.experiment_id.as_deref(), Some("exp-1"));
        assert_eq!(metadata.variant_id.as_deref(), Some("var-1"));
        assert!(
            metadata
                .updated_at
                .duration_since(metadata.created_at)
                .is_ok()
        );
        assert_eq!(
            run.stage_auto_commit().await,
            Some(StageAutoCommitPolicy {
                enabled: true,
                quiescence_ms: 10,
                max_open_ms: 20,
                exclude_when_waiting: false,
            })
        );
    }

    #[tokio::test]
    async fn try_advance_patch_seq_is_atomic() {
        let registry = InMemoryRunRegistry::new();
        let run = registry
            .create_run(CreateRunInput {
                next_patch_seq: Some(0),
                ..Default::default()
            })
            .await;

        assert_eq!(run.try_advance_patch_seq(0).await, Ok(1));
        assert_eq!(run.try_advance_patch_seq(0).await, Err(1));
        assert_eq!(run.try_advance_patch_seq(1).await, Ok(2));
    }

    #[test]
    fn patch_store_keeps_patches_in_order() {
        let store = InMemoryPatchStore::new();
        let run_id = "run-patch";
        let patch_zero = CanonicalJsonDocument {
            canonical_json_utf8: br#"{"patch_seq":0}"#.to_vec(),
            sha256: None,
            schema_id: "cork.graph_patch.v0.1".to_string(),
        };
        let patch_one = CanonicalJsonDocument {
            canonical_json_utf8: br#"{"patch_seq":1}"#.to_vec(),
            sha256: None,
            schema_id: "cork.graph_patch.v0.1".to_string(),
        };

        store
            .append_patch(run_id, 0, patch_zero.clone())
            .expect("append patch 0");
        store
            .append_patch(run_id, 1, patch_one.clone())
            .expect("append patch 1");

        let patches = store.patches_in_order(run_id);
        assert_eq!(patches, vec![patch_zero, patch_one.clone()]);

        let err = store.append_patch(run_id, 3, patch_one);
        assert!(matches!(
            err,
            Err(PatchStoreError::PatchSeqMismatch {
                expected: 2,
                provided: 3
            })
        ));
    }

    #[tokio::test]
    async fn list_runs_with_paging_and_filters() {
        let registry = InMemoryRunRegistry::new();
        let run_a = registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp-1".to_string()),
                status: Some(RunStatus::RunPending),
                ..Default::default()
            })
            .await;
        let _run_b = registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp-1".to_string()),
                status: Some(RunStatus::RunRunning),
                ..Default::default()
            })
            .await;
        let run_c = registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp-2".to_string()),
                status: Some(RunStatus::RunPending),
                ..Default::default()
            })
            .await;

        let page1 = registry
            .list_runs(
                None,
                RunFilters {
                    experiment_id: Some("exp-1".to_string()),
                    variant_id: None,
                    status: None,
                },
                1,
            )
            .await;
        assert_eq!(page1.runs.len(), 1);
        assert_eq!(page1.runs[0].run_id(), run_a.run_id());
        let token = page1.next_page_token.expect("next token");

        let page2 = registry
            .list_runs(
                Some(&token),
                RunFilters {
                    experiment_id: Some("exp-1".to_string()),
                    variant_id: None,
                    status: None,
                },
                1,
            )
            .await;
        assert_eq!(page2.runs.len(), 1);
        assert!(page2.next_page_token.is_none());

        let status_filtered = registry
            .list_runs(
                None,
                RunFilters {
                    experiment_id: None,
                    variant_id: None,
                    status: Some(RunStatus::RunPending),
                },
                10,
            )
            .await;
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

    #[tokio::test]
    async fn append_assigns_contiguous_event_seq() {
        let log = InMemoryEventLog::new();
        let first = log.append(RunEvent::default()).await;
        let second = log.append(RunEvent::default()).await;
        assert_eq!(first.event_seq, 0);
        assert_eq!(second.event_seq, 1);
    }

    #[test]
    fn log_store_assigns_scope_seq_and_filters() {
        let store = InMemoryLogStore::new();
        let run_id = "run-1";
        let log_a = LogRecord {
            level: "INFO".to_string(),
            message: "first".to_string(),
            trace_id_hex: "trace-a".to_string(),
            span_id_hex: "span-a".to_string(),
            scope_id: "".to_string(),
            scope_seq: 0,
            attrs: Default::default(),
            ts: None,
        };
        let log_b = LogRecord {
            level: "INFO".to_string(),
            message: "second".to_string(),
            trace_id_hex: "trace-a".to_string(),
            span_id_hex: "span-a".to_string(),
            scope_id: "".to_string(),
            scope_seq: 0,
            attrs: Default::default(),
            ts: None,
        };

        let stored_a = store.append_log(run_id, "stage-a", "node-a", log_a);
        let stored_b = store.append_log(run_id, "stage-a", "node-a", log_b);
        assert_eq!(stored_a.scope_seq, 0);
        assert_eq!(stored_b.scope_seq, 1);
        assert_eq!(stored_a.scope_id, stored_b.scope_id);

        let page = store.list_logs(
            run_id,
            None,
            LogFilters {
                scope_id: Some(stored_a.scope_id.clone()),
                span_id_hex: None,
                stage_id: Some("stage-a".to_string()),
                node_id: Some("node-a".to_string()),
            },
            10,
        );
        assert_eq!(page.logs.len(), 2);
    }

    #[test]
    fn log_store_paginates() {
        let store = InMemoryLogStore::new();
        let run_id = "run-2";
        for idx in 0..3 {
            let log = LogRecord {
                level: "INFO".to_string(),
                message: format!("log-{idx}"),
                trace_id_hex: "".to_string(),
                span_id_hex: "".to_string(),
                scope_id: "".to_string(),
                scope_seq: 0,
                attrs: Default::default(),
                ts: None,
            };
            store.append_log(run_id, "stage", "node", log);
        }

        let first_page = store.list_logs(run_id, None, LogFilters::default(), 2);
        assert_eq!(first_page.logs.len(), 2);
        let token = first_page.next_page_token.expect("next token");

        let second_page = store.list_logs(run_id, Some(&token), LogFilters::default(), 2);
        assert_eq!(second_page.logs.len(), 1);
        assert!(second_page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn subscribe_returns_backlog_and_live_events() {
        let log = InMemoryEventLog::new();
        log.append(RunEvent::default()).await;

        let mut subscription = log.subscribe(0).await;
        assert_eq!(subscription.backlog.len(), 1);
        assert_eq!(subscription.backlog[0].event_seq, 0);

        log.append(RunEvent::default()).await;
        let received = timeout(Duration::from_secs(1), subscription.receiver.recv())
            .await
            .expect("recv timeout")
            .expect("recv failed");
        assert_eq!(received.event_seq, 1);

        let empty = log.subscribe(2).await;
        assert!(empty.backlog.is_empty());
    }

    #[test]
    fn graph_store_adds_nodes_and_edges() {
        let store = InMemoryGraphStore::new();
        let run_id = "run-1";
        let node_a = NodeSpec {
            node_id: "node-a".to_string(),
            kind: "TOOL".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec: Value::Null,
            deps: Vec::new(),
            scheduling: None,
            htn: None,
            ttl_ms: None,
        };
        let node_b = NodeSpec {
            node_id: "node-b".to_string(),
            kind: "LLM".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec: Value::Null,
            deps: vec!["node-a".to_string()],
            scheduling: None,
            htn: None,
            ttl_ms: None,
        };

        store.add_node(run_id, node_a).expect("add node a");
        store.add_node(run_id, node_b).expect("add node b");
        store
            .add_edge(run_id, "node-a", "node-b")
            .expect("add edge");

        let deps = store.deps(run_id, "node-b").expect("deps");
        assert_eq!(deps, vec!["node-a".to_string()]);
    }

    #[test]
    fn graph_store_rejects_cycles() {
        let store = InMemoryGraphStore::new();
        let run_id = "run-2";
        let node_a = NodeSpec {
            node_id: "node-a".to_string(),
            kind: "TOOL".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec: Value::Null,
            deps: Vec::new(),
            scheduling: None,
            htn: None,
            ttl_ms: None,
        };
        let node_b = NodeSpec {
            node_id: "node-b".to_string(),
            kind: "LLM".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec: Value::Null,
            deps: Vec::new(),
            scheduling: None,
            htn: None,
            ttl_ms: None,
        };

        store.add_node(run_id, node_a).expect("add node a");
        store.add_node(run_id, node_b).expect("add node b");
        store
            .add_edge(run_id, "node-a", "node-b")
            .expect("add edge a->b");
        let result = store.add_edge(run_id, "node-b", "node-a");
        assert!(matches!(result, Err(GraphStoreError::CycleDetected { .. })));
    }

    #[test]
    fn state_store_applies_json_pointer() {
        let store = InMemoryStateStore::new();
        let run_id = "run-3";
        store
            .apply_state_put(run_id, "RUN", None, "/foo/bar", serde_json::json!(42))
            .expect("apply run state");
        let run_state = store.run_state(run_id).expect("run state");
        assert_eq!(run_state.pointer("/foo/bar"), Some(&serde_json::json!(42)));

        store
            .apply_state_put(
                run_id,
                "STAGE",
                Some("stage-1"),
                "/meta",
                serde_json::json!({"ok": true}),
            )
            .expect("apply stage state");
        let stage_state = store.stage_state(run_id, "stage-1").expect("stage state");
        assert_eq!(
            stage_state.pointer("/meta/ok"),
            Some(&serde_json::json!(true))
        );
    }

    #[test]
    fn node_output_parses_json_payloads() {
        let output = NodeOutput::from_payload(
            NodePayload {
                content_type: "application/json; charset=utf-8".to_string(),
                data: br#"{"ok": true}"#.to_vec(),
            },
            Vec::new(),
        );
        assert!(output.is_json());
        assert_eq!(output.parsed_json, Some(serde_json::json!({ "ok": true })));

        let text_output = NodeOutput::from_payload(
            NodePayload {
                content_type: "text/plain".to_string(),
                data: b"hello".to_vec(),
            },
            Vec::new(),
        );
        assert!(!text_output.is_json());
        assert_eq!(text_output.parsed_json, None);
    }
}
