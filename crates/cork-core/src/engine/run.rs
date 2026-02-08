use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose;
use cork_proto::cork::v1::run_event;
use cork_proto::cork::v1::{
    GetRunResponse, InvokeToolRequest, LogRecord, NodeStateChanged, NodeStatus, Payload, RunEvent,
    RunHandle, RunStatus, TraceContext,
};
use cork_store::{
    ArtifactRef, EventLog, LogStore, NodeOutput, NodePayload, NodeSpec, RunCtx, RunRegistry,
    StageAutoCommitPolicy, StateStore,
};
use prost_types::Timestamp;
use serde_json::Value;

use super::refs::{RenderAs, ValueRef, ValueRefType, resolve_value_ref};
use crate::scheduler::list::SchedulingProfile;
use crate::scheduler::resource::{ResourceManager, ResourceManagerError};
use crate::worker::client::{InvocationBudget, InvocationContext, WorkerClient};

pub async fn run_ctx_to_response(run_ctx: &Arc<RunCtx>) -> GetRunResponse {
    let metadata = run_ctx.metadata().await;
    GetRunResponse {
        handle: Some(RunHandle {
            run_id: run_ctx.run_id().to_string(),
        }),
        status: metadata.status as i32,
        created_at: Some(system_time_to_timestamp(metadata.created_at)),
        updated_at: Some(system_time_to_timestamp(metadata.updated_at)),
        hashes: metadata.hash_bundle,
        active_stage_id: metadata.active_stage_id.unwrap_or_default(),
        policy: None,
    }
}

pub async fn get_run_response(registry: &dyn RunRegistry, run_id: &str) -> Option<GetRunResponse> {
    let run_ctx = registry.get_run(run_id).await?;
    Some(run_ctx_to_response(&run_ctx).await)
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
pub enum SchedulerTieBreak {
    Fifo,
    Priority,
    ShortestEstimatedFirst,
    LongestEstimatedFirst,
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
    pub scheduler_tie_break: SchedulerTieBreak,
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
    let scheduler_tie_break = parse_scheduler_tie_break(value)?;
    let stage_auto_commit = parse_stage_auto_commit(value)?;
    Ok(ValidatedPolicy {
        resource_pools,
        scheduler_mode,
        scheduler_tie_break,
        stage_auto_commit,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRuntimeStatus {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
    TimedOut,
    Expired,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRuntimeState {
    pub status: NodeRuntimeStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageRuntimeStatus {
    Pending,
    Active,
    Committed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRuntimeState {
    pub status: StageRuntimeStatus,
}

pub fn evaluate_node_readiness(
    run_id: &str,
    node: &NodeSpec,
    current_state: &NodeRuntimeState,
    node_states: &HashMap<String, NodeRuntimeState>,
    stage_states: &HashMap<String, StageRuntimeState>,
    contract: &ValidatedContractManifest,
    state_store: &dyn StateStore,
) -> NodeRuntimeState {
    if current_state.status != NodeRuntimeStatus::Pending {
        return current_state.clone();
    }

    if let Err(err) = deps_ready(node, node_states) {
        return NodeRuntimeState {
            status: NodeRuntimeStatus::Pending,
            last_error: Some(err),
        };
    }

    if let Err(err) = stage_order_ready(node, stage_states, contract) {
        return NodeRuntimeState {
            status: NodeRuntimeStatus::Pending,
            last_error: Some(err),
        };
    }

    if let Err(err) = refs_ready(run_id, node, state_store) {
        return NodeRuntimeState {
            status: NodeRuntimeStatus::Pending,
            last_error: Some(err),
        };
    }

    NodeRuntimeState {
        status: NodeRuntimeStatus::Ready,
        last_error: None,
    }
}

fn deps_ready(
    node: &NodeSpec,
    node_states: &HashMap<String, NodeRuntimeState>,
) -> Result<(), String> {
    for dep in &node.deps {
        let Some(state) = node_states.get(dep) else {
            return Err(format!("dependency {dep} is missing state"));
        };
        if state.status != NodeRuntimeStatus::Succeeded {
            return Err(format!("dependency {dep} is not succeeded"));
        }
    }
    Ok(())
}

fn stage_order_ready(
    node: &NodeSpec,
    stage_states: &HashMap<String, StageRuntimeState>,
    contract: &ValidatedContractManifest,
) -> Result<(), String> {
    let stage_id =
        node_stage_id(&node.node_id).ok_or_else(|| "node stage_id is missing".to_string())?;
    let Some(position) = contract
        .stage_order
        .iter()
        .position(|stage| stage == stage_id)
    else {
        return Err(format!("stage {stage_id} is not in contract order"));
    };
    for upstream in &contract.stage_order[..position] {
        let Some(stage_state) = stage_states.get(upstream) else {
            return Err(format!("upstream stage {upstream} state is missing"));
        };
        if stage_state.status != StageRuntimeStatus::Committed {
            return Err(format!("upstream stage {upstream} is not committed"));
        }
    }
    Ok(())
}

fn refs_ready(run_id: &str, node: &NodeSpec, state_store: &dyn StateStore) -> Result<(), String> {
    let refs = collect_exec_value_refs(run_id, &node.exec)?;
    for value_ref in refs {
        resolve_value_ref(&value_ref, state_store)
            .map_err(|err| format!("ref {:?} resolution failed: {err:?}", value_ref.ref_type))?;
    }
    Ok(())
}

fn collect_exec_value_refs(run_id: &str, exec: &Value) -> Result<Vec<ValueRef>, String> {
    let mut refs = Vec::new();
    if let Some(tool_exec) = exec.get("tool")
        && let Some(input) = tool_exec.get("input")
    {
        collect_input_spec_refs(run_id, input, &mut refs)?;
    }
    if let Some(llm_exec) = exec.get("llm") {
        let messages = llm_exec
            .get("messages")
            .and_then(|value| value.as_array())
            .ok_or_else(|| "llm.messages is missing".to_string())?;
        for message in messages {
            let parts = message
                .get("parts")
                .and_then(|value| value.as_array())
                .ok_or_else(|| "llm.message.parts is missing".to_string())?;
            for part in parts {
                if let Some(value_ref) = part.get("ref") {
                    refs.push(parse_value_ref(run_id, value_ref)?);
                }
            }
        }
    }
    Ok(refs)
}

fn collect_input_spec_refs(
    run_id: &str,
    input_spec: &Value,
    refs: &mut Vec<ValueRef>,
) -> Result<(), String> {
    let Some(_obj) = input_spec.as_object() else {
        return Err("input spec must be object".to_string());
    };
    collect_value_refs(run_id, input_spec, refs)?;
    Ok(())
}

fn collect_value_refs(run_id: &str, value: &Value, refs: &mut Vec<ValueRef>) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            if let Some(value_ref) = map.get("ref") {
                refs.push(parse_value_ref(run_id, value_ref)?);
            }
            for entry in map.values() {
                collect_value_refs(run_id, entry, refs)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_value_refs(run_id, item, refs)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_value_ref(run_id: &str, value: &Value) -> Result<ValueRef, String> {
    let Some(obj) = value.as_object() else {
        return Err("ref must be object".to_string());
    };
    let ref_type = obj
        .get("ref_type")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "ref_type is missing".to_string())?;
    let ref_type = match ref_type {
        "RUN_STATE" => ValueRefType::RunState,
        "STAGE_STATE" => ValueRefType::StageState,
        "NODE_OUTPUT" => ValueRefType::NodeOutput,
        "NODE_ARTIFACT" => ValueRefType::NodeArtifact,
        other => return Err(format!("unknown ref_type {other}")),
    };
    let json_pointer = obj
        .get("json_pointer")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let stage_id = obj
        .get("stage_id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let node_id = obj
        .get("node_id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let artifact_index = obj
        .get("artifact_index")
        .and_then(|value| value.as_u64())
        .map(|value| value as usize);
    let render_as = obj
        .get("render_as")
        .and_then(|value| value.as_str())
        .map(|value| match value {
            "JSON_COMPACT" => Ok(RenderAs::JsonCompact),
            "JSON_PRETTY" => Ok(RenderAs::JsonPretty),
            "TEXT" => Ok(RenderAs::Text),
            other => Err(format!("unknown render_as {other}")),
        })
        .transpose()?;

    let json_pointer = match ref_type {
        ValueRefType::RunState | ValueRefType::StageState | ValueRefType::NodeOutput => {
            json_pointer.ok_or_else(|| "json_pointer is required".to_string())?
        }
        ValueRefType::NodeArtifact => json_pointer.unwrap_or_default(),
    };
    if matches!(ref_type, ValueRefType::StageState) && stage_id.is_none() {
        return Err("stage_id is required".to_string());
    }
    if matches!(
        ref_type,
        ValueRefType::NodeOutput | ValueRefType::NodeArtifact
    ) && node_id.is_none()
    {
        return Err("node_id is required".to_string());
    }
    if matches!(ref_type, ValueRefType::NodeArtifact) && artifact_index.is_none() {
        return Err("artifact_index is required".to_string());
    }

    Ok(ValueRef {
        run_id: run_id.to_string(),
        ref_type,
        json_pointer,
        stage_id,
        node_id,
        artifact_index,
        render_as,
    })
}

pub(crate) fn node_stage_id(node_id: &str) -> Option<&str> {
    let mut parts = node_id.split('/');
    parts.next().filter(|value| !value.is_empty())
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

fn parse_scheduler_tie_break(value: &Value) -> Result<SchedulerTieBreak, PolicyError> {
    let tie_break = value
        .get("scheduler")
        .and_then(|scheduler| scheduler.get("tie_break"))
        .and_then(|tie_break| tie_break.as_str())
        .ok_or_else(|| PolicyError::Schema("scheduler.tie_break is missing".to_string()))?;
    match tie_break {
        "FIFO" => Ok(SchedulerTieBreak::Fifo),
        "PRIORITY" => Ok(SchedulerTieBreak::Priority),
        "SHORTEST_ESTIMATED_FIRST" => Ok(SchedulerTieBreak::ShortestEstimatedFirst),
        "LONGEST_ESTIMATED_FIRST" => Ok(SchedulerTieBreak::LongestEstimatedFirst),
        other => Err(PolicyError::Schema(format!(
            "unknown scheduler.tie_break {other}"
        ))),
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

#[derive(Debug)]
pub enum StartNodeError {
    MissingStageId,
    InvalidExec(String),
    ResourceManager(ResourceManagerError),
}

pub struct StartNodeContext<'a> {
    pub run_id: &'a str,
    pub node: &'a NodeSpec,
    pub node_states: &'a mut HashMap<String, NodeRuntimeState>,
    pub resource_manager: &'a ResourceManager,
    pub event_log: &'a dyn EventLog,
    pub log_store: &'a dyn LogStore,
    pub state_store: &'a dyn StateStore,
    pub worker: &'a mut WorkerClient,
    pub budget: InvocationBudget,
    pub trace_context: Option<TraceContext>,
}

pub async fn start_ready_node(
    context: StartNodeContext<'_>,
) -> Result<Option<NodeRuntimeStatus>, StartNodeError> {
    let StartNodeContext {
        run_id,
        node,
        node_states,
        resource_manager,
        event_log,
        log_store,
        state_store,
        worker,
        budget,
        trace_context,
    } = context;
    let current = node_states
        .get(&node.node_id)
        .cloned()
        .unwrap_or(NodeRuntimeState {
            status: NodeRuntimeStatus::Pending,
            last_error: None,
        });
    if current.status != NodeRuntimeStatus::Ready {
        return Ok(None);
    }

    let scheduling = SchedulingProfile::from_node(node);
    let reservation = resource_manager
        .try_reserve(
            &scheduling.resource_requests,
            &scheduling.resource_alternatives,
        )
        .map_err(StartNodeError::ResourceManager)?;
    let Some(_reservation) = reservation else {
        return Ok(None);
    };

    let stage_id = node_stage_id(&node.node_id).ok_or(StartNodeError::MissingStageId)?;
    let start_log = LogRecord {
        ts: Some(now_timestamp()),
        level: "INFO".to_string(),
        message: "node_start".to_string(),
        trace_id_hex: "".to_string(),
        span_id_hex: "".to_string(),
        scope_id: "".to_string(),
        scope_seq: 0,
        attrs: Default::default(),
    };
    let start_log = log_store.append_log(run_id, stage_id, &node.node_id, start_log);
    event_log
        .append(RunEvent {
            event_seq: 0,
            ts: start_log.ts,
            event: Some(run_event::Event::Log(start_log)),
        })
        .await;

    append_node_state(
        event_log,
        stage_id,
        &node.node_id,
        NodeStatus::NodeReady,
        NodeStatus::NodeRunning,
        None,
    )
    .await;
    node_states.insert(
        node.node_id.clone(),
        NodeRuntimeState {
            status: NodeRuntimeStatus::Running,
            last_error: None,
        },
    );

    let invocation = InvocationContext {
        run_id,
        stage_id,
        node_id: &node.node_id,
        event_log,
        log_store,
        state_store,
        budget,
        trace_context,
    };
    let request = build_tool_request(run_id, node)?;
    let result = match worker.invoke_tool_stream(request, &invocation).await {
        Ok(_) => NodeRuntimeState {
            status: NodeRuntimeStatus::Succeeded,
            last_error: None,
        },
        Err(status) => NodeRuntimeState {
            status: NodeRuntimeStatus::Failed,
            last_error: Some(format!("gRPC {:?}: {}", status.code(), status.message())),
        },
    };
    node_states.insert(node.node_id.clone(), result.clone());
    Ok(Some(result.status))
}

fn build_tool_request(run_id: &str, node: &NodeSpec) -> Result<InvokeToolRequest, StartNodeError> {
    let tool = node
        .exec
        .get("tool")
        .and_then(|value| value.as_object())
        .ok_or_else(|| StartNodeError::InvalidExec("exec.tool is missing".to_string()))?;
    let tool_name = tool
        .get("tool_name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| StartNodeError::InvalidExec("tool_name is missing".to_string()))?;
    let tool_version = tool
        .get("tool_version")
        .and_then(|value| value.as_str())
        .ok_or_else(|| StartNodeError::InvalidExec("tool_version is missing".to_string()))?;
    let idempotency_key = tool
        .get("idempotency_key")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let input = parse_tool_input(tool.get("input"))?;
    let invocation_id = format!(
        "{run_id}:{}:{}",
        node.node_id,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default()
    );
    Ok(InvokeToolRequest {
        invocation_id,
        tool_name: tool_name.to_string(),
        tool_version: tool_version.to_string(),
        input,
        deadline: None,
        idempotency_key,
        trace_context: None,
    })
}

fn parse_tool_input(value: Option<&Value>) -> Result<Option<Payload>, StartNodeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(literal) = value.get("literal").and_then(|value| value.as_object()) {
        let content_type = literal
            .get("content_type")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                StartNodeError::InvalidExec("literal.content_type is missing".to_string())
            })?;
        let data_base64 = literal
            .get("data_base64")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                StartNodeError::InvalidExec("literal.data_base64 is missing".to_string())
            })?;
        let data = general_purpose::STANDARD
            .decode(data_base64.as_bytes())
            .map_err(|err| StartNodeError::InvalidExec(format!("base64 error: {err}")))?;
        return Ok(Some(Payload {
            content_type: content_type.to_string(),
            data,
            encoding: "".to_string(),
            sha256: None,
        }));
    }
    let data = serde_json::to_vec(value)
        .map_err(|err| StartNodeError::InvalidExec(format!("input json error: {err}")))?;
    Ok(Some(Payload {
        content_type: "application/json".to_string(),
        data,
        encoding: "".to_string(),
        sha256: None,
    }))
}

async fn append_node_state(
    event_log: &dyn EventLog,
    stage_id: &str,
    node_id: &str,
    old_status: NodeStatus,
    new_status: NodeStatus,
    reason: Option<String>,
) {
    let event = RunEvent {
        event_seq: 0,
        ts: Some(now_timestamp()),
        event: Some(run_event::Event::NodeState(NodeStateChanged {
            stage_id: stage_id.to_string(),
            node_id: node_id.to_string(),
            old_status: old_status as i32,
            new_status: new_status as i32,
            reason: reason.unwrap_or_default(),
        })),
    };
    event_log.append(event).await;
}

fn now_timestamp() -> Timestamp {
    system_time_to_timestamp(SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::resource::{ConcurrencyLimits, ResourceManager};
    use crate::worker::client::WorkerClient;
    use cork_proto::cork::v1::cork_worker_server::{CorkWorker, CorkWorkerServer};
    use cork_proto::cork::v1::{InvokeToolRequest, InvokeToolResponse, InvokeToolStreamChunk};
    use cork_store::{CreateRunInput, InMemoryRunRegistry, InMemoryStateStore, RunFilters};
    use cork_store::{InMemoryEventLog, InMemoryLogStore};
    use serde_json::json;
    use std::pin::Pin;
    use tokio::net::TcpListener;
    use tokio_stream::Stream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    #[tokio::test]
    async fn run_ctx_to_response_maps_fields() {
        let registry = InMemoryRunRegistry::new();
        let run = registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp".to_string()),
                variant_id: Some("var".to_string()),
                status: Some(RunStatus::RunRunning),
                ..Default::default()
            })
            .await;

        let response = run_ctx_to_response(&run).await;
        assert_eq!(response.handle.unwrap().run_id, run.run_id());
        assert_eq!(response.status, RunStatus::RunRunning as i32);
        assert!(response.created_at.is_some());
        assert!(response.updated_at.is_some());
    }

    #[tokio::test]
    async fn get_run_response_returns_none_when_missing() {
        let registry = InMemoryRunRegistry::new();
        assert!(get_run_response(&registry, "missing").await.is_none());
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

    #[tokio::test]
    async fn list_runs_filters_in_registry() {
        let registry = InMemoryRunRegistry::new();
        registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp-1".to_string()),
                status: Some(RunStatus::RunPending),
                ..Default::default()
            })
            .await;
        registry
            .create_run(CreateRunInput {
                experiment_id: Some("exp-2".to_string()),
                status: Some(RunStatus::RunPending),
                ..Default::default()
            })
            .await;

        let page = registry
            .list_runs(
                None,
                RunFilters {
                    experiment_id: Some("exp-1".to_string()),
                    variant_id: None,
                    status: Some(RunStatus::RunPending),
                },
                10,
            )
            .await;
        assert_eq!(page.runs.len(), 1);
        assert_eq!(
            page.runs[0].metadata().await.experiment_id.as_deref(),
            Some("exp-1")
        );
    }

    fn node_spec(node_id: &str, deps: Vec<&str>, exec: Value) -> NodeSpec {
        NodeSpec {
            node_id: node_id.to_string(),
            kind: "tool".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec,
            deps: deps.into_iter().map(str::to_string).collect(),
            scheduling: None,
            htn: None,
            ttl_ms: None,
        }
    }

    fn tool_exec_with_ref(ref_value: Value) -> Value {
        json!({
            "tool": {
                "tool_name": "tool-a",
                "tool_version": "v1",
                "side_effect": "NONE",
                "input": { "ref": ref_value }
            }
        })
    }

    fn run_state_ref_value(pointer: &str) -> Value {
        json!({
            "ref_type": "RUN_STATE",
            "json_pointer": pointer
        })
    }

    fn pending_state() -> NodeRuntimeState {
        NodeRuntimeState {
            status: NodeRuntimeStatus::Pending,
            last_error: None,
        }
    }

    #[test]
    fn evaluate_node_readiness_blocks_on_unsatisfied_deps() {
        let node = node_spec(
            "stage-a/node-b",
            vec!["stage-a/node-a"],
            tool_exec_with_ref(run_state_ref_value("/input")),
        );
        let mut node_states = HashMap::new();
        node_states.insert(
            "stage-a/node-a".to_string(),
            NodeRuntimeState {
                status: NodeRuntimeStatus::Pending,
                last_error: None,
            },
        );
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

        let next = evaluate_node_readiness(
            "run-1",
            &node,
            &pending_state(),
            &node_states,
            &stage_states,
            &contract,
            &store,
        );
        assert_eq!(next.status, NodeRuntimeStatus::Pending);
        assert!(
            next.last_error
                .as_deref()
                .unwrap_or_default()
                .contains("dependency")
        );
    }

    #[test]
    fn evaluate_node_readiness_blocks_on_unresolved_refs() {
        let node = node_spec(
            "stage-a/node-b",
            Vec::new(),
            tool_exec_with_ref(run_state_ref_value("/missing")),
        );
        let node_states = HashMap::new();
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

        let next = evaluate_node_readiness(
            "run-1",
            &node,
            &pending_state(),
            &node_states,
            &stage_states,
            &contract,
            &store,
        );
        assert_eq!(next.status, NodeRuntimeStatus::Pending);
        assert!(
            next.last_error
                .as_deref()
                .unwrap_or_default()
                .contains("resolution failed")
        );
    }

    #[test]
    fn evaluate_node_readiness_blocks_on_nested_unresolved_refs() {
        let node = node_spec(
            "stage-a/node-b",
            Vec::new(),
            json!({
                "tool": {
                    "tool_name": "tool-a",
                    "tool_version": "v1",
                    "side_effect": "NONE",
                    "input": {
                        "query": {
                            "ref": run_state_ref_value("/missing")
                        },
                        "filters": [
                            "static",
                            { "ref": run_state_ref_value("/also-missing") }
                        ]
                    }
                }
            }),
        );
        let node_states = HashMap::new();
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

        let next = evaluate_node_readiness(
            "run-1",
            &node,
            &pending_state(),
            &node_states,
            &stage_states,
            &contract,
            &store,
        );
        assert_eq!(next.status, NodeRuntimeStatus::Pending);
        assert!(
            next.last_error
                .as_deref()
                .unwrap_or_default()
                .contains("resolution failed")
        );
    }

    #[test]
    fn evaluate_node_readiness_blocks_on_uncommitted_stage() {
        let node = node_spec(
            "stage-b/node-b",
            Vec::new(),
            tool_exec_with_ref(run_state_ref_value("/input")),
        );
        let node_states = HashMap::new();
        let stage_states = HashMap::from([
            (
                "stage-a".to_string(),
                StageRuntimeState {
                    status: StageRuntimeStatus::Active,
                },
            ),
            (
                "stage-b".to_string(),
                StageRuntimeState {
                    status: StageRuntimeStatus::Pending,
                },
            ),
        ]);
        let contract = ValidatedContractManifest {
            stage_order: vec!["stage-a".to_string(), "stage-b".to_string()],
        };
        let store = InMemoryStateStore::new();

        let next = evaluate_node_readiness(
            "run-1",
            &node,
            &pending_state(),
            &node_states,
            &stage_states,
            &contract,
            &store,
        );
        assert_eq!(next.status, NodeRuntimeStatus::Pending);
        assert!(
            next.last_error
                .as_deref()
                .unwrap_or_default()
                .contains("upstream stage")
        );
    }

    #[test]
    fn evaluate_node_readiness_sets_ready_when_all_checks_pass() {
        let node = node_spec(
            "stage-a/node-b",
            vec!["stage-a/node-a"],
            tool_exec_with_ref(run_state_ref_value("/input")),
        );
        let mut node_states = HashMap::new();
        node_states.insert(
            "stage-a/node-a".to_string(),
            NodeRuntimeState {
                status: NodeRuntimeStatus::Succeeded,
                last_error: None,
            },
        );
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
        store
            .apply_state_put("run-1", "RUN", None, "/input", json!("ok"))
            .expect("state put");

        let next = evaluate_node_readiness(
            "run-1",
            &node,
            &pending_state(),
            &node_states,
            &stage_states,
            &contract,
            &store,
        );
        assert_eq!(next.status, NodeRuntimeStatus::Ready);
        assert!(next.last_error.is_none());
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
        assert_eq!(validated.scheduler_tie_break, SchedulerTieBreak::Fifo);
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

    struct TestWorker;

    #[tonic::async_trait]
    impl CorkWorker for TestWorker {
        async fn health(
            &self,
            _request: Request<cork_proto::cork::v1::HealthRequest>,
        ) -> Result<Response<cork_proto::cork::v1::HealthResponse>, Status> {
            Ok(Response::new(cork_proto::cork::v1::HealthResponse {
                status: "ok".to_string(),
                worker_id: "worker-1".to_string(),
            }))
        }

        async fn invoke_tool(
            &self,
            _request: Request<InvokeToolRequest>,
        ) -> Result<Response<InvokeToolResponse>, Status> {
            Ok(Response::new(InvokeToolResponse {
                output: Some(Payload {
                    content_type: "text/plain".to_string(),
                    data: b"ok".to_vec(),
                    encoding: "".to_string(),
                    sha256: None,
                }),
                artifacts: Vec::new(),
                error_code: "".to_string(),
                error_message: "".to_string(),
            }))
        }

        type InvokeToolStreamStream =
            Pin<Box<dyn Stream<Item = Result<InvokeToolStreamChunk, Status>> + Send>>;

        async fn invoke_tool_stream(
            &self,
            _request: Request<InvokeToolRequest>,
        ) -> Result<Response<Self::InvokeToolStreamStream>, Status> {
            let response = InvokeToolResponse {
                output: Some(Payload {
                    content_type: "text/plain".to_string(),
                    data: b"ok".to_vec(),
                    encoding: "".to_string(),
                    sha256: None,
                }),
                artifacts: Vec::new(),
                error_code: "".to_string(),
                error_message: "".to_string(),
            };
            let chunks = vec![Ok(InvokeToolStreamChunk {
                ts: None,
                chunk: Some(cork_proto::cork::v1::invoke_tool_stream_chunk::Chunk::Final(response)),
            })];
            Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
        }
    }

    async fn start_test_worker() -> (tonic::transport::Channel, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let incoming = TcpListenerStream::new(listener);
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CorkWorkerServer::new(TestWorker))
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .expect("channel")
            .connect()
            .await
            .expect("connect");
        (channel, handle)
    }

    #[tokio::test]
    async fn start_ready_node_emits_state_events_in_order() {
        let (channel, handle) = start_test_worker().await;
        let mut worker = WorkerClient::new(channel);
        let event_log = InMemoryEventLog::new();
        let log_store = InMemoryLogStore::new();
        let state_store = InMemoryStateStore::new();
        let resource_manager = ResourceManager::new(
            ConcurrencyLimits {
                cpu_max: 1,
                io_max: 1,
                per_provider_max: 1,
            },
            Vec::new(),
            Vec::new(),
        )
        .expect("resource manager");

        let node = NodeSpec {
            node_id: "stage-a/node-a".to_string(),
            kind: "TOOL".to_string(),
            anchor_position: "WITHIN".to_string(),
            exec: json!({
                "tool": {
                    "tool_name": "tool-a",
                    "tool_version": "v1",
                    "side_effect": "NONE",
                    "input": { "literal": { "content_type": "application/json", "data_base64": "e30=" } }
                }
            }),
            deps: Vec::new(),
            scheduling: Some(json!({
                "resources": [{ "resource_id": "cpu", "amount": 1 }]
            })),
            htn: None,
            ttl_ms: None,
        };
        let mut node_states = HashMap::new();
        node_states.insert(
            node.node_id.clone(),
            NodeRuntimeState {
                status: NodeRuntimeStatus::Ready,
                last_error: None,
            },
        );

        let result = start_ready_node(StartNodeContext {
            run_id: "run-1",
            node: &node,
            node_states: &mut node_states,
            resource_manager: &resource_manager,
            event_log: &event_log,
            log_store: &log_store,
            state_store: &state_store,
            worker: &mut worker,
            budget: InvocationBudget::default(),
            trace_context: None,
        })
        .await
        .expect("start");
        assert_eq!(result, Some(NodeRuntimeStatus::Succeeded));
        assert_eq!(
            node_states.get(&node.node_id).expect("state").status,
            NodeRuntimeStatus::Succeeded
        );

        let events = event_log.subscribe(0).await.backlog;
        let node_events: Vec<NodeStateChanged> = events
            .into_iter()
            .filter_map(|event| match event.event {
                Some(run_event::Event::NodeState(node_state)) => Some(node_state),
                _ => None,
            })
            .collect();
        assert_eq!(node_events.len(), 2);
        assert_eq!(node_events[0].old_status, NodeStatus::NodeReady as i32);
        assert_eq!(node_events[0].new_status, NodeStatus::NodeRunning as i32);
        assert_eq!(node_events[1].old_status, NodeStatus::NodeRunning as i32);
        assert_eq!(node_events[1].new_status, NodeStatus::NodeSucceeded as i32);

        handle.abort();
    }
}
