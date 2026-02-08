use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use cork_core::api::{CorkCoreService, CorkCoreServiceConfig, DEFAULT_GRPC_MAX_MESSAGE_BYTES};
use cork_core::engine::autocommit::tick_stage_auto_commit;
use cork_core::engine::run::{
    NodeRuntimeState, NodeRuntimeStatus, StageRuntimeState, StageRuntimeStatus,
    validate_contract_manifest,
};
use cork_core::shutdown::{ShutdownMode, ShutdownPolicy};
use cork_core::worker::client::{InvocationBudget, InvocationContext, WorkerClient};
use cork_hash::{composite_graph_hash, contract_hash, patch_hash, sha256};
use cork_proto::cork::v1::cork_core_server::CorkCore;
use cork_proto::cork::v1::cork_worker_server::{CorkWorker, CorkWorkerServer};
use cork_proto::cork::v1::{
    ApplyGraphPatchRequest, CanonicalJsonDocument, GetCompositeGraphRequest, GetLogsRequest,
    GetRunRequest, InvokeToolRequest, InvokeToolResponse, InvokeToolStreamChunk, LogRecord,
    Payload, RunHandle, RunStatus, Sha256, StreamRunEventsRequest, SubmitRunRequest,
};
use cork_store::{
    CircuitBreakerPolicy, CreateRunInput, EventLog, InMemoryEventLog, InMemoryLogStore,
    InMemoryRunRegistry, InMemoryStateStore, RetryBackoff, RetryBackoffPolicy, RetryJitter,
    RunRegistry, WorkerPolicy, WorkerRetryPolicy,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

struct TestWorker;

#[tonic::async_trait]
impl CorkWorker for TestWorker {
    async fn health(
        &self,
        _request: Request<cork_proto::cork::v1::HealthRequest>,
    ) -> Result<Response<cork_proto::cork::v1::HealthResponse>, Status> {
        Ok(Response::new(cork_proto::cork::v1::HealthResponse {
            status: "ok".to_string(),
            worker_id: "worker-e2e".to_string(),
        }))
    }

    async fn invoke_tool(
        &self,
        _request: Request<InvokeToolRequest>,
    ) -> Result<Response<InvokeToolResponse>, Status> {
        Ok(Response::new(InvokeToolResponse {
            output: Some(Payload {
                content_type: "application/json".to_string(),
                data: br#"{"ok":true}"#.to_vec(),
                encoding: "".to_string(),
                sha256: None,
            }),
            artifacts: Vec::new(),
            error_code: "".to_string(),
            error_message: "".to_string(),
        }))
    }

    type InvokeToolStreamStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<InvokeToolStreamChunk, Status>> + Send>>;

    async fn invoke_tool_stream(
        &self,
        _request: Request<InvokeToolRequest>,
    ) -> Result<Response<Self::InvokeToolStreamStream>, Status> {
        let log = LogRecord {
            ts: None,
            level: "INFO".to_string(),
            message: "stream-log".to_string(),
            trace_id_hex: "".to_string(),
            span_id_hex: "".to_string(),
            scope_id: "".to_string(),
            scope_seq: 0,
            attrs: Default::default(),
        };
        let chunks = vec![
            Ok(InvokeToolStreamChunk {
                ts: None,
                chunk: Some(cork_proto::cork::v1::invoke_tool_stream_chunk::Chunk::Log(
                    log,
                )),
            }),
            Ok(InvokeToolStreamChunk {
                ts: None,
                chunk: Some(
                    cork_proto::cork::v1::invoke_tool_stream_chunk::Chunk::Final(
                        InvokeToolResponse {
                            output: Some(Payload {
                                content_type: "application/json".to_string(),
                                data: br#"{"ok":true}"#.to_vec(),
                                encoding: "".to_string(),
                                sha256: None,
                            }),
                            artifacts: Vec::new(),
                            error_code: "".to_string(),
                            error_message: "".to_string(),
                        },
                    ),
                ),
            }),
        ];
        Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
    }
}

async fn start_worker_server() -> (tonic::transport::Channel, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
    let addr = listener.local_addr().expect("worker addr");
    let incoming = TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                CorkWorkerServer::new(TestWorker)
                    .max_decoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(incoming)
            .await
            .expect("serve worker");
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("worker channel")
        .connect()
        .await
        .expect("worker connect");
    (channel, handle)
}

struct FlakyWorker {
    remaining_failures: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl CorkWorker for FlakyWorker {
    async fn health(
        &self,
        _request: Request<cork_proto::cork::v1::HealthRequest>,
    ) -> Result<Response<cork_proto::cork::v1::HealthResponse>, Status> {
        Ok(Response::new(cork_proto::cork::v1::HealthResponse {
            status: "ok".to_string(),
            worker_id: "worker-flaky".to_string(),
        }))
    }

    async fn invoke_tool(
        &self,
        _request: Request<InvokeToolRequest>,
    ) -> Result<Response<InvokeToolResponse>, Status> {
        Err(Status::unavailable("not implemented"))
    }

    type InvokeToolStreamStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<InvokeToolStreamChunk, Status>> + Send>>;

    async fn invoke_tool_stream(
        &self,
        _request: Request<InvokeToolRequest>,
    ) -> Result<Response<Self::InvokeToolStreamStream>, Status> {
        let remaining = self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_sub(1))
            })
            .unwrap_or(0);
        if remaining > 0 {
            return Err(Status::unavailable("temporary outage"));
        }
        let response = InvokeToolResponse {
            output: Some(Payload {
                content_type: "application/json".to_string(),
                data: br#"{"ok":true}"#.to_vec(),
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

async fn start_flaky_worker(
    failures: usize,
) -> (tonic::transport::Channel, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
    let addr = listener.local_addr().expect("worker addr");
    let incoming = TcpListenerStream::new(listener);
    let remaining_failures = Arc::new(AtomicUsize::new(failures));
    let worker = FlakyWorker { remaining_failures };
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                CorkWorkerServer::new(worker)
                    .max_decoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(DEFAULT_GRPC_MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(incoming)
            .await
            .expect("serve worker");
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("worker channel")
        .connect()
        .await
        .expect("worker connect");
    (channel, handle)
}

fn test_worker_policy(max_attempts: u32) -> WorkerPolicy {
    WorkerPolicy {
        timeout_ms: Some(5_000),
        retry: WorkerRetryPolicy {
            max_attempts,
            backoff: RetryBackoff {
                policy: RetryBackoffPolicy::Exponential,
                base_delay_ms: 1,
                max_delay_ms: 5,
                jitter: RetryJitter::None,
            },
            retry_on: vec![
                cork_store::WorkerRetryCondition::GrpcUnavailable,
                cork_store::WorkerRetryCondition::Timeout,
            ],
            circuit_breaker: CircuitBreakerPolicy {
                enabled: false,
                open_after_failures: 3,
                half_open_after_ms: 1_000,
            },
        },
    }
}

fn build_sha256(bytes: &[u8; 32]) -> Sha256 {
    Sha256 {
        bytes32: bytes.to_vec(),
    }
}

fn canonical_doc(value: Value, schema_id: &str) -> CanonicalJsonDocument {
    let canonical_json_utf8 = serde_json::to_vec(&value).expect("serialize json");
    let digest = sha256(&canonical_json_utf8);
    CanonicalJsonDocument {
        canonical_json_utf8,
        sha256: Some(build_sha256(&digest)),
        schema_id: schema_id.to_string(),
    }
}

fn build_contract_manifest() -> Value {
    json!({
        "schema_version": "cork.contract_manifest.v0.1",
        "manifest_id": "manifest-001",
        "defaults": {
            "expansion_policy": {
                "allow_dynamic": false,
                "allow_kinds": ["TOOL"],
                "max_dynamic_nodes": 0,
                "max_steps_in_stage": 1,
                "allow_cross_stage_deps": "NONE"
            },
            "stage_budget": {},
            "stage_ttl": {},
            "completion_policy": {
                "fail_on_any_failure": false,
                "require_commit": true
            }
        },
        "contract_graph": {
            "stages": [
                {
                    "stage_id": "stage1",
                    "expansion_policy": {
                        "allow_dynamic": true,
                        "allow_kinds": ["TOOL"],
                        "max_dynamic_nodes": 2,
                        "max_steps_in_stage": 2,
                        "allow_cross_stage_deps": "NONE"
                    }
                },
                {
                    "stage_id": "stage2",
                    "dependencies": [
                        { "stage_id": "stage1", "constraint": "HARD" }
                    ],
                    "expansion_policy": {
                        "allow_dynamic": false,
                        "allow_kinds": ["TOOL"],
                        "max_dynamic_nodes": 0,
                        "max_steps_in_stage": 1,
                        "allow_cross_stage_deps": "NONE"
                    }
                }
            ]
        }
    })
}

fn build_contract_manifest_defaults_only() -> Value {
    json!({
        "schema_version": "cork.contract_manifest.v0.1",
        "manifest_id": "manifest-002",
        "defaults": {
            "expansion_policy": {
                "allow_dynamic": true,
                "allow_kinds": ["TOOL"],
                "max_dynamic_nodes": 2,
                "max_steps_in_stage": 2,
                "allow_cross_stage_deps": "NONE"
            },
            "stage_budget": {},
            "stage_ttl": {},
            "completion_policy": {
                "fail_on_any_failure": false,
                "require_commit": true
            }
        },
        "contract_graph": {
            "stages": [
                {
                    "stage_id": "stage1"
                }
            ]
        }
    })
}

fn build_policy() -> Value {
    json!({
        "schema_version": "cork.policy.v0.1",
        "policy_id": "policy-001",
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
            "quiescence_ms": 1,
            "max_open_ms": 1000,
            "exclude_when_waiting": false
        },
        "resource_pools": [
            {
                "resource_id": "pool-1",
                "capacity": 1,
                "kind": "CUMULATIVE",
                "tags": ["batch"]
            }
        ]
    })
}

fn build_patch(run_id: &str, patch_seq: u64, stage_id: &str) -> CanonicalJsonDocument {
    let ops = json!([
        {
            "op_type": "NODE_ADDED",
            "node_added": {
                "node": {
                    "node_id": "stage1/tool-1",
                    "kind": "TOOL",
                    "anchor_position": "WITHIN",
                    "deps": [],
                    "exec": {
                        "tool": {
                            "tool_name": "echo",
                            "tool_version": "v1",
                            "input": {
                                "literal": {
                                    "content_type": "application/json",
                                    "data_base64": "e30="
                                }
                            },
                            "side_effect": "NONE"
                        }
                    }
                }
            }
        }
    ]);
    let payload = json!({
        "schema_version": "cork.graph_patch.v0.1",
        "run_id": run_id,
        "patch_seq": patch_seq,
        "stage_id": stage_id,
        "ops": ops
    });
    canonical_doc(payload, "cork.graph_patch.v0.1")
}

#[tokio::test]
async fn e2e_submit_patch_execute_commit() {
    let (worker_channel, _worker_handle) = start_worker_server().await;
    let registry: Arc<dyn RunRegistry> = Arc::new(InMemoryRunRegistry::new());
    let service = CorkCoreService::with_run_registry(Arc::clone(&registry));

    let contract_json = build_contract_manifest();
    let policy_json = build_policy();
    let contract_doc = canonical_doc(contract_json.clone(), "cork.contract_manifest.v0.1");
    let policy_doc = canonical_doc(policy_json, "cork.policy.v0.1");

    let submit_request = SubmitRunRequest {
        contract_manifest: Some(contract_doc),
        policy: Some(policy_doc),
        initial_input: Some(Payload {
            content_type: "application/json".to_string(),
            data: br#"{}"#.to_vec(),
            encoding: "".to_string(),
            sha256: None,
        }),
        experiment_id: "exp-1".to_string(),
        variant_id: "var-1".to_string(),
        trace_context: None,
    };
    let submit_response = service
        .submit_run(Request::new(submit_request))
        .await
        .expect("submit run")
        .into_inner();
    let handle = submit_response.handle.expect("handle");
    let run_id = handle.run_id.clone();

    let patch_doc = build_patch(&run_id, 0, "stage1");
    let patch_request = ApplyGraphPatchRequest {
        handle: Some(handle.clone()),
        patch: Some(patch_doc),
        actor_id: "actor-1".to_string(),
    };
    let patch_response = service
        .apply_graph_patch(Request::new(patch_request))
        .await
        .expect("apply patch")
        .into_inner();
    assert!(patch_response.accepted);

    let mut node_states = HashMap::new();
    node_states.insert(
        "stage1/tool-1".to_string(),
        NodeRuntimeState {
            status: NodeRuntimeStatus::Pending,
            last_error: None,
        },
    );
    let mut stage_states = HashMap::new();
    stage_states.insert(
        "stage1".to_string(),
        StageRuntimeState {
            status: StageRuntimeStatus::Active,
        },
    );
    stage_states.insert(
        "stage2".to_string(),
        StageRuntimeState {
            status: StageRuntimeStatus::Pending,
        },
    );

    let mut worker_client = WorkerClient::new(worker_channel);
    let event_log = service.event_log_for_run(&run_id);
    let log_store = service.log_store();
    let state_store = service.state_store();
    let context = InvocationContext {
        run_id: &run_id,
        stage_id: "stage1",
        node_id: "stage1/tool-1",
        run_ctx: None,
        event_log: event_log.as_ref(),
        log_store: log_store.as_ref(),
        state_store: state_store.as_ref(),
        budget: InvocationBudget::default(),
        trace_context: None,
        worker_policy: None,
    };
    let invoke_request = InvokeToolRequest {
        invocation_id: "inv-1".to_string(),
        tool_name: "echo".to_string(),
        tool_version: "v1".to_string(),
        input: Some(Payload {
            content_type: "application/json".to_string(),
            data: br#"{}"#.to_vec(),
            encoding: "".to_string(),
            sha256: None,
        }),
        deadline: None,
        idempotency_key: "".to_string(),
        trace_context: None,
    };
    worker_client
        .invoke_tool_stream(invoke_request, &context)
        .await
        .expect("invoke tool");

    node_states.insert(
        "stage1/tool-1".to_string(),
        NodeRuntimeState {
            status: NodeRuntimeStatus::Succeeded,
            last_error: None,
        },
    );

    let contract = validate_contract_manifest(&contract_json).expect("validated contract");
    let graph_store = service.graph_store();
    let now = SystemTime::now();
    let past = now.checked_sub(Duration::from_millis(10)).expect("past");
    let run_ctx = registry.get_run(&run_id).await.expect("run ctx");
    run_ctx.set_stage_started_at(Some(past)).await;
    run_ctx.set_last_patch_at(Some(past)).await;
    let result = tick_stage_auto_commit(
        &run_ctx,
        &contract,
        &node_states,
        &mut stage_states,
        graph_store.as_ref(),
        event_log.as_ref(),
        now,
    )
    .await;
    assert!(result.is_some(), "stage1 auto-commit should fire");

    let now2 = SystemTime::now();
    let past2 = now2.checked_sub(Duration::from_millis(10)).expect("past2");
    run_ctx.set_stage_started_at(Some(past2)).await;
    run_ctx.set_last_patch_at(Some(past2)).await;
    let result = tick_stage_auto_commit(
        &run_ctx,
        &contract,
        &node_states,
        &mut stage_states,
        graph_store.as_ref(),
        event_log.as_ref(),
        now2,
    )
    .await;
    assert!(result.is_some(), "stage2 auto-commit should fire");

    run_ctx.set_status(RunStatus::RunSucceeded).await;

    let stream_response = service
        .stream_run_events(Request::new(StreamRunEventsRequest {
            handle: Some(RunHandle {
                run_id: run_id.clone(),
            }),
            since_event_seq: 0,
        }))
        .await
        .expect("stream events");
    let mut stream = stream_response.into_inner();
    let mut events = Vec::new();
    for _ in 0..5 {
        let event = stream.next().await.expect("event item").expect("event");
        events.push(event);
    }

    let sequences: Vec<i64> = events.iter().map(|event| event.event_seq).collect();
    assert_eq!(sequences, vec![0, 1, 2, 3, 4]);

    let patch_event = events.iter().find_map(|event| match &event.event {
        Some(cork_proto::cork::v1::run_event::Event::GraphPatch(patch)) => Some(patch),
        _ => None,
    });
    let patch_event = patch_event.expect("graph patch event");
    let patch_json: Value =
        serde_json::from_slice(&patch_event.canonical_json_utf8).expect("patch json");
    assert_eq!(patch_json["patch_seq"], 0);

    let log_event = events.iter().find_map(|event| match &event.event {
        Some(cork_proto::cork::v1::run_event::Event::Log(log)) => Some(log.clone()),
        _ => None,
    });
    let log_event = log_event.expect("log event");

    let logs_response = service
        .get_logs(Request::new(GetLogsRequest {
            handle: Some(handle.clone()),
            scope_id: log_event.scope_id.clone(),
            span_id_hex: "".to_string(),
            stage_id: "".to_string(),
            node_id: "".to_string(),
            page_size: 10,
            page_token: "".to_string(),
        }))
        .await
        .expect("get logs")
        .into_inner();
    assert_eq!(logs_response.logs.len(), 1);
    assert_eq!(logs_response.logs[0].scope_id, log_event.scope_id);
    assert_eq!(logs_response.logs[0].scope_seq, 0);

    let composite_response = service
        .get_composite_graph(Request::new(GetCompositeGraphRequest {
            handle: Some(handle.clone()),
        }))
        .await
        .expect("get composite graph")
        .into_inner();
    assert_eq!(composite_response.patches_in_order.len(), 1);
    let patch_json: Value =
        serde_json::from_slice(&composite_response.patches_in_order[0].canonical_json_utf8)
            .expect("patch json");
    assert_eq!(patch_json["patch_seq"], 0);

    let contract_digest = contract_hash(
        &composite_response
            .contract_manifest
            .unwrap()
            .canonical_json_utf8,
    );
    let patch_digest = patch_hash(&composite_response.patches_in_order[0].canonical_json_utf8);
    let composite_digest = composite_graph_hash(&contract_digest, &[patch_digest]);
    let composite_hash = composite_response
        .hashes
        .expect("hashes")
        .composite_graph_hash
        .expect("composite hash")
        .bytes32;
    assert_eq!(composite_hash, composite_digest.to_vec());

    let run_ctx = registry.get_run(&run_id).await.expect("run ctx");
    assert_eq!(run_ctx.metadata().await.status, RunStatus::RunSucceeded);
    assert_eq!(run_ctx.next_patch_seq().await, 1);
}

#[tokio::test]
async fn e2e_minimal_defaults_expansion_policy() {
    let registry: Arc<dyn RunRegistry> = Arc::new(InMemoryRunRegistry::new());
    let service = CorkCoreService::with_run_registry(Arc::clone(&registry));

    let contract_json = build_contract_manifest_defaults_only();
    let policy_json = build_policy();
    let contract_doc = canonical_doc(contract_json, "cork.contract_manifest.v0.1");
    let policy_doc = canonical_doc(policy_json, "cork.policy.v0.1");

    let submit_request = SubmitRunRequest {
        contract_manifest: Some(contract_doc),
        policy: Some(policy_doc),
        initial_input: Some(Payload {
            content_type: "application/json".to_string(),
            data: br#"{}"#.to_vec(),
            encoding: "".to_string(),
            sha256: None,
        }),
        experiment_id: "exp-2".to_string(),
        variant_id: "var-2".to_string(),
        trace_context: None,
    };
    let submit_response = service
        .submit_run(Request::new(submit_request))
        .await
        .expect("submit run")
        .into_inner();
    let handle = submit_response.handle.expect("handle");
    let run_id = handle.run_id.clone();

    let patch_doc = build_patch(&run_id, 0, "stage1");
    let patch_request = ApplyGraphPatchRequest {
        handle: Some(handle),
        patch: Some(patch_doc),
        actor_id: "actor-1".to_string(),
    };
    let patch_response = service
        .apply_graph_patch(Request::new(patch_request))
        .await
        .expect("apply patch")
        .into_inner();
    assert!(patch_response.accepted);
}

#[tokio::test]
async fn e2e_get_run_returns_policy_hash_bundle() {
    let registry: Arc<dyn RunRegistry> = Arc::new(InMemoryRunRegistry::new());
    let service = CorkCoreService::with_run_registry(Arc::clone(&registry));

    let contract_json = build_contract_manifest();
    let policy_json = build_policy();
    let contract_doc = canonical_doc(contract_json, "cork.contract_manifest.v0.1");
    let policy_doc = canonical_doc(policy_json, "cork.policy.v0.1");

    let submit_response = service
        .submit_run(Request::new(SubmitRunRequest {
            contract_manifest: Some(contract_doc),
            policy: Some(policy_doc.clone()),
            initial_input: Some(Payload {
                content_type: "application/json".to_string(),
                data: br#"{}"#.to_vec(),
                encoding: "".to_string(),
                sha256: None,
            }),
            experiment_id: "exp-policy".to_string(),
            variant_id: "var-policy".to_string(),
            trace_context: None,
        }))
        .await
        .expect("submit run")
        .into_inner();
    let handle = submit_response.handle.expect("handle");

    let get_response = service
        .get_run(Request::new(GetRunRequest {
            handle: Some(handle.clone()),
        }))
        .await
        .expect("get run")
        .into_inner();
    let policy = get_response.policy.expect("policy");
    assert_eq!(policy.schema_id, "cork.policy.v0.1");
    assert_eq!(policy.sha256, policy_doc.sha256);

    let run_policy_hash = get_response
        .hashes
        .as_ref()
        .and_then(|hashes| hashes.policy_hash.as_ref())
        .expect("run policy hash");
    let submit_policy_hash = submit_response
        .hashes
        .as_ref()
        .and_then(|hashes| hashes.policy_hash.as_ref())
        .expect("submit policy hash");
    assert_eq!(run_policy_hash.bytes32, submit_policy_hash.bytes32);

    let composite_response = service
        .get_composite_graph(Request::new(GetCompositeGraphRequest {
            handle: Some(handle),
        }))
        .await
        .expect("get composite graph")
        .into_inner();
    let composite_policy_hash = composite_response
        .hashes
        .as_ref()
        .and_then(|hashes| hashes.policy_hash.as_ref())
        .expect("composite policy hash");
    assert_eq!(composite_policy_hash.bytes32, submit_policy_hash.bytes32);
}

#[tokio::test]
async fn worker_retry_recovers_from_temporary_failure() {
    let (channel, handle) = start_flaky_worker(2).await;
    let mut client = WorkerClient::new(channel);
    let event_log = InMemoryEventLog::new();
    let log_store = InMemoryLogStore::new();
    let state_store = InMemoryStateStore::new();
    let context = InvocationContext {
        run_id: "run-retry-1",
        stage_id: "stage-retry",
        node_id: "stage-retry/node-retry",
        run_ctx: None,
        event_log: &event_log,
        log_store: &log_store,
        state_store: &state_store,
        budget: InvocationBudget::default(),
        trace_context: None,
        worker_policy: Some(test_worker_policy(4)),
    };
    let request = InvokeToolRequest {
        invocation_id: "inv-retry-1".to_string(),
        tool_name: "tool".to_string(),
        tool_version: "v1".to_string(),
        input: None,
        deadline: None,
        idempotency_key: "".to_string(),
        trace_context: None,
    };

    let response = client
        .invoke_tool_stream(request, &context)
        .await
        .expect("retry success");
    assert!(response.output.is_some());

    let events = event_log.subscribe(0).await.expect("subscribe").backlog;
    assert!(events.iter().any(|event| matches!(
        event.event.as_ref(),
        Some(cork_proto::cork::v1::run_event::Event::NodeState(
            cork_proto::cork::v1::NodeStateChanged { new_status, .. }
        )) if *new_status == cork_proto::cork::v1::NodeStatus::NodeSucceeded as i32
    )));

    handle.abort();
}

#[tokio::test]
async fn shutdown_cancels_active_runs_and_emits_policy_event() {
    let service = CorkCoreService::with_config(CorkCoreServiceConfig {
        shutdown_policy: ShutdownPolicy {
            mode: ShutdownMode::Cancel,
            drain_timeout: Duration::from_millis(1),
        },
        ..CorkCoreServiceConfig::default()
    });
    let run = service
        .create_run(CreateRunInput {
            status: Some(RunStatus::RunRunning),
            ..Default::default()
        })
        .await;
    let event_log = service.event_log_for_run(run.run_id());

    service.handle_shutdown().await;

    let metadata = run.metadata().await;
    assert_eq!(metadata.status, RunStatus::RunCancelled);
    let events = event_log.subscribe(0).await.expect("subscribe").backlog;
    assert!(events.iter().any(|event| matches!(
        event.event.as_ref(),
        Some(cork_proto::cork::v1::run_event::Event::Policy(
            cork_proto::cork::v1::PolicyEvent { kind, .. }
        )) if kind == "shutdown_cancel"
    )));
}

#[tokio::test]
async fn worker_retry_fails_after_exhaustion() {
    let (channel, handle) = start_flaky_worker(10).await;
    let mut client = WorkerClient::new(channel);
    let event_log = InMemoryEventLog::new();
    let log_store = InMemoryLogStore::new();
    let state_store = InMemoryStateStore::new();
    let context = InvocationContext {
        run_id: "run-retry-2",
        stage_id: "stage-retry",
        node_id: "stage-retry/node-retry",
        run_ctx: None,
        event_log: &event_log,
        log_store: &log_store,
        state_store: &state_store,
        budget: InvocationBudget::default(),
        trace_context: None,
        worker_policy: Some(test_worker_policy(2)),
    };
    let request = InvokeToolRequest {
        invocation_id: "inv-retry-2".to_string(),
        tool_name: "tool".to_string(),
        tool_version: "v1".to_string(),
        input: None,
        deadline: None,
        idempotency_key: "".to_string(),
        trace_context: None,
    };

    let result = client.invoke_tool_stream(request, &context).await;
    assert!(result.is_err());

    let events = event_log.subscribe(0).await.expect("subscribe").backlog;
    assert!(events.iter().any(|event| matches!(
        event.event.as_ref(),
        Some(cork_proto::cork::v1::run_event::Event::NodeState(
            cork_proto::cork::v1::NodeStateChanged { new_status, .. }
        )) if *new_status == cork_proto::cork::v1::NodeStatus::NodeFailed as i32
    )));

    handle.abort();
}
