//! CORK Schema validation library.
//!
//! This crate provides JSON Schema (Draft 2020-12) validation for CORK documents.

use jsonschema::draft202012;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    InvalidSchema(String),
    ValidationErrors(Vec<String>),
}

pub fn validate_contract_manifest(value: &Value) -> Result<(), SchemaError> {
    let schema = load_contract_manifest_schema()?;
    let validator =
        draft202012::new(&schema).map_err(|err| SchemaError::InvalidSchema(err.to_string()))?;
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|err| err.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(SchemaError::ValidationErrors(errors))
    }
}

pub fn validate_policy(value: &Value) -> Result<(), SchemaError> {
    let schema = load_policy_schema()?;
    let validator =
        draft202012::new(&schema).map_err(|err| SchemaError::InvalidSchema(err.to_string()))?;
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|err| err.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(SchemaError::ValidationErrors(errors))
    }
}

fn load_contract_manifest_schema() -> Result<Value, SchemaError> {
    let schema_str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../schemas/cork.contract_manifest.v0.1.schema.json"
    ));
    serde_json::from_str(schema_str).map_err(|err| SchemaError::InvalidSchema(err.to_string()))
}

fn load_policy_schema() -> Result<Value, SchemaError> {
    let schema_str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../schemas/cork.policy.v0.1.schema.json"
    ));
    serde_json::from_str(schema_str).map_err(|err| SchemaError::InvalidSchema(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validate_contract_manifest_accepts_valid_payload() {
        let manifest = json!({
            "schema_version": "cork.contract_manifest.v0.1",
            "manifest_id": "manifest-1234",
            "contract_graph": {
                "stages": [
                    {
                        "stage_id": "stage_a",
                        "dependencies": [
                            { "stage_id": "stage_b", "constraint": "HARD" }
                        ]
                    },
                    { "stage_id": "stage_b" }
                ]
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
        });

        assert!(validate_contract_manifest(&manifest).is_ok());
    }

    #[test]
    fn validate_contract_manifest_rejects_missing_fields() {
        let manifest = json!({
            "schema_version": "cork.contract_manifest.v0.1",
            "manifest_id": "manifest-1234"
        });

        let result = validate_contract_manifest(&manifest);
        assert!(matches!(result, Err(SchemaError::ValidationErrors(_))));
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
                "enabled": false,
                "quiescence_ms": 0,
                "max_open_ms": 0,
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

    #[test]
    fn validate_policy_accepts_valid_payload() {
        let policy = build_policy();
        assert!(validate_policy(&policy).is_ok());
    }

    #[test]
    fn validate_policy_rejects_missing_fields() {
        let mut policy = build_policy();
        if let Some(object) = policy.as_object_mut() {
            object.remove("scheduler");
        }
        let result = validate_policy(&policy);
        assert!(matches!(result, Err(SchemaError::ValidationErrors(_))));
    }
}
