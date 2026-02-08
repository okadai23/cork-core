//! CORK Canonicalization library.
//!
//! This crate provides Pre-normalization and JCS (RFC 8785) canonicalization
//! for deterministic JSON representation.

use serde_json::Value;
use std::cmp::Ordering;

/// Pre-normalize a contract manifest by sorting set-like arrays.
pub fn prenorm_contract(mut value: Value) -> Value {
    if let Value::Object(root) = &mut value
        && let Some(Value::Object(contract_graph)) = root.get_mut("contract_graph")
        && let Some(Value::Array(stages)) = contract_graph.get_mut("stages")
    {
        stages.sort_by_key(|stage| extract_string(stage, "stage_id"));
        for stage in stages.iter_mut() {
            prenorm_stage(stage);
        }
    }
    value
}

/// Pre-normalize a policy by sorting set-like arrays.
pub fn prenorm_policy(mut value: Value) -> Value {
    if let Value::Object(root) = &mut value {
        if let Some(Value::Object(rate_limit)) = root.get_mut("rate_limit")
            && let Some(Value::Array(providers)) = rate_limit.get_mut("providers")
        {
            providers.sort_by_key(|provider| extract_string(provider, "provider_id"));
        }

        if let Some(Value::Object(guardrails)) = root.get_mut("guardrails") {
            for key in ["pre_call", "during_call", "post_call"] {
                if let Some(Value::Array(guards)) = guardrails.get_mut(key) {
                    guards.sort_by_key(|guard| extract_string(guard, "name"));
                }
            }
        }

        if let Some(Value::Object(retry)) = root.get_mut("retry")
            && let Some(Value::Array(retry_on)) = retry.get_mut("retry_on")
        {
            sort_array_strings(retry_on);
        }

        if let Some(Value::Object(worker)) = root.get_mut("worker")
            && let Some(Value::Object(retry)) = worker.get_mut("retry")
            && let Some(Value::Array(retry_on)) = retry.get_mut("retry_on")
        {
            sort_array_strings(retry_on);
        }

        if let Some(Value::Object(cache)) = root.get_mut("cache")
            && let Some(Value::Array(rules)) = cache.get_mut("rules")
        {
            rules.sort_by_key(|rule| (extract_i64(rule, "priority"), extract_string(rule, "name")));
        }

        if let Some(Value::Array(resource_pools)) = root.get_mut("resource_pools") {
            resource_pools.sort_by_key(|pool| extract_string(pool, "resource_id"));
        }
    }

    value
}

/// Pre-normalize a GraphPatch by sorting set-like arrays while preserving op order.
pub fn prenorm_patch(mut value: Value) -> Value {
    if let Value::Object(root) = &mut value
        && let Some(Value::Array(ops)) = root.get_mut("ops")
    {
        for op in ops.iter_mut() {
            if let Value::Object(op_obj) = op
                && let Some(Value::Object(node_added)) = op_obj.get_mut("node_added")
                && let Some(node) = node_added.get_mut("node")
            {
                prenorm_node(node);
            }
        }
    }

    value
}

/// Serialize a JSON value using JCS (RFC 8785) canonicalization.
pub fn jcs_bytes(value: &Value) -> Vec<u8> {
    serde_json_canonicalizer::to_vec(value)
        .expect("JCS canonicalization should not fail for valid JSON values")
}

fn prenorm_stage(stage: &mut Value) {
    if let Value::Object(stage_obj) = stage {
        if let Some(Value::Array(deps)) = stage_obj.get_mut("dependencies") {
            deps.sort_by(|a, b| {
                let ordering = extract_string(a, "stage_id").cmp(&extract_string(b, "stage_id"));
                if ordering == Ordering::Equal {
                    extract_string(a, "constraint").cmp(&extract_string(b, "constraint"))
                } else {
                    ordering
                }
            });
        }

        if let Some(Value::Object(expansion_policy)) = stage_obj.get_mut("expansion_policy")
            && let Some(Value::Array(allow_kinds)) = expansion_policy.get_mut("allow_kinds")
        {
            sort_array_strings(allow_kinds);
        }

        if let Some(Value::Array(tags)) = stage_obj.get_mut("tags") {
            sort_array_strings(tags);
        }
    }
}

fn prenorm_node(node: &mut Value) {
    if let Value::Object(node_obj) = node {
        if let Some(Value::Array(deps)) = node_obj.get_mut("deps") {
            sort_array_strings(deps);
        }

        if let Some(Value::Object(exec)) = node_obj.get_mut("exec")
            && let Some(Value::Object(merge)) = exec.get_mut("merge")
        {
            let should_sort = merge
                .get("input_order")
                .and_then(Value::as_str)
                .is_some_and(|order| order == "STABLE_SORT");
            if should_sort && let Some(Value::Array(inputs)) = merge.get_mut("inputs") {
                sort_array_strings(inputs);
            }
        }
    }
}

fn sort_array_strings(array: &mut [Value]) {
    array.sort_by_key(value_as_string);
}

fn value_as_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| match value {
            Value::Number(num) => Some(num.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn extract_string(value: &Value, key: &str) -> String {
    match value {
        Value::Object(map) => map
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        _ => String::new(),
    }
}

fn extract_i64(value: &Value, key: &str) -> i64 {
    match value {
        Value::Object(map) => map.get(key).and_then(Value::as_i64).unwrap_or_default(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prenorm_patch_sorts_deps_and_merge_inputs() {
        let patch = json!({
            "schema_version": "cork.graph_patch.v0.1",
            "run_id": "run-1",
            "patch_seq": 0,
            "stage_id": "stage-a",
            "ops": [
                {
                    "op_type": "NODE_ADDED",
                    "node_added": {
                        "node": {
                            "node_id": "node-1",
                            "kind": "MERGE",
                            "anchor_position": "WITHIN",
                            "deps": ["b", "a"],
                            "exec": {
                                "merge": {
                                    "strategy": "ALL",
                                    "inputs": ["z", "y"],
                                    "input_order": "STABLE_SORT"
                                }
                            }
                        }
                    }
                }
            ]
        });

        let prenorm = prenorm_patch(patch);
        let deps = prenorm["ops"][0]["node_added"]["node"]["deps"]
            .as_array()
            .expect("deps array");
        assert_eq!(
            deps,
            &vec![Value::String("a".into()), Value::String("b".into())]
        );

        let inputs = prenorm["ops"][0]["node_added"]["node"]["exec"]["merge"]["inputs"]
            .as_array()
            .expect("inputs array");
        assert_eq!(
            inputs,
            &vec![Value::String("y".into()), Value::String("z".into())]
        );
    }
}
