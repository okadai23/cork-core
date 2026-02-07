use cork_store::{ArtifactRef, StateStore};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueRefType {
    RunState,
    StageState,
    NodeOutput,
    NodeArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderAs {
    JsonCompact,
    JsonPretty,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueRef {
    pub run_id: String,
    pub ref_type: ValueRefType,
    pub json_pointer: String,
    pub stage_id: Option<String>,
    pub node_id: Option<String>,
    pub artifact_index: Option<usize>,
    pub render_as: Option<RenderAs>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedValue {
    Json(Value),
    Text(String),
    Artifact(ArtifactRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRefError {
    InvalidPointerSyntax(String),
    NotFound(String),
    TypeMismatch(String),
    NotJsonOutput(String),
}

pub fn resolve_value_ref(
    value_ref: &ValueRef,
    store: &dyn StateStore,
) -> Result<ResolvedValue, ValueRefError> {
    match value_ref.ref_type {
        ValueRefType::RunState => resolve_json_pointer(
            value_ref.json_pointer.as_str(),
            store.run_state(&value_ref.run_id),
        ),
        ValueRefType::StageState => {
            let stage_id = value_ref
                .stage_id
                .as_deref()
                .ok_or_else(|| ValueRefError::TypeMismatch("stage_id is required".to_string()))?;
            resolve_json_pointer(
                value_ref.json_pointer.as_str(),
                store.stage_state(&value_ref.run_id, stage_id),
            )
        }
        ValueRefType::NodeOutput => resolve_node_output(value_ref, store),
        ValueRefType::NodeArtifact => resolve_node_artifact(value_ref, store),
    }
}

fn resolve_node_output(
    value_ref: &ValueRef,
    store: &dyn StateStore,
) -> Result<ResolvedValue, ValueRefError> {
    let node_id = value_ref
        .node_id
        .as_deref()
        .ok_or_else(|| ValueRefError::TypeMismatch("node_id is required".to_string()))?;
    let output = store
        .node_output(&value_ref.run_id, node_id)
        .ok_or_else(|| ValueRefError::NotFound(format!("node_output {node_id}")))?;
    if output.is_json() {
        let json = output
            .parsed_json
            .as_ref()
            .ok_or_else(|| ValueRefError::TypeMismatch("missing parsed_json".to_string()))?;
        resolve_json_pointer(value_ref.json_pointer.as_str(), Some(json.clone()))
    } else {
        if !value_ref.json_pointer.is_empty() {
            return Err(ValueRefError::NotJsonOutput(
                "non-json output cannot use json_pointer".to_string(),
            ));
        }
        if value_ref.render_as != Some(RenderAs::Text) {
            return Err(ValueRefError::NotJsonOutput(
                "non-json output requires render_as=TEXT".to_string(),
            ));
        }
        let text = String::from_utf8(output.payload.data.clone()).map_err(|_| {
            ValueRefError::TypeMismatch("node output payload is not utf-8".to_string())
        })?;
        Ok(ResolvedValue::Text(text))
    }
}

fn resolve_node_artifact(
    value_ref: &ValueRef,
    store: &dyn StateStore,
) -> Result<ResolvedValue, ValueRefError> {
    let node_id = value_ref
        .node_id
        .as_deref()
        .ok_or_else(|| ValueRefError::TypeMismatch("node_id is required".to_string()))?;
    let artifact_index = value_ref
        .artifact_index
        .ok_or_else(|| ValueRefError::TypeMismatch("artifact_index is required".to_string()))?;
    let output = store
        .node_output(&value_ref.run_id, node_id)
        .ok_or_else(|| ValueRefError::NotFound(format!("node_output {node_id}")))?;
    let artifact = output.artifacts.get(artifact_index).ok_or_else(|| {
        ValueRefError::NotFound(format!(
            "artifact_index {artifact_index} for node {node_id}"
        ))
    })?;
    Ok(ResolvedValue::Artifact(artifact.clone()))
}

fn resolve_json_pointer(
    json_pointer: &str,
    value: Option<Value>,
) -> Result<ResolvedValue, ValueRefError> {
    validate_json_pointer(json_pointer)?;
    let value = value.ok_or_else(|| ValueRefError::NotFound("state not found".to_string()))?;
    let resolved = value
        .pointer(json_pointer)
        .ok_or_else(|| ValueRefError::NotFound(format!("json_pointer {json_pointer} not found")))?;
    Ok(ResolvedValue::Json(resolved.clone()))
}

fn validate_json_pointer(json_pointer: &str) -> Result<(), ValueRefError> {
    if json_pointer.is_empty() {
        return Ok(());
    }
    if !json_pointer.starts_with('/') {
        return Err(ValueRefError::InvalidPointerSyntax(
            json_pointer.to_string(),
        ));
    }
    // cspell:ignore peekable
    let mut chars = json_pointer.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.peek() {
                Some('0') | Some('1') => {
                    chars.next();
                }
                _ => {
                    return Err(ValueRefError::InvalidPointerSyntax(
                        json_pointer.to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cork_store::{InMemoryStateStore, NodeOutput, NodePayload};
    use serde_json::json;

    fn run_state_ref(run_id: &str, json_pointer: &str) -> ValueRef {
        ValueRef {
            run_id: run_id.to_string(),
            ref_type: ValueRefType::RunState,
            json_pointer: json_pointer.to_string(),
            stage_id: None,
            node_id: None,
            artifact_index: None,
            render_as: None,
        }
    }

    #[test]
    fn resolves_run_state_pointer() {
        let store = InMemoryStateStore::new();
        store
            .apply_state_put("run-1", "RUN", None, "/path/to", json!(42))
            .expect("state put");

        let resolved =
            resolve_value_ref(&run_state_ref("run-1", "/path/to"), &store).expect("resolved");
        assert_eq!(resolved, ResolvedValue::Json(json!(42)));
    }

    #[test]
    fn rejects_invalid_pointer_syntax() {
        let store = InMemoryStateStore::new();
        store
            .apply_state_put("run-1", "RUN", None, "/path", json!({"a": 1}))
            .expect("state put");
        let result = resolve_value_ref(&run_state_ref("run-1", "/bad~pointer"), &store);
        assert!(matches!(
            result,
            Err(ValueRefError::InvalidPointerSyntax(_))
        ));
    }

    #[test]
    fn resolves_node_output_json_pointer() {
        let store = InMemoryStateStore::new();
        let output = NodeOutput::from_payload(
            NodePayload {
                content_type: "application/json".to_string(),
                data: br#"{"value": {"nested": "ok"}}"#.to_vec(),
            },
            Vec::new(),
        );
        store.set_node_output("run-1", "node-1", output);

        let value_ref = ValueRef {
            run_id: "run-1".to_string(),
            ref_type: ValueRefType::NodeOutput,
            json_pointer: "/value/nested".to_string(),
            stage_id: None,
            node_id: Some("node-1".to_string()),
            artifact_index: None,
            render_as: Some(RenderAs::JsonCompact),
        };
        let resolved = resolve_value_ref(&value_ref, &store).expect("resolved");
        assert_eq!(resolved, ResolvedValue::Json(json!("ok")));
    }

    #[test]
    fn rejects_non_json_output_with_pointer() {
        let store = InMemoryStateStore::new();
        let output = NodeOutput::from_payload(
            NodePayload {
                content_type: "text/plain".to_string(),
                data: b"hello".to_vec(),
            },
            Vec::new(),
        );
        store.set_node_output("run-1", "node-1", output);

        let value_ref = ValueRef {
            run_id: "run-1".to_string(),
            ref_type: ValueRefType::NodeOutput,
            json_pointer: "/nope".to_string(),
            stage_id: None,
            node_id: Some("node-1".to_string()),
            artifact_index: None,
            render_as: Some(RenderAs::Text),
        };
        let result = resolve_value_ref(&value_ref, &store);
        assert!(matches!(result, Err(ValueRefError::NotJsonOutput(_))));
    }

    #[test]
    fn resolves_node_output_text_without_pointer() {
        let store = InMemoryStateStore::new();
        let output = NodeOutput::from_payload(
            NodePayload {
                content_type: "text/plain".to_string(),
                data: b"hello".to_vec(),
            },
            Vec::new(),
        );
        store.set_node_output("run-1", "node-1", output);

        let value_ref = ValueRef {
            run_id: "run-1".to_string(),
            ref_type: ValueRefType::NodeOutput,
            json_pointer: "".to_string(),
            stage_id: None,
            node_id: Some("node-1".to_string()),
            artifact_index: None,
            render_as: Some(RenderAs::Text),
        };
        let resolved = resolve_value_ref(&value_ref, &store).expect("resolved");
        assert_eq!(resolved, ResolvedValue::Text("hello".to_string()));
    }

    #[test]
    fn rejects_non_json_output_without_text_render() {
        let store = InMemoryStateStore::new();
        let output = NodeOutput::from_payload(
            NodePayload {
                content_type: "text/plain".to_string(),
                data: b"hello".to_vec(),
            },
            Vec::new(),
        );
        store.set_node_output("run-1", "node-1", output);

        let value_ref = ValueRef {
            run_id: "run-1".to_string(),
            ref_type: ValueRefType::NodeOutput,
            json_pointer: "".to_string(),
            stage_id: None,
            node_id: Some("node-1".to_string()),
            artifact_index: None,
            render_as: None,
        };
        let result = resolve_value_ref(&value_ref, &store);
        assert!(matches!(result, Err(ValueRefError::NotJsonOutput(_))));
    }

    #[test]
    fn resolves_node_artifact() {
        let store = InMemoryStateStore::new();
        let output = NodeOutput::from_payload(
            NodePayload {
                content_type: "application/json".to_string(),
                data: br#"{}"#.to_vec(),
            },
            vec![ArtifactRef {
                uri: "file://artifact.txt".to_string(),
                sha256_hex: "abc123".to_string(),
                content_type: Some("text/plain".to_string()),
                size_bytes: Some(5),
            }],
        );
        store.set_node_output("run-1", "node-1", output);

        let value_ref = ValueRef {
            run_id: "run-1".to_string(),
            ref_type: ValueRefType::NodeArtifact,
            json_pointer: "".to_string(),
            stage_id: None,
            node_id: Some("node-1".to_string()),
            artifact_index: Some(0),
            render_as: None,
        };
        let resolved = resolve_value_ref(&value_ref, &store).expect("resolved");
        assert_eq!(
            resolved,
            ResolvedValue::Artifact(ArtifactRef {
                uri: "file://artifact.txt".to_string(),
                sha256_hex: "abc123".to_string(),
                content_type: Some("text/plain".to_string()),
                size_bytes: Some(5),
            })
        );
    }
}
