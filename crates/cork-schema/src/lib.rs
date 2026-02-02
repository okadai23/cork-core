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

fn load_contract_manifest_schema() -> Result<Value, SchemaError> {
    let schema_str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../schemas/cork.contract_manifest.v0.1.schema.json"
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
}
