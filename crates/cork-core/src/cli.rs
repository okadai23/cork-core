use clap::{Parser, Subcommand};
use cork_canon::{jcs_bytes, prenorm_contract, prenorm_patch, prenorm_policy};
use cork_hash::sha256;
use cork_proto::cork::v1::{
    ApplyGraphPatchRequest, CanonicalJsonDocument, GetRunRequest, Payload, RunHandle, Sha256,
    StreamRunEventsRequest, SubmitRunRequest,
};
use serde_json::Value;
use std::path::{Path, PathBuf};

const CONTRACT_SCHEMA_ID: &str = "cork.contract_manifest.v0.1";
const POLICY_SCHEMA_ID: &str = "cork.policy.v0.1";
const PATCH_SCHEMA_ID: &str = "cork.graph_patch.v0.1";

#[derive(Parser, Debug)]
#[command(name = "corkctl", about = "CORK Core CLI client")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the Cork Core gRPC server.
    Serve {
        /// gRPC listen address (e.g. 127.0.0.1:50051).
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
    /// Submit a new run with contract/policy JSON files.
    SubmitRun {
        /// gRPC server address (e.g. http://127.0.0.1:50051).
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,
        /// Contract manifest JSON file.
        #[arg(long)]
        contract: PathBuf,
        /// Policy JSON file.
        #[arg(long)]
        policy: PathBuf,
        /// Initial input payload file (optional).
        #[arg(long)]
        input: Option<PathBuf>,
        /// Content type for input payload.
        #[arg(long, default_value = "application/json")]
        input_content_type: String,
        /// Experiment id (optional).
        #[arg(long, default_value = "")]
        experiment_id: String,
        /// Variant id (optional).
        #[arg(long, default_value = "")]
        variant_id: String,
    },
    /// Apply a graph patch to an existing run.
    ApplyPatch {
        /// gRPC server address (e.g. http://127.0.0.1:50051).
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,
        /// Run id to patch.
        #[arg(long)]
        run_id: String,
        /// Patch JSON file.
        #[arg(long)]
        patch: PathBuf,
        /// Actor id for audit trail (optional).
        #[arg(long, default_value = "")]
        actor_id: String,
    },
    /// Stream run events for a run.
    StreamEvents {
        /// gRPC server address (e.g. http://127.0.0.1:50051).
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,
        /// Run id to stream.
        #[arg(long)]
        run_id: String,
        /// Event sequence to start from.
        #[arg(long, default_value_t = 0)]
        since_event_seq: i64,
    },
    /// Get run status.
    GetRun {
        /// gRPC server address (e.g. http://127.0.0.1:50051).
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,
        /// Run id to fetch.
        #[arg(long)]
        run_id: String,
    },
}

pub enum CanonicalKind {
    Contract,
    Policy,
    Patch,
}

pub fn canonicalize_json(
    path: &Path,
    kind: CanonicalKind,
) -> Result<CanonicalJsonDocument, String> {
    let contents =
        std::fs::read(path).map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let json: Value = serde_json::from_slice(&contents)
        .map_err(|err| format!("invalid json {}: {err}", path.display()))?;
    let (pre_normalized, schema_id) = match kind {
        CanonicalKind::Contract => (prenorm_contract(json), CONTRACT_SCHEMA_ID),
        CanonicalKind::Policy => (prenorm_policy(json), POLICY_SCHEMA_ID),
        CanonicalKind::Patch => (prenorm_patch(json), PATCH_SCHEMA_ID),
    };
    let canonical_bytes = jcs_bytes(&pre_normalized);
    let digest = sha256(&canonical_bytes);
    Ok(CanonicalJsonDocument {
        canonical_json_utf8: canonical_bytes,
        sha256: Some(Sha256 {
            bytes32: digest.to_vec(),
        }),
        schema_id: schema_id.to_string(),
    })
}

pub fn payload_from_file(path: &Path, content_type: &str) -> Result<Payload, String> {
    let data =
        std::fs::read(path).map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let digest = sha256(&data);
    Ok(Payload {
        content_type: content_type.to_string(),
        data,
        encoding: "utf-8".to_string(),
        sha256: Some(Sha256 {
            bytes32: digest.to_vec(),
        }),
    })
}

pub fn submit_run_request(
    contract: CanonicalJsonDocument,
    policy: CanonicalJsonDocument,
    input: Option<Payload>,
    experiment_id: String,
    variant_id: String,
) -> SubmitRunRequest {
    SubmitRunRequest {
        contract_manifest: Some(contract),
        policy: Some(policy),
        initial_input: input,
        experiment_id,
        variant_id,
        trace_context: None,
    }
}

pub fn apply_patch_request(
    run_id: String,
    patch: CanonicalJsonDocument,
    actor_id: String,
) -> ApplyGraphPatchRequest {
    ApplyGraphPatchRequest {
        handle: Some(RunHandle { run_id }),
        patch: Some(patch),
        actor_id,
    }
}

pub fn stream_events_request(run_id: String, since_event_seq: i64) -> StreamRunEventsRequest {
    StreamRunEventsRequest {
        handle: Some(RunHandle { run_id }),
        since_event_seq,
    }
}

pub fn get_run_request(run_id: String) -> GetRunRequest {
    GetRunRequest {
        handle: Some(RunHandle { run_id }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn cli_parses_submit_run_flags() {
        let cli = Cli::try_parse_from([
            "corkctl",
            "submit-run",
            "--contract",
            "contract.json",
            "--policy",
            "policy.json",
            "--input",
            "input.json",
            "--experiment-id",
            "exp-1",
            "--variant-id",
            "var-1",
        ])
        .expect("parse cli");

        match cli.command {
            Command::SubmitRun {
                contract,
                policy,
                input,
                experiment_id,
                variant_id,
                ..
            } => {
                assert_eq!(contract, PathBuf::from("contract.json"));
                assert_eq!(policy, PathBuf::from("policy.json"));
                assert_eq!(input, Some(PathBuf::from("input.json")));
                assert_eq!(experiment_id, "exp-1");
                assert_eq!(variant_id, "var-1");
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn canonicalize_contract_sets_schema_and_sha() {
        let mut file = NamedTempFile::new().expect("temp file");
        let sample = json!({
            "schema_version": "cork.contract_manifest.v0.1",
            "contract_graph": { "stages": [] },
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
        let payload = serde_json::to_vec(&sample).expect("json");
        file.write_all(&payload).expect("write");

        let doc = canonicalize_json(file.path(), CanonicalKind::Contract).expect("canonicalize");
        assert_eq!(doc.schema_id, CONTRACT_SCHEMA_ID);
        let sha = doc.sha256.expect("sha");
        assert_eq!(sha.bytes32.len(), 32);
    }
}
