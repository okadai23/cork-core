//! CORK Hash library.
//!
//! This crate provides domain-separated SHA-256 hashing for CORK documents.

use sha2::{Digest, Sha256};

pub const CONTRACT_PREFIX: &[u8] = b"CORK-CONTRACT\0";
pub const POLICY_PREFIX: &[u8] = b"CORK-POLICY\0";
pub const PATCH_PREFIX: &[u8] = b"CORK-PATCH\0";
pub const COMPOSITE_PREFIX: &[u8] = b"CORK-COMPOSITE\0";
pub const RUNCFG_PREFIX: &[u8] = b"CORK-RUNCFG\0";

/// Compute a SHA-256 digest for the given bytes.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Compute a domain-separated SHA-256 digest for the given prefix and bytes.
pub fn domain_hash(prefix: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(prefix);
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

pub fn contract_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(CONTRACT_PREFIX, bytes)
}

pub fn policy_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(POLICY_PREFIX, bytes)
}

pub fn patch_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(PATCH_PREFIX, bytes)
}

pub fn composite_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(COMPOSITE_PREFIX, bytes)
}

pub fn composite_graph_hash(contract_hash: &[u8; 32], patch_hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(32 + 1 + (patch_hashes.len() * 32));
    bytes.extend_from_slice(contract_hash);
    bytes.push(0);
    for patch_hash in patch_hashes {
        bytes.extend_from_slice(patch_hash);
    }
    composite_hash(&bytes)
}

pub fn run_config_hash(bytes: &[u8]) -> [u8; 32] {
    domain_hash(RUNCFG_PREFIX, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cork_canon::{jcs_bytes, prenorm_contract, prenorm_patch};
    use serde_json::json;

    #[test]
    fn sha256_matches_known_vector() {
        let digest = sha256(b"hello");
        let expected = [
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ];
        assert_eq!(digest, expected);
    }

    #[test]
    fn contract_hash_is_stable_for_field_order() {
        let contract_a = json!({
            "schema_version": "cork.contract_manifest.v0.1",
            "manifest_id": "manifest-1",
            "contract_graph": {
                "stages": [
                    {
                        "stage_id": "b",
                        "dependencies": [
                            { "stage_id": "c", "constraint": "SOFT" },
                            { "stage_id": "a", "constraint": "HARD" }
                        ],
                        "expansion_policy": {
                            "allow_dynamic": true,
                            "allow_kinds": ["TOOL", "LLM"],
                            "max_dynamic_nodes": 1,
                            "max_steps_in_stage": 1,
                            "allow_cross_stage_deps": "NONE"
                        },
                        "tags": ["beta", "alpha"]
                    },
                    {
                        "stage_id": "a",
                        "expansion_policy": {
                            "allow_dynamic": true,
                            "allow_kinds": ["MERGE"],
                            "max_dynamic_nodes": 1,
                            "max_steps_in_stage": 1,
                            "allow_cross_stage_deps": "NONE"
                        }
                    }
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

        let contract_b = json!({
            "manifest_id": "manifest-1",
            "schema_version": "cork.contract_manifest.v0.1",
            "defaults": {
                "completion_policy": {
                    "require_commit": true,
                    "fail_on_any_failure": false
                },
                "stage_ttl": {},
                "stage_budget": {},
                "expansion_policy": {
                    "max_steps_in_stage": 0,
                    "allow_cross_stage_deps": "NONE",
                    "allow_kinds": [],
                    "max_dynamic_nodes": 0,
                    "allow_dynamic": true
                }
            },
            "contract_graph": {
                "stages": [
                    {
                        "stage_id": "a",
                        "expansion_policy": {
                            "allow_kinds": ["MERGE"],
                            "allow_cross_stage_deps": "NONE",
                            "max_dynamic_nodes": 1,
                            "allow_dynamic": true,
                            "max_steps_in_stage": 1
                        }
                    },
                    {
                        "tags": ["alpha", "beta"],
                        "expansion_policy": {
                            "allow_kinds": ["LLM", "TOOL"],
                            "allow_cross_stage_deps": "NONE",
                            "max_steps_in_stage": 1,
                            "allow_dynamic": true,
                            "max_dynamic_nodes": 1
                        },
                        "dependencies": [
                            { "constraint": "HARD", "stage_id": "a" },
                            { "constraint": "SOFT", "stage_id": "c" }
                        ],
                        "stage_id": "b"
                    }
                ]
            }
        });

        let bytes_a = jcs_bytes(&prenorm_contract(contract_a));
        let bytes_b = jcs_bytes(&prenorm_contract(contract_b));
        assert_eq!(contract_hash(&bytes_a), contract_hash(&bytes_b));
    }

    #[test]
    fn patch_hash_sorts_deps_and_merge_inputs() {
        let patch_a = json!({
            "schema_version": "cork.graph_patch.v0.1",
            "run_id": "run-1",
            "patch_seq": 1,
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

        let patch_b = json!({
            "schema_version": "cork.graph_patch.v0.1",
            "run_id": "run-1",
            "patch_seq": 1,
            "stage_id": "stage-a",
            "ops": [
                {
                    "op_type": "NODE_ADDED",
                    "node_added": {
                        "node": {
                            "node_id": "node-1",
                            "kind": "MERGE",
                            "anchor_position": "WITHIN",
                            "deps": ["a", "b"],
                            "exec": {
                                "merge": {
                                    "strategy": "ALL",
                                    "inputs": ["y", "z"],
                                    "input_order": "STABLE_SORT"
                                }
                            }
                        }
                    }
                }
            ]
        });

        let bytes_a = jcs_bytes(&prenorm_patch(patch_a));
        let bytes_b = jcs_bytes(&prenorm_patch(patch_b));
        assert_eq!(patch_hash(&bytes_a), patch_hash(&bytes_b));
    }

    #[test]
    fn composite_graph_hash_changes_with_patches() {
        let contract_digest = contract_hash(b"contract");
        let composite_empty = composite_graph_hash(&contract_digest, &[]);
        let patch_digest = patch_hash(b"patch");
        let composite_one = composite_graph_hash(&contract_digest, &[patch_digest]);
        assert_ne!(composite_empty, composite_one);
    }

    #[test]
    fn composite_graph_hash_matches_manual_concat() {
        let contract_digest = contract_hash(b"contract");
        let patch_digest = patch_hash(b"patch");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&contract_digest);
        bytes.push(0);
        bytes.extend_from_slice(&patch_digest);
        let manual = composite_hash(&bytes);
        let computed = composite_graph_hash(&contract_digest, &[patch_digest]);
        assert_eq!(manual, computed);
    }
}
