# CORK Hashing v0.1

Hash function: SHA-256 over bytes.

Domain separators (prefix bytes, include NUL):
- "CORK-CONTRACT\0"
- "CORK-POLICY\0"
- "CORK-PATCH\0"
- "CORK-COMPOSITE\0"
- "CORK-RUNCFG\0"

## contract_manifest_hash
SHA256("CORK-CONTRACT\0" || canonical_contract_json_utf8)

## policy_hash
SHA256("CORK-POLICY\0" || canonical_policy_json_utf8)

## patch_hash
SHA256("CORK-PATCH\0" || canonical_patch_json_utf8)

## composite_graph_hash
SHA256("CORK-COMPOSITE\0" || contract_manifest_hash || "\0" || patch_hash_0 || ... || patch_hash_N)

Constraints:
- patch_seq MUST be contiguous: 0..N without gaps.

## run_config_hash
SHA256("CORK-RUNCFG\0" || contract_manifest_hash || "\0" || policy_hash)

