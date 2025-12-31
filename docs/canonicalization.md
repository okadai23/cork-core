# CORK Canonicalization v0.1

## Purpose
CORK defines canonicalization to:
- produce stable hashes for contract manifests, policies, patches, and composite graphs,
- guarantee replayability and reproducibility of dynamic graph expansion (GraphPatch).

## Canonical JSON
CORK canonical JSON is:

1. Pre-normalization (CORK-specific)
2. JCS serialization (RFC 8785)
3. UTF-8 bytes

## Pre-normalization (set-like arrays)
Before applying JCS, sort arrays that are semantically sets:

### Contract Manifest
- contract_graph.stages: sort by stage_id asc
- Stage.dependencies: sort by stage_id asc, then constraint asc
- Stage.expansion_policy.allow_kinds: sort asc
- Stage.tags: sort asc

### Policy
- rate_limit.providers: sort by provider_id asc
- guardrails.pre_call/during_call/post_call: sort by name asc
- retry.retry_on: sort asc
- cache.rules: sort by priority asc, then name asc
- resource_pools: sort by resource_id asc

### GraphPatch
- GraphPatch objects MUST NOT be reordered. patch_seq defines order.
- ops list order MUST be preserved.
- Within NodeAdded.node.deps: sort asc
- Within MergeExec.inputs: if input_order=STABLE_SORT then sort asc, else preserve order

## Output
Canonical JSON is a UTF-8 byte sequence.
Hashing uses SHA-256 with domain separation.

