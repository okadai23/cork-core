use cork_hash::{composite_graph_hash, patch_hash};
use cork_proto::cork::v1::{HashBundle, Sha256};
use cork_store::RunCtx;

#[derive(Debug, Clone)]
pub struct PatchTracker {
    contract_hash: [u8; 32],
    patch_hashes: Vec<[u8; 32]>,
}

impl PatchTracker {
    pub fn new(contract_hash: [u8; 32]) -> Self {
        Self {
            contract_hash,
            patch_hashes: Vec::new(),
        }
    }

    pub fn patch_hashes(&self) -> &[[u8; 32]] {
        &self.patch_hashes
    }

    pub fn apply_patch(&mut self, run_ctx: &RunCtx, patch_bytes: &[u8]) -> HashBundle {
        let digest = patch_hash(patch_bytes);
        self.patch_hashes.push(digest);
        self.update_hash_bundle(run_ctx)
    }

    fn update_hash_bundle(&self, run_ctx: &RunCtx) -> HashBundle {
        let composite = composite_graph_hash(&self.contract_hash, &self.patch_hashes);
        let existing = run_ctx.metadata().hash_bundle;
        let (policy_hash, run_config_hash) = existing
            .as_ref()
            .map(|bundle| (bundle.policy_hash.clone(), bundle.run_config_hash.clone()))
            .unwrap_or((None, None));
        let bundle = HashBundle {
            contract_manifest_hash: Some(bytes_to_sha256(&self.contract_hash)),
            policy_hash,
            run_config_hash,
            composite_graph_hash: Some(bytes_to_sha256(&composite)),
        };
        run_ctx.set_hash_bundle(Some(bundle.clone()));
        bundle
    }
}

fn bytes_to_sha256(bytes: &[u8; 32]) -> Sha256 {
    Sha256 {
        bytes32: bytes.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cork_hash::contract_hash;
    use cork_store::{CreateRunInput, InMemoryRunRegistry, RunRegistry};

    #[test]
    fn apply_patch_updates_hash_bundle() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry.create_run(CreateRunInput::default());
        let contract_digest = contract_hash(b"contract");
        let mut tracker = PatchTracker::new(contract_digest);

        let bundle = tracker.apply_patch(&run_ctx, br#"{"op":"noop"}"#);
        let stored = run_ctx.metadata().hash_bundle.expect("hash bundle");

        assert_eq!(
            stored
                .contract_manifest_hash
                .as_ref()
                .expect("contract hash")
                .bytes32,
            contract_digest.to_vec()
        );
        assert_eq!(
            stored
                .composite_graph_hash
                .as_ref()
                .expect("composite hash")
                .bytes32,
            bundle
                .composite_graph_hash
                .as_ref()
                .expect("composite hash")
                .bytes32
        );
    }

    #[test]
    fn composite_graph_hash_tracks_patch_sequence() {
        let registry = InMemoryRunRegistry::new();
        let run_ctx = registry.create_run(CreateRunInput::default());
        let contract_digest = contract_hash(b"contract");
        let mut tracker = PatchTracker::new(contract_digest);

        tracker.apply_patch(&run_ctx, br#"{"op":"first"}"#);
        tracker.apply_patch(&run_ctx, br#"{"op":"second"}"#);

        let expected = composite_graph_hash(&contract_digest, tracker.patch_hashes());
        let stored = run_ctx.metadata().hash_bundle.expect("hash bundle");
        let stored_composite = stored
            .composite_graph_hash
            .as_ref()
            .expect("composite hash")
            .bytes32
            .clone();

        assert_eq!(stored_composite, expected.to_vec());
    }
}
