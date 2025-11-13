//! KVS architectural layering pattern (module split)
//!
//! This module re-exports types and traits from submodules:
//! - types: KVSCluster, KVSNode, KVSClusters
//! - wire_down: KVSWire (before_storage routing/ordering)
//! - wire_up: AfterWire (after_storage replication/responders)
//! - spec: KVSSpec (cluster creation/registration)
//!
//! Note: This directory-based split replaces the former single-file `kvs_layer.rs`.
//! The goal is to keep each concern small and clear for learners exploring how
//! before_storage (routing/ordering) composes with after_storage (replication/responders).
//! See `pipelines/` and `examples/*_detail.rs` for end-to-end wiring patterns.

pub mod spec;
pub mod types;
pub mod wire_down;
pub mod wire_up;

pub use spec::KVSSpec;
pub use types::{KVSCluster, KVSClusters, KVSNode};
pub use wire_down::KVSWire;
pub use wire_up::AfterWire;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::after_storage::NoReplication;
    use crate::before_storage::routing::SingleNodeRouter;

    struct TestLayer;

    #[test]
    fn layer_creation() {
        let _layer = KVSCluster::<TestLayer, SingleNodeRouter, NoReplication, ()>::new(
            SingleNodeRouter,
            NoReplication,
            (),
        );
    }

    #[test]
    fn layer_default() {
        let _layer = KVSCluster::<TestLayer, SingleNodeRouter, NoReplication, ()>::default();
    }

    #[test]
    fn layers_lookup() {
        let layers = KVSClusters::new();
        assert!(layers.try_get::<TestLayer>().is_none());
    }
}
