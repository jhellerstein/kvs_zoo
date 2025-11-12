//! Leaf-level after hooks (after-storage, native)

use hydro_lang::prelude::*;

use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy};
use crate::kvs_core::KVSNode;
use serde::{Deserialize, Serialize};

/// Trait for leaf after-hooks that decide how to emit responses locally.
pub trait LeafAfterHook {
    fn respond<'a>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        tagged_responses: Stream<(bool, String), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded>;
}

/// Simple responder that only forwards responses for original (non-replica) ops.
#[derive(Clone, Debug, Default)]
pub struct Responder;

impl Responder {
    pub fn new() -> Self {
        Self
    }
}

impl LeafAfterHook for Responder {
    fn respond<'a>(
        &self,
        _leaf_cluster: &Cluster<'a, KVSNode>,
        tagged_responses: Stream<(bool, String), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        tagged_responses
            .filter_map(q!(|(is_replica, resp)| if !is_replica { Some(resp) } else { None }))
            .assume_ordering(nondet!(/** local responses only for originals */))
    }
}

/// Responder also customizes the upward response pass: already handled filtering
/// in `respond`, so we just pass through here (explicit override for clarity).
impl MaintenanceAfterResponses for Responder {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

// Allow Responder to serve as a leaf "maintenance" component by also implementing
// the replication trait as a no-op. This satisfies existing generic bounds while
// we evolve the separation between replication and after-hooks.
impl<V> ReplicationStrategy<V> for Responder
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    fn replicate_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        // No replication at leaf after-hook layer
        local_data
    }

    fn replicate_slotted_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Preserve slots; no additional replication
        local_slotted_data
    }
}
