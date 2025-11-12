//! Slot-aware broadcast replication
//!
//! Provides a slot-preserving variant of broadcast replication so learners can
//! keep the common, unslotted broadcast logic in `replication::broadcast` free
//! of slot-specific details.

use crate::after_storage::replication::BroadcastReplicationConfig;
use crate::after_storage::ReplicationStrategy;
use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use lattices::Merge;
use serde::{Deserialize, Serialize};

/// Broadcast replication that preserves slots
#[derive(Clone, Debug)]
pub struct SlottedBroadcastReplication<V> {
    config: BroadcastReplicationConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for SlottedBroadcastReplication<V> {
    fn default() -> Self {
        Self { config: BroadcastReplicationConfig::default(), _phantom: std::marker::PhantomData }
    }
}

impl<V> SlottedBroadcastReplication<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: BroadcastReplicationConfig) -> Self {
        Self { config, _phantom: std::marker::PhantomData }
    }
}

impl<V> ReplicationStrategy<V> for SlottedBroadcastReplication<V>
where
    V: Clone
        + std::fmt::Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + Merge<V>,
{
    fn replicate_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Delegate to vanilla broadcast to avoid code duplication
        let vanilla = crate::after_storage::replication::BroadcastReplication::<V>::with_config(
            self.config.clone(),
        );
        vanilla.replicate_data(cluster, local_data)
    }

    fn replicate_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Preserve slots during dissemination
        local_slotted_data
            .broadcast_bincode(cluster, nondet!(/** broadcast slotted ops to all nodes */))
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
    }
}
