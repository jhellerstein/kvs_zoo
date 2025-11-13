//! Broadcast Replication Strategy (after-storage, unified)
//!
//! Implements the unified `replicate_updates` API, splitting unslotted and
//! slotted updates internally to avoid code duplication.

use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy, ReplicationUpdate};
use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use lattices::Merge;
use serde::{Deserialize, Serialize};

/// Configuration for broadcast replication
#[derive(Clone, Debug)]
pub struct BroadcastReplicationConfig {
    /// Batch multiple updates before broadcasting
    pub enable_batching: bool,
    /// Maximum time to wait before sending a batch (in milliseconds)
    pub batch_timeout_ms: u64,
    /// Maximum number of keys per batch
    pub max_batch_size: usize,
}

impl Default for BroadcastReplicationConfig {
    fn default() -> Self {
        Self {
            enable_batching: false,
            batch_timeout_ms: 100,
            max_batch_size: 50,
        }
    }
}

impl BroadcastReplicationConfig {
    /// Create config optimized for low latency (immediate broadcasting)
    pub fn low_latency() -> Self {
        Self {
            enable_batching: false,
            batch_timeout_ms: 50,
            max_batch_size: 1,
        }
    }

    /// Create config optimized for high throughput (batched broadcasting)
    pub fn high_throughput() -> Self {
        Self {
            enable_batching: true,
            batch_timeout_ms: 200,
            max_batch_size: 100,
        }
    }

    /// Create config for synchronous broadcasting (immediate, no batching)
    pub fn synchronous() -> Self {
        Self {
            enable_batching: false,
            batch_timeout_ms: 0,
            max_batch_size: 1,
        }
    }
}

/// Broadcast replication: sends updates to all cluster nodes
#[derive(Clone, Debug)]
pub struct BroadcastReplication<V> {
    config: BroadcastReplicationConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for BroadcastReplication<V> {
    fn default() -> Self {
        Self {
            config: BroadcastReplicationConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> BroadcastReplication<V> {
    /// Create a new broadcast replication strategy with default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new broadcast replication strategy with custom configuration
    pub fn with_config(config: BroadcastReplicationConfig) -> Self {
        Self {
            config,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> ReplicationStrategy<V> for BroadcastReplication<V>
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
    fn replicate_updates<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        updates: Stream<ReplicationUpdate<V>, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<ReplicationUpdate<V>, Cluster<'a, KVSNode>, Unbounded> {
        let unslotted_in = updates
            .clone()
            .filter_map(q!(|u| match u { ReplicationUpdate::Unslotted(t) => Some(t), _ => None }));
        let slotted_in = updates
            .filter_map(q!(|u| match u { ReplicationUpdate::Slotted(t) => Some(t), _ => None }));

        let unslotted_out_raw = if self.config.enable_batching {
            self.handle_replication_periodic(cluster, unslotted_in)
        } else {
            self.handle_replication_immediate(cluster, unslotted_in)
        };
        let unslotted_out = unslotted_out_raw.map(q!(|t| ReplicationUpdate::Unslotted(t)));

        let slotted_out = slotted_in
            .broadcast_bincode(cluster, nondet!(/** broadcast slotted ops to all nodes */))
            .values()
            .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
                nondet!(/** broadcast messages unordered */),
            )
            .map(q!(|t| ReplicationUpdate::Slotted(t)));

        unslotted_out
            .interleave(slotted_out)
            .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                nondet!(/** merged replication updates (total order for consumers) */),
            )
            .assume_retries::<hydro_lang::live_collections::stream::ExactlyOnce>(
                nondet!(/** consumers expect exactly-once semantics */),
            )
    }
}

impl<V> BroadcastReplication<V>
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
    /// Immediate synchronous broadcast replication
    pub fn handle_replication_immediate<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        local_put_tuples
            .broadcast_bincode(cluster, nondet!(/** immediate broadcast to all nodes */))
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
    }

    /// Periodic background broadcast replication
    pub fn handle_replication_periodic<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        let ticker = cluster.tick();
        let batch_timeout_ms = self.config.batch_timeout_ms;

        let accumulated_kvs = local_put_tuples.into_keyed().fold_commutative(
            q!(|| V::default()),
            q!(|acc, v| {
                lattices::Merge::merge(acc, v);
            }),
        );

        let periodic_broadcast = accumulated_kvs
            .snapshot(&ticker, nondet!(/** snapshot for periodic broadcast */))
            .entries()
            .all_ticks()
            .sample_every(
                q!(std::time::Duration::from_millis(batch_timeout_ms)),
                nondet!(/** periodic broadcast interval */),
            )
            .broadcast_bincode(cluster, nondet!(/** periodic broadcast to all nodes */));

        periodic_broadcast
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
            .assume_retries(nondet!(/** duplicates from sampling are acceptable */))
    }
}

// Upward pass hook: Broadcast replication doesn't modify responses by default
impl<V> MaintenanceAfterResponses for BroadcastReplication<V> {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_replication_creation() {
        let _broadcast = BroadcastReplication::<String>::new();
        let _broadcast_default = BroadcastReplication::<String>::default();
    }

    #[test]
    fn test_broadcast_replication_with_config() {
        let config = BroadcastReplicationConfig::low_latency();
        let _broadcast = BroadcastReplication::<String>::with_config(config);
    }

    #[test]
    fn test_broadcast_replication_config_presets() {
        let _low_latency = BroadcastReplicationConfig::low_latency();
        let _high_throughput = BroadcastReplicationConfig::high_throughput();
        let _synchronous = BroadcastReplicationConfig::synchronous();
        let _default = BroadcastReplicationConfig::default();
    }

    #[test]
    fn test_broadcast_replication_implements_replication_strategy() {
        fn _test_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
        _test_replication_strategy::<crate::values::CausalString>(BroadcastReplication::<
            crate::values::CausalString,
        >::new());
    }

    #[test]
    fn test_broadcast_vs_gossip_replication_strategies() {
        fn _accepts_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}

        _accepts_replication_strategy::<crate::values::CausalString>(BroadcastReplication::<
            crate::values::CausalString,
        >::new());
        _accepts_replication_strategy::<crate::values::CausalString>(
            crate::after_storage::replication::SimpleGossip::<crate::values::CausalString>::default(
            ),
        );
    }
}
