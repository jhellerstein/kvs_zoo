//! Broadcast Replication Strategy (after-storage)

use crate::after_storage::{AfterResponses, ClusterCommunication, ReplicationStrategy};
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
pub struct BroadcastReplication<K, V> {
    config: BroadcastReplicationConfig,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Default for BroadcastReplication<K, V> {
    fn default() -> Self {
        Self {
            config: BroadcastReplicationConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<K, V> BroadcastReplication<K, V> {
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

impl<K, V> ClusterCommunication for BroadcastReplication<K, V> {
    fn requires_cluster_scope() -> bool {
        true
    }
}

impl<K, V> ReplicationStrategy<K, V> for BroadcastReplication<K, V>
where
    K: Clone
        + std::fmt::Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + std::hash::Hash,
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
    fn replicate_data<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<
        (K, V),
        Cluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        if self.config.enable_batching {
            self.handle_replication_periodic(cluster, local_data)
        } else {
            self.handle_replication_immediate(cluster, local_data)
        }
    }
}

impl<K, V> BroadcastReplication<K, V>
where
    K: Clone
        + std::fmt::Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + std::hash::Hash,
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
    pub fn handle_replication_immediate<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<
        (K, V),
        Cluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        local_put_tuples
            .broadcast(cluster, TCP.fail_stop().bincode(), nondet!(/** membership */))
            .values()
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }

    /// Periodic background broadcast replication
    pub fn handle_replication_periodic<'a, O>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<
        (K, V),
        Cluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        let ticker = cluster.tick();
        let batch_timeout_ms = self.config.batch_timeout_ms;

        let accumulated_kvs = local_put_tuples.into_keyed().fold(
            q!(|| V::default()),
            q!(|acc, v| {
                lattices::Merge::merge(acc, v);
            }, commutative = manual_proof!(/** lattice merge is commutative */)),
        );

        let periodic_broadcast = accumulated_kvs
            .snapshot(&ticker, nondet!(/** snapshot for periodic broadcast */))
            .entries()
            .all_ticks()
            .sample_every(
                q!(std::time::Duration::from_millis(batch_timeout_ms)),
                nondet!(/** periodic broadcast interval */),
            )
            .broadcast(cluster, TCP.fail_stop().bincode(), nondet!(/** membership */));

        periodic_broadcast
            .values()
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
            .assume_retries(nondet!(/** duplicates from sampling are acceptable */))
    }
}

// Upward pass hook: Broadcast replication doesn't modify responses by default
impl<K, V> AfterResponses for BroadcastReplication<K, V> {
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
        let _broadcast = BroadcastReplication::<String, String>::new();
        let _broadcast_default = BroadcastReplication::<String, String>::default();
    }

    #[test]
    fn test_broadcast_replication_with_config() {}

    #[test]
    fn test_broadcast_replication_config_presets() {
        let _low_latency = BroadcastReplicationConfig::low_latency();
        let _high_throughput = BroadcastReplicationConfig::high_throughput();
        let _synchronous = BroadcastReplicationConfig::synchronous();
        let _default = BroadcastReplicationConfig::default();
    }

    #[test]
    fn test_broadcast_replication_implements_replication_strategy() {
        fn _test_replication_strategy<K, V>(_strategy: impl ReplicationStrategy<K, V>) {}
        _test_replication_strategy::<String, crate::values::CausalString>(BroadcastReplication::<
            String,
            crate::values::CausalString,
        >::new());
    }

    #[test]
    fn test_broadcast_vs_gossip_replication_strategies() {
        fn _accepts_replication_strategy<K, V>(_strategy: impl ReplicationStrategy<K, V>) {}

        _accepts_replication_strategy::<String, crate::values::CausalString>(
            BroadcastReplication::<String, crate::values::CausalString>::new(),
        );
        _accepts_replication_strategy::<String, crate::values::CausalString>(
            crate::after_storage::replication::SimpleGossip::<String, crate::values::CausalString>::default(
            ),
        );
    }
}
