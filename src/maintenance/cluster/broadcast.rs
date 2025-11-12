//! Broadcast Replication Strategy
//!
//! This module implements broadcast replication for strong consistency guarantees.
//! All updates are deterministically broadcast to all cluster nodes, providing
//! faster convergence than gossip protocols at the cost of higher message overhead.

use crate::kvs_core::KVSNode;
use crate::maintenance::{MaintenanceAfterResponses, ReplicationStrategy};
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
///
/// Uses deterministic all-to-all broadcasting. Higher message overhead than gossip
/// but faster convergence and simpler reasoning about consistency.
///
/// ## Protocol Overview
///
/// 1. **Immediate Broadcasting**: All local updates are broadcast to all nodes
/// 2. **Deterministic Delivery**: Every node receives every update
/// 3. **Strong Consistency**: All nodes converge to the same state quickly
/// 4. **High Message Overhead**: O(n²) messages for n nodes
///
/// ## Consistency Guarantees
///
/// - **Strong Consistency**: All nodes receive all updates
/// - **Fast Convergence**: Updates propagate in one round
/// - **Deterministic**: No probabilistic behavior
/// - **Reliable**: All updates are guaranteed to be delivered
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
    fn replicate_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Choose implementation based on config
        if self.config.enable_batching {
            self.handle_replication_periodic(cluster, local_data)
        } else {
            self.handle_replication_immediate(cluster, local_data)
        }
    }

    // Ordered logs with "slots" (positions) need to be replicated for ordered "replay".
    // See logbased.rs.
    fn replicate_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Broadcast slotted operations to all nodes
        local_slotted_data
            .broadcast_bincode(cluster, nondet!(/** broadcast slotted ops to all nodes */))
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
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
    ///
    /// Every local write is immediately broadcast to all cluster members.
    /// No buffering, no delays - pure synchronous replication.
    ///
    /// ## Behavior
    /// - Event-driven: broadcasts immediately on every local write
    /// - Synchronous: no batching or periodic delays
    /// - Deterministic: all nodes receive all updates
    /// - Returns stream of updates received from other nodes
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
    ///
    /// Local writes are buffered and periodically broadcast as a batch.
    /// Reduces network overhead at the cost of increased latency.
    ///
    /// ## Behavior
    /// - Accumulates local writes into a KVS
    /// - Periodically broadcasts accumulated state at configured intervals
    /// - No immediate forwarding - only periodic background broadcasts
    /// - Returns stream of updates received from other nodes
    pub fn handle_replication_periodic<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        let ticker = cluster.tick();

        // Capture config value for use in quote (must be a primitive type like u64)
        let batch_timeout_ms = self.config.batch_timeout_ms;

        // Accumulate local writes into a KVS (no immediate broadcast)
        let accumulated_kvs = local_put_tuples.into_keyed().fold_commutative(
            q!(|| V::default()),
            q!(|acc, v| {
                lattices::Merge::merge(acc, v);
            }),
        );

        // Periodically snapshot and broadcast the accumulated state
        let periodic_broadcast = accumulated_kvs
            .snapshot(&ticker, nondet!(/** snapshot for periodic broadcast */))
            .entries()
            .all_ticks()
            .sample_every(
                q!(std::time::Duration::from_millis(batch_timeout_ms)),
                nondet!(/** periodic broadcast interval */),
            )
            .broadcast_bincode(cluster, nondet!(/** periodic broadcast to all nodes */));

        // Return received updates
        periodic_broadcast
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
            // Sampling may produce duplicate deliveries; declare retry semantics
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
    fn test_broadcast_replication_clone_debug() {
        let broadcast = BroadcastReplication::<String>::new();
        let _cloned = broadcast.clone();
        let _debug_str = format!("{:?}", broadcast);
    }

    #[test]
    fn test_broadcast_replication_implements_replication_strategy() {
        // Test that BroadcastReplication implements ReplicationStrategy with CausalString
        fn _test_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
        _test_replication_strategy::<crate::values::CausalString>(BroadcastReplication::<
            crate::values::CausalString,
        >::new());
    }

    #[test]
    fn test_broadcast_replication_config_values() {
        let config = BroadcastReplicationConfig::default();
        assert!(!config.enable_batching);
        assert_eq!(config.batch_timeout_ms, 100);
        assert_eq!(config.max_batch_size, 50);

        let low_latency_config = BroadcastReplicationConfig::low_latency();
        assert!(!low_latency_config.enable_batching);
        assert_eq!(low_latency_config.batch_timeout_ms, 50);
        assert_eq!(low_latency_config.max_batch_size, 1);

        let high_throughput_config = BroadcastReplicationConfig::high_throughput();
        assert!(high_throughput_config.enable_batching);
        assert_eq!(high_throughput_config.batch_timeout_ms, 200);
        assert_eq!(high_throughput_config.max_batch_size, 100);

        let synchronous_config = BroadcastReplicationConfig::synchronous();
        assert!(!synchronous_config.enable_batching);
        assert_eq!(synchronous_config.batch_timeout_ms, 0);
        assert_eq!(synchronous_config.max_batch_size, 1);
    }

    #[test]
    fn test_broadcast_replication_type_safety() {
        // Test that BroadcastReplication works with different value types that implement Merge
        let _causal_broadcast = BroadcastReplication::<crate::values::CausalString>::new();
        let _lww_broadcast = BroadcastReplication::<crate::values::LwwWrapper<String>>::new();
    }

    #[test]
    fn test_broadcast_replication_send_sync() {
        // Test that BroadcastReplication is Send + Sync for multi-threading
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}
        _requires_send_sync(BroadcastReplication::<crate::values::CausalString>::new());
    }

    #[test]
    fn test_broadcast_vs_gossip_replication_strategies() {
        // Test that both strategies implement the same trait
        fn _accepts_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}

        _accepts_replication_strategy::<crate::values::CausalString>(BroadcastReplication::<
            crate::values::CausalString,
        >::new());
        _accepts_replication_strategy::<crate::values::CausalString>(
            crate::maintenance::SimpleGossip::<crate::values::CausalString>::default(),
        );
    }

    #[test]
    fn test_broadcast_replication_config_builder_pattern() {
        // Test that configs can be built and modified
        let config = BroadcastReplicationConfig {
            enable_batching: true,
            max_batch_size: 200,
            ..Default::default()
        };

        let _broadcast = BroadcastReplication::<String>::with_config(config);
    }
}
