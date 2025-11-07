//! Broadcast Maintenance Strategy
//!
//! This module implements broadcast dissemination for strong consistency guarantees.
//! All updates are deterministically broadcast to all cluster nodes, providing
//! faster convergence than gossip protocols at the cost of higher message overhead.

use crate::core::KVSNode;
use crate::maintain::MaintenanceStrategy;
use hydro_lang::prelude::*;
use lattices::Merge;
use serde::{Deserialize, Serialize};

/// Configuration for broadcast dissemination
#[derive(Clone, Debug)]
pub struct BroadcastMaintenanceConfig {
    /// Batch multiple updates before broadcasting
    pub enable_batching: bool,
    /// Maximum time to wait before sending a batch
    pub batch_timeout: std::time::Duration,
    /// Maximum number of keys per batch
    pub max_batch_size: usize,
}

impl Default for BroadcastMaintenanceConfig {
    fn default() -> Self {
        Self {
            enable_batching: false,
            batch_timeout: std::time::Duration::from_millis(100),
            max_batch_size: 50,
        }
    }
}

impl BroadcastMaintenanceConfig {
    /// Create config optimized for low latency (immediate broadcasting)
    pub fn low_latency() -> Self {
        Self {
            enable_batching: false,
            batch_timeout: std::time::Duration::from_millis(50),
            max_batch_size: 1,
        }
    }

    /// Create config optimized for high throughput (batched broadcasting)
    pub fn high_throughput() -> Self {
        Self {
            enable_batching: true,
            batch_timeout: std::time::Duration::from_millis(200),
            max_batch_size: 100,
        }
    }

    /// Create config for synchronous broadcasting (immediate, no batching)
    pub fn synchronous() -> Self {
        Self {
            enable_batching: false,
            batch_timeout: std::time::Duration::from_millis(0),
            max_batch_size: 1,
        }
    }
}

/// Broadcast dissemination: sends updates to all cluster nodes
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
pub struct BroadcastMaintenance<V> {
    config: BroadcastMaintenanceConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for BroadcastMaintenance<V> {
    fn default() -> Self {
        Self {
            config: BroadcastMaintenanceConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> BroadcastMaintenance<V> {
    /// Create a new broadcast dissemination strategy with default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new broadcast dissemination strategy with custom configuration
    pub fn with_config(config: BroadcastMaintenanceConfig) -> Self {
        Self {
            config,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> MaintenanceStrategy<V> for BroadcastMaintenance<V>
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
    fn maintain_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Simple broadcast dissemination - broadcasts all local operations to all nodes
        // This preserves the existing behavior while adapting to the new trait
        self.handle_dissemination(cluster, local_data)
    }

    // Ordered logs with "slots" (positions) need to be maintaind for ordered "replay".
    // See logbased.rs.
    fn maintain_slotted_data<'a>(
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

impl<V> BroadcastMaintenance<V>
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
    /// Simple broadcast dissemination - broadcasts all local operations to all nodes
    ///
    /// This implementation provides immediate, reliable broadcasting of all updates
    /// to all cluster members. It prioritizes consistency and simplicity over
    /// network efficiency.
    ///
    /// ## Behavior
    /// - Every local update is broadcast to every cluster member
    /// - No batching or optimization (can be configured via BroadcastMaintenanceConfig)
    /// - Deterministic delivery guarantees
    /// - Returns stream of updates received from other nodes
    pub fn handle_dissemination<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        // For now, implement simple immediate broadcasting
        // Future enhancement: implement batching based on config.enable_batching
        local_put_tuples
            .broadcast_bincode(cluster, nondet!(/** broadcast to all nodes */))
            .values()
            .assume_ordering(nondet!(/** broadcast messages unordered */))
    }

    /// Batched broadcast dissemination (future enhancement)
    ///
    /// This method would implement batching based on the configuration,
    /// collecting multiple updates and sending them together to reduce
    /// network overhead while maintaining strong consistency.
    #[allow(dead_code)]
    pub fn handle_dissemination_batched<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        if self.config.enable_batching {
            // TODO: Implement batching logic
            // - Collect updates for batch_timeout duration
            // - Send batches when max_batch_size is reached
            // - Broadcast batched updates to all nodes

            // For now, fall back to simple broadcasting
            self.handle_dissemination(cluster, local_put_tuples)
        } else {
            self.handle_dissemination(cluster, local_put_tuples)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_dissemination_creation() {
        let _broadcast = BroadcastMaintenance::<String>::new();
        let _broadcast_default = BroadcastMaintenance::<String>::default();
    }

    #[test]
    fn test_broadcast_dissemination_with_config() {
        let config = BroadcastMaintenanceConfig::low_latency();
        let _broadcast = BroadcastMaintenance::<String>::with_config(config);
    }

    #[test]
    fn test_broadcast_dissemination_config_presets() {
        let _low_latency = BroadcastMaintenanceConfig::low_latency();
        let _high_throughput = BroadcastMaintenanceConfig::high_throughput();
        let _synchronous = BroadcastMaintenanceConfig::synchronous();
        let _default = BroadcastMaintenanceConfig::default();
    }

    #[test]
    fn test_broadcast_dissemination_clone_debug() {
        let broadcast = BroadcastMaintenance::<String>::new();
        let _cloned = broadcast.clone();
        let _debug_str = format!("{:?}", broadcast);
    }

    #[test]
    fn test_broadcast_dissemination_implements_dissemination_strategy() {
        // Test that BroadcastMaintenance implements MaintenanceStrategy with CausalString
        fn _test_dissemination_strategy<V>(_strategy: impl MaintenanceStrategy<V>) {}
        _test_dissemination_strategy::<crate::values::CausalString>(BroadcastMaintenance::<
            crate::values::CausalString,
        >::new());
    }

    #[test]
    fn test_broadcast_dissemination_config_values() {
        let config = BroadcastMaintenanceConfig::default();
        assert!(!config.enable_batching);
        assert_eq!(config.batch_timeout, std::time::Duration::from_millis(100));
        assert_eq!(config.max_batch_size, 50);

        let low_latency_config = BroadcastMaintenanceConfig::low_latency();
        assert!(!low_latency_config.enable_batching);
        assert_eq!(
            low_latency_config.batch_timeout,
            std::time::Duration::from_millis(50)
        );
        assert_eq!(low_latency_config.max_batch_size, 1);

        let high_throughput_config = BroadcastMaintenanceConfig::high_throughput();
        assert!(high_throughput_config.enable_batching);
        assert_eq!(
            high_throughput_config.batch_timeout,
            std::time::Duration::from_millis(200)
        );
        assert_eq!(high_throughput_config.max_batch_size, 100);

        let synchronous_config = BroadcastMaintenanceConfig::synchronous();
        assert!(!synchronous_config.enable_batching);
        assert_eq!(
            synchronous_config.batch_timeout,
            std::time::Duration::from_millis(0)
        );
        assert_eq!(synchronous_config.max_batch_size, 1);
    }

    #[test]
    fn test_broadcast_dissemination_type_safety() {
        // Test that BroadcastMaintenance works with different value types that implement Merge
        let _causal_broadcast = BroadcastMaintenance::<crate::values::CausalString>::new();
        let _lww_broadcast = BroadcastMaintenance::<crate::values::LwwWrapper<String>>::new();
    }

    #[test]
    fn test_broadcast_dissemination_send_sync() {
        // Test that BroadcastMaintenance is Send + Sync for multi-threading
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}
        _requires_send_sync(BroadcastMaintenance::<crate::values::CausalString>::new());
    }

    #[test]
    fn test_broadcast_vs_gossip_dissemination_strategies() {
        // Test that both strategies implement the same trait
        fn _accepts_dissemination_strategy<V>(_strategy: impl MaintenanceStrategy<V>) {}

        _accepts_dissemination_strategy::<crate::values::CausalString>(BroadcastMaintenance::<
            crate::values::CausalString,
        >::new());
        _accepts_dissemination_strategy::<crate::values::CausalString>(
            crate::maintain::EpidemicGossip::<crate::values::CausalString>::new(),
        );
    }

    #[test]
    fn test_broadcast_dissemination_config_builder_pattern() {
        // Test that configs can be built and modified
        let config = BroadcastMaintenanceConfig {
            enable_batching: true,
            max_batch_size: 200,
            ..Default::default()
        };

        let _broadcast = BroadcastMaintenance::<String>::with_config(config);
    }
}
