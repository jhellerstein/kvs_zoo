//! Maintenance Strategies for KVS Systems
//!
//! This module provides background data synchronization strategies that operate
//! independently of operation processing. Maintenance strategies handle concerns
//! like eventual consistency, strong consistency, and anti-entropy repair.
//!
//! ## Core Concepts
//!
//! - **MaintenanceStrategy**: Trait for background data synchronization
//! - **NoMaintenance**: No-op strategy for single-node systems
//! - **Unit type ()**: Alternative no-op implementation
//! - **EpidemicGossip**: Gossip-based eventual consistency
//! - **BroadcastMaintenance**: Broadcast-based strong consistency
//!
//! ## Separation of Concerns
//!
//! Maintenance strategies are completely separate from operation interceptors:
//! - Operation interceptors handle incoming operations (routing, consensus, auth)
//! - Maintenance strategies handle background data sync between nodes
//!
//! ## Usage
//!
//! ```rust
//! use kvs_zoo::maintain::{MaintenanceStrategy, NoMaintenance, EpidemicGossip, BroadcastMaintenance};
//!
//! // Single-node system (no maintenance needed)
//! let maintenance = NoMaintenance::new();
//! // or
//! let maintenance = ();
//!
//! // Multi-node system with gossip
//! let maintenance: EpidemicGossip<String> = EpidemicGossip::default();
//!
//! // Multi-node system with broadcast
//! let maintenance: BroadcastMaintenance<String> = BroadcastMaintenance::default();
//! ```

pub mod broadcast;
pub mod gossip;
pub mod logbased;

use crate::core::KVSNode;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Re-export dissemination strategies for convenience
pub use broadcast::{BroadcastMaintenance, BroadcastMaintenanceConfig};
pub use gossip::{EpidemicGossip, EpidemicGossipConfig};
pub use logbased::LogBased;

/// Core trait for dissemination strategies
///
/// Maintenance strategies handle background data synchronization between nodes,
/// operating independently of operation processing. They ensure data consistency
/// and availability across the distributed system.
pub trait MaintenanceStrategy<V> {
    /// Replicate data across the cluster (unordered)
    ///
    /// Takes a stream of local data updates and returns a stream of maintaind
    /// data received from other nodes. The strategy determines how data is
    /// synchronized (gossip, broadcast, etc.).
    fn maintain_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Replicate slotted data across the cluster (ordered by slot)
    ///
    /// Takes a stream of slot-indexed data updates and returns a stream of
    /// maintaind data received from other nodes, maintaining slot ordering.
    /// This is used by consensus protocols like Paxos to ensure operations
    /// are applied in the same order across all replicas.
    ///
    /// Default implementation simply strips slots and uses unordered dissemination.
    fn maintain_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Default: strip slots, maintain unordered (loses ordering guarantees)
        // Note: This doesn't preserve slots properly - use LogBased wrapper for proper ordering
        let unslotted = local_slotted_data.map(q!(|(_slot, key, value)| (key, value)));
        let maintaind = self.maintain_data(cluster, unslotted);
        // Re-add dummy slot 0 (ordering is lost)
        maintaind.map(q!(|(key, value)| (0usize, key, value)))
    }
}

/// No-op dissemination strategy for single-node systems
///
/// This strategy performs no dissemination, making it suitable for:
/// - Single-node deployments
/// - Development and testing
/// - Systems that don't require dissemination
#[derive(Clone, Debug, Default)]
pub struct NoMaintenance;

impl NoMaintenance {
    /// Create a new no-dissemination strategy
    pub fn new() -> Self {
        Self
    }
}

impl<V> MaintenanceStrategy<V> for NoMaintenance {
    fn maintain_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No dissemination - just return the local data stream unchanged
        local_data
    }
}

/// Unit type implementation for no dissemination
///
/// The unit type `()` can be used as a convenient no-op dissemination strategy,
/// providing the same behavior as NoMaintenance with even less overhead.
impl<V> MaintenanceStrategy<V> for () {
    fn maintain_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No dissemination - just return the local data stream unchanged
        local_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_dissemination_creation() {
        let _no_repl = NoMaintenance::new();
        let _no_repl_default = NoMaintenance::new();
    }

    #[test]
    fn test_no_dissemination_clone_debug() {
        let no_repl = NoMaintenance::new();
        let _cloned = no_repl.clone();
        let _debug_str = format!("{:?}", no_repl);
    }

    #[test]
    fn test_unit_type_dissemination_strategy() {
        // Test that unit type can be used as dissemination strategy
        let unit_repl: () = ();

        // This should compile, demonstrating that () implements MaintenanceStrategy
        fn _test_unit_dissemination_strategy<V>(_strategy: impl MaintenanceStrategy<V>) {}
        _test_unit_dissemination_strategy::<String>(unit_repl);
        _test_unit_dissemination_strategy::<String>(NoMaintenance::new());
    }

    #[test]
    fn test_dissemination_strategy_trait_implementations() {
        // Test that both NoMaintenance and () implement MaintenanceStrategy
        fn _accepts_dissemination_strategy<V>(_strategy: impl MaintenanceStrategy<V>) {}

        // Test NoMaintenance
        _accepts_dissemination_strategy::<String>(NoMaintenance::new());
        _accepts_dissemination_strategy::<i32>(NoMaintenance::new());

        // Test unit type
        _accepts_dissemination_strategy::<String>(());
        _accepts_dissemination_strategy::<Vec<u8>>(());
    }

    #[test]
    fn test_dissemination_strategy_trait_object() {
        // Test that we can use trait objects if needed
        let strategies: Vec<Box<dyn MaintenanceStrategy<String>>> = vec![
            Box::new(NoMaintenance::new()),
            // Unit type can't be boxed directly, but NoMaintenance serves the same purpose
        ];

        assert_eq!(strategies.len(), 1);
    }

    #[test]
    fn test_dissemination_strategy_generic_types() {
        // Test that dissemination strategies work with different value types
        fn _test_with_type<V, R: MaintenanceStrategy<V>>(_strategy: R) {}

        // Test with different value types
        _test_with_type::<String, NoMaintenance>(NoMaintenance::new());
        _test_with_type::<i32, NoMaintenance>(NoMaintenance::new());
        _test_with_type::<Vec<u8>, NoMaintenance>(NoMaintenance::new());

        _test_with_type::<String, ()>(());
        _test_with_type::<i32, ()>(());
        _test_with_type::<Vec<u8>, ()>(());
    }

    #[test]
    fn test_no_dissemination_vs_unit_type_equivalence() {
        // Test that NoMaintenance and () provide equivalent functionality
        // Both should be usable in the same contexts

        fn _use_no_dissemination_strategy<V>(_strategy: NoMaintenance) {}
        fn _use_unit_dissemination_strategy<V>(_strategy: ()) {}
        fn _use_any_dissemination_strategy<V>(_strategy: impl MaintenanceStrategy<V>) {}

        let no_repl = NoMaintenance::new();
        let unit_repl = ();

        _use_no_dissemination_strategy::<String>(no_repl);
        _use_unit_dissemination_strategy::<String>(unit_repl);

        _use_any_dissemination_strategy::<String>(NoMaintenance::new());
        _use_any_dissemination_strategy::<String>(());
    }

    #[test]
    fn test_dissemination_strategy_send_sync() {
        // Test that dissemination strategies are Send + Sync for multi-threading
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}

        _requires_send_sync(NoMaintenance::new());
        _requires_send_sync(());
    }

    #[test]
    fn test_dissemination_strategy_static_lifetime() {
        // Test that dissemination strategies have static lifetime
        fn _requires_static<T: 'static>(_t: T) {}

        _requires_static(NoMaintenance::new());
        _requires_static(());
    }

    #[test]
    fn test_dissemination_strategy_type_safety() {
        // Test compile-time type safety for dissemination strategies

        // These should all compile without issues
        let _string_no_repl: NoMaintenance = NoMaintenance::new();
        let _unit_repl: () = ();

        // Test that the trait is properly parameterized
        fn _typed_dissemination_strategy<V>() -> impl MaintenanceStrategy<V> {
            NoMaintenance::new()
        }

        let _string_strategy = _typed_dissemination_strategy::<String>();
        let _int_strategy = _typed_dissemination_strategy::<i32>();
    }
}
