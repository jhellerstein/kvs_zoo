//! Maintenance Strategies for KVS Systems
//!
//! This module provides data maintenance strategies organized by scope:
//! - **cluster**: Strategies that coordinate across multiple nodes (gossip, broadcast, log-based)
//! - **node**: Strategies that operate on individual nodes (tombstone cleanup)
//!
//! ## Core Concepts
//!
//! - **ReplicationStrategy**: Trait for background data synchronization
//! - **NoReplication**: No-op strategy for single-node systems
//! - **Unit type ()**: Alternative no-op implementation
//!
//! ## Cluster-level strategies
//! - **EpidemicGossip**: Gossip-based eventual consistency between replicas
//! - **BroadcastReplication**: Broadcast-based strong consistency
//! - **LogBased**: Log-based replication (used with Paxos)
//!
//! ## Node-level strategies
//! - **TombstoneCleanup**: Garbage collection for deleted entries
//!
//! ## Separation of Concerns
//!
//! Maintenance strategies are completely separate from operation dispatchers:
//! - Operation dispatchers handle incoming operations (routing, ordering, filtering)
//! - Maintenance strategies handle background data consistency
//!
//! ## Usage
//!
//! ```rust
//! use kvs_zoo::maintenance::{ReplicationStrategy, NoReplication};
//! use kvs_zoo::maintenance::cluster::{EpidemicGossip, BroadcastReplication};
//! use kvs_zoo::maintenance::node::TombstoneCleanup;
//!
//! // Single-node system (no replication needed)
//! let replication = NoReplication::new();
//! // or
//! let replication = ();
//!
//! // Multi-node system with gossip
//! let replication: EpidemicGossip<String> = EpidemicGossip::default();
//!
//! // Multi-node system with broadcast
//! let replication: BroadcastReplication<String> = BroadcastReplication::default();
//! ```

pub mod cluster;
pub mod node;

use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

// Re-export commonly used strategies for convenience
pub use cluster::{BroadcastReplication, BroadcastReplicationConfig, EpidemicGossip, EpidemicGossipConfig, LogBased};
pub use node::{TombstoneCleanup, TombstoneCleanupConfig};

/// Core trait for replication strategies
///
/// Replication strategies handle background data synchronization between nodes,
/// operating independently of operation processing. They ensure data consistency
/// and availability across the distributed system.
pub trait ReplicationStrategy<V> {
    /// Replicate data across the cluster (unordered)
    ///
    /// Takes a stream of local data updates and returns a stream of data
    /// replicated by other nodes. The strategy determines how data is
    /// synchronized (gossip, broadcast, etc.).
    fn replicate_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Replicate slotted data across the cluster (ordered by slot)
    ///
    /// Takes a stream of slot-indexed data updates and returns a stream of
    /// replicated data received from other nodes, maintaining slot ordering.
    /// This is used by consensus protocols like Paxos to ensure operations
    /// are applied in the same order across all replicas.
    ///
    /// Default implementation simply strips slots and uses unordered replication.
    fn replicate_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Default: strip slots, replicate unordered (loses ordering guarantees)
        // Note: This doesn't preserve slots properly - use LogBased wrapper for proper ordering
        let unslotted = local_slotted_data.map(q!(|(_slot, key, value)| (key, value)));
        let replicated = self.replicate_data(cluster, unslotted);
        // Re-add dummy slot 0 (ordering is lost)
        replicated.map(q!(|(key, value)| (0usize, key, value)))
    }
}

/// No-op replication strategy for single-node systems
///
/// This strategy performs no replication, making it suitable for:
/// - Single-node deployments
/// - Development and testing
/// - Systems that don't require replication
#[derive(Clone, Debug, Default)]
pub struct NoReplication;

impl NoReplication {
    /// Create a new no-replication strategy
    pub fn new() -> Self {
        Self
    }
}

impl<V> ReplicationStrategy<V> for NoReplication {
    fn replicate_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No replication - just return the local data stream unchanged
        local_data
    }
}

/// Unit type implementation for no replication
///
/// The unit type `()` can be used as a convenient no-op replication strategy,
/// providing the same behavior as NoReplication with even less overhead.
impl<V> ReplicationStrategy<V> for () {
    fn replicate_data<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No replication - just return the local data stream unchanged
        local_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_replication_creation() {
        let _no_repl = NoReplication::new();
        let _no_repl_default = NoReplication::new();
    }

    #[test]
    fn test_no_replication_clone_debug() {
        let no_repl = NoReplication::new();
        let _cloned = no_repl.clone();
        let _debug_str = format!("{:?}", no_repl);
    }

    #[test]
    fn test_unit_type_replication_strategy() {
        // Test that unit type can be used as replication strategy
        let unit_repl: () = ();

        // This should compile, demonstrating that () implements ReplicationStrategy
        fn _test_unit_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
        _test_unit_replication_strategy::<String>(unit_repl);
        _test_unit_replication_strategy::<String>(NoReplication::new());
    }

    #[test]
    fn test_replication_strategy_trait_implementations() {
        // Test that both NoReplication and () implement ReplicationStrategy
        fn _accepts_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}

        // Test NoReplication
        _accepts_replication_strategy::<String>(NoReplication::new());
        _accepts_replication_strategy::<i32>(NoReplication::new());

        // Test unit type
        _accepts_replication_strategy::<String>(());
        _accepts_replication_strategy::<Vec<u8>>(());
    }

    #[test]
    fn test_replication_strategy_trait_object() {
        // Test that we can use trait objects if needed
        let strategies: Vec<Box<dyn ReplicationStrategy<String>>> = vec![
            Box::new(NoReplication::new()),
            // Unit type can't be boxed directly, but NoReplication serves the same purpose
        ];

        assert_eq!(strategies.len(), 1);
    }

    #[test]
    fn test_replication_strategy_generic_types() {
        // Test that replication strategies work with different value types
        fn _test_with_type<V, R: ReplicationStrategy<V>>(_strategy: R) {}

        // Test with different value types
        _test_with_type::<String, NoReplication>(NoReplication::new());
        _test_with_type::<i32, NoReplication>(NoReplication::new());
        _test_with_type::<Vec<u8>, NoReplication>(NoReplication::new());

        _test_with_type::<String, ()>(());
        _test_with_type::<i32, ()>(());
        _test_with_type::<Vec<u8>, ()>(());
    }

    #[test]
    fn test_no_replication_vs_unit_type_equivalence() {
        // Test that NoReplication and () provide equivalent functionality
        // Both should be usable in the same contexts

        fn _use_no_replication_strategy<V>(_strategy: NoReplication) {}
        fn _use_unit_replication_strategy<V>(_strategy: ()) {}
        fn _use_any_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}

        let no_repl = NoReplication::new();
        let unit_repl = ();

        _use_no_replication_strategy::<String>(no_repl);
        _use_unit_replication_strategy::<String>(unit_repl);

        _use_any_replication_strategy::<String>(NoReplication::new());
        _use_any_replication_strategy::<String>(());
    }

    #[test]
    fn test_replication_strategy_send_sync() {
        // Test that replication strategies are Send + Sync for multi-threading
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}

        _requires_send_sync(NoReplication::new());
        _requires_send_sync(());
    }

    #[test]
    fn test_replication_strategy_static_lifetime() {
        // Test that replication strategies have static lifetime
        fn _requires_static<T: 'static>(_t: T) {}

        _requires_static(NoReplication::new());
        _requires_static(());
    }

    #[test]
    fn test_replication_strategy_type_safety() {
        // Test compile-time type safety for replication strategies

        // These should all compile without issues
        let _string_no_repl: NoReplication = NoReplication::new();
        let _unit_repl: () = ();

        // Test that the trait is properly parameterized
        fn _typed_replication_strategy<V>() -> impl ReplicationStrategy<V> {
            NoReplication::new()
        }

        let _string_strategy = _typed_replication_strategy::<String>();
        let _int_strategy = _typed_replication_strategy::<i32>();
    }
}
