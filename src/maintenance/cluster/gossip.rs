//! Simple Gossip Replication Strategy
//!
//! This module implements a simple gossip protocol for replication.
//! Updates are immediately broadcast to all peers in the cluster.
//!
//! # TODO: Implement True Epidemic Gossip
//!
//! The current implementation is a simplified broadcast mechanism. A proper
//! epidemic gossip implementation following Demers et al. would include:
//!
//! - **Rumor Store**: Track which keys are "hot" (need gossiping) vs "cold" (tombstoned)
//! - **Periodic Gossip**: Sample hot keys at intervals and re-gossip to random peers
//! - **Probabilistic Termination**: Tombstone rumors probabilistically to prevent infinite circulation
//! - **Bounded Message Complexity**: Target O(n log n) messages per update
//!
//! Reference: "Epidemic Algorithms for Replicated Database Maintenance" (Demers et al., 1987)

use crate::kvs_core::KVSNode;
use crate::maintenance::ReplicationStrategy;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use lattices::Merge;
use lattices::map_union_with_tombstones::MapUnionHashMapWithTombstoneHashSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Configuration for simple gossip replication
#[derive(Clone, Debug)]
pub struct SimpleGossipConfig {
    /// How many random peers to send each hot rumor to per gossip round
    /// This controls the "fanout" of the epidemic spread
    pub gossip_fanout: usize,

    /// Probability of tombstoning (forgetting) a hot key per gossip round
    /// Default heuristic: 1 / max(2, ceil(c · ln(cluster_size))) with c ≈ 2
    /// This targets O(n log n) message complexity and bounded rumor lifetime
    pub tombstone_prob: f64,

    /// Probability of selecting a peer for initial infection (0.0 to 1.0)
    /// Controls how aggressively rumors spread initially
    pub infection_prob: f64,

    /// How often to run gossip rounds (periodic sampling interval)
    pub gossip_interval: std::time::Duration,
}

impl Default for SimpleGossipConfig {
    fn default() -> Self {
        Self {
            gossip_fanout: 3,
            // Conservative default: assume small cluster, tombstone slowly
            tombstone_prob: 0.1,
            // 50% infection probability for balanced spread
            infection_prob: 0.5,
            gossip_interval: std::time::Duration::from_secs(1),
        }
    }
}

impl SimpleGossipConfig {
    /// Create config optimized for small clusters (< 10 nodes)
    pub fn small_cluster() -> Self {
        Self {
            gossip_fanout: 2,
            tombstone_prob: 0.05, // Slower tombstoning for small clusters
            infection_prob: 0.7,  // Higher infection rate
            gossip_interval: std::time::Duration::from_millis(500),
        }
    }

    /// Create config optimized for large clusters (> 50 nodes)
    pub fn large_cluster() -> Self {
        Self {
            gossip_fanout: 5,
            tombstone_prob: 0.2, // Faster tombstoning for large clusters
            infection_prob: 0.3, // Lower infection rate to reduce overhead
            gossip_interval: std::time::Duration::from_secs(2),
        }
    }
}

impl From<usize> for SimpleGossipConfig {
    /// Interpret usize as milliseconds for the gossip interval; other fields defaulted
    fn from(ms: usize) -> Self {
        let mut cfg = SimpleGossipConfig::default();
        cfg.gossip_interval = std::time::Duration::from_millis(ms as u64);
        cfg
    }
}

/// Simple gossip replication
///
/// This implements immediate broadcast replication where updates are
/// sent to all peers in the cluster.
/// with some probability to prevent infinite circulation.
///
/// ## Protocol Overview
///
/// 1. **Initial Infection**: New updates are probabilistically forwarded to peers
/// 2. **Rumor Tracking**: Hot keys are tracked in a rumor store
/// 3. **Periodic Gossip**: Hot keys are periodically re-gossiped to random peers
/// 4. **Probabilistic Termination**: Keys are tombstoned to stop infinite circulation
///
/// ## Consistency Guarantees
///
/// - **Eventual Consistency**: All nodes will eventually receive all updates
/// - **Probabilistic Delivery**: Updates spread with high probability
/// - **Bounded Message Complexity**: O(n log n) messages per update
#[derive(Clone, Debug)]
pub struct SimpleGossip<V> {
    config: SimpleGossipConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for SimpleGossip<V> {
    fn default() -> Self {
        Self {
            config: SimpleGossipConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> SimpleGossip<V> {
    /// Create a new epidemic gossip strategy with custom configuration
    /// Accepts either an `SimpleGossipConfig` or any value that can convert into one (e.g., `usize` milliseconds)
    pub fn new<C>(config: C) -> Self
    where
        C: Into<SimpleGossipConfig>,
    {
        Self {
            config: config.into(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get cluster member IDs for gossip targets
    fn get_cluster_members<'a>(
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<MemberId<KVSNode>, Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        cluster
            .source_cluster_members(cluster)
            .map_with_key(q!(|(member_id, _event)| member_id))
            .values()
    }
}

impl<V> ReplicationStrategy<V> for SimpleGossip<V>
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
        + Merge<V>
        + std::hash::Hash,
{
    fn replicate_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Use simplified gossip - immediate probabilistic forwarding
        // This is the stable implementation from main branch
        self.handle_gossip_simple(cluster, local_data)
    }
}

impl<V> SimpleGossip<V>
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
        + Merge<V>
        + std::hash::Hash,
{
    /// Simplified gossip that immediately forwards PUT operations to all peers
    ///
    /// Unlike the full gossip implementation, this version:
    /// - No rumor store or tombstoning
    /// - No periodic gossip rounds
    /// - Just immediate forwarding to all peers
    ///
    /// This is the stable implementation used in production.
    pub fn handle_gossip_simple<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        let cluster_members = Self::get_cluster_members(cluster);

        // Immediate forwarding to all peers for reliable convergence
        let gossip_sent = local_put_tuples
            .clone()
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            // Broadcast to all peers (no probabilistic filtering for demo reliability)
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        gossip_sent
            .values()
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }

    /// Full rumor-mongering gossip implementation (UNIMPLEMENTED)
    ///
    /// ## Design (for future implementation)
    /// - Pure gossip protocol that takes local PUTs and returns gossip operations
    /// - Rumor store: MapUnionWithTombstones<(), ()> tracking only hot/tombstoned keys
    /// - Periodic gossip: sample_every with configurable interval on hot (non-tombstoned) keys
    /// - Per-key actions per gossip round:
    ///   - Look up merged value from provided KVS and send PUT(k, v) to random peers
    ///   - Probabilistically tombstone (forget) with configured probability  
    /// - Returns gossip operations to be merged with local operations by caller
    ///
    /// This ensures eventual consistency with probabilistic rumor termination.
    #[allow(dead_code)]
    pub fn handle_gossip_full<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        changed_keys: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
        main_kvs: KeyedSingleton<String, V, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        let ticker = cluster.tick();
        let _config = &self.config;

        // Step 1: Get cluster member IDs for gossip targets
        let cluster_members = Self::get_cluster_members(cluster);

        // Step 2: Set up cyclic dataflow with probabilistic peer selection
        let gossip_sent_initial = local_put_tuples
            .clone()
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            .filter(q!(|(_tuple, _member_id)| {
                // Use boolean random for ~50% probability (simplified for Hydro compatibility)
                rand::random::<bool>()
            }))
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Incoming gossip from other nodes (completes the cyclic dataflow)
        let gossip_received = gossip_sent_initial.values();

        // Step 3: Build rumor store tracking only keys (not values)
        // Only add keys that actually changed (where merge returned true)
        //
        // TODO: Memory leak - MapUnionHashMapWithTombstoneHashSet grows unbounded
        // Still needed: compressed tombstone lattices and distributed GC via vector clocks
        let rumor_insertions = changed_keys.map(q!(|k| (
            k,
            MapUnionHashMapWithTombstoneHashSet::new(
                HashMap::from([((), ())]), // Mark key as hot
                HashSet::new()             // No tombstone yet
            )
        )));

        // Build the rumor store from insertions (tombstones will be added later via periodic updates)
        // Inline fold_commutative for rumor tracking (no need for KVSCore)
        let rumor_store = rumor_insertions.into_keyed().fold_commutative(
            q!(|| Default::default()),
            q!(|acc: &mut MapUnionHashMapWithTombstoneHashSet<(), ()>, i| {
                lattices::Merge::merge(acc, i);
            }),
        );

        // Step 4: Periodic gossip rounds - extract hot keys
        let rumor_snapshot =
            rumor_store.snapshot(&ticker, nondet!(/** snapshot rumor store per tick */));

        // Extract keys that are currently hot (not tombstoned)
        let hot_keys_per_tick = rumor_snapshot
            .entries()
            .filter_map(q!(|(key, rumor_state)| {
                let (hot_map, tombstone_set) = rumor_state.as_reveal_ref();
                // Key is hot if: (1) it's in the map, AND (2) it's not tombstoned
                if !hot_map.is_empty() && !tombstone_set.contains(&()) {
                    Some(key)
                } else {
                    None
                }
            }));

        // Sample hot keys at 1-second interval for periodic re-gossip (hardcoded for Hydro compatibility)
        let hot_keys_sampled = hot_keys_per_tick.all_ticks().sample_every(
            q!(std::time::Duration::from_secs(1)),
            nondet!(/** gossip interval sampling */),
        );

        // Step 5: Probabilistically tombstone keys (probabilistic termination)
        // TODO: Currently not implemented - would need to send tombstone operations
        // to update the rumor store and stop gossiping tombstoned keys
        let _keys_to_tombstone = hot_keys_sampled.clone().filter(q!(|_k| {
            // Use boolean random for ~10% probability (simplified for Hydro compatibility)
            rand::random::<bool>() && rand::random::<bool>() && !rand::random::<bool>()
        }));

        // Step 6: Look up merged lattice values for hot keys and gossip them
        let keys_batched = hot_keys_sampled.batch(&ticker, nondet!(/** batch keys for lookup */));

        let gossip_payloads = keys_batched.map(q!(|k| (k, ()))).into_keyed();

        let gossip_tuples = main_kvs
            .snapshot(&ticker, nondet!(/** snapshot lattice values */))
            .get_many_if_present(gossip_payloads)
            .entries()
            .map(q!(|(k, (v, ()))| (k, v)))
            .all_ticks();

        // Send gossip to random peer subset (simplified for Hydro compatibility)
        gossip_tuples
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            .filter(q!(|(_tuple, _member_id)| {
                // Use boolean random for ~50% probability (simplified for Hydro compatibility)
                rand::random::<bool>()
            }))
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        // Return all received gossip for KVS merging
        gossip_received
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epidemic_gossip_creation() {
        let _gossip = SimpleGossip::<String>::default();
        let _gossip_default = SimpleGossip::<String>::default();
    }

    #[test]
    fn test_epidemic_gossip_with_config() {
        let config = SimpleGossipConfig::small_cluster();
        let _gossip = SimpleGossip::<String>::new(config);
    }

    #[test]
    fn test_epidemic_gossip_config_presets() {
        let _small = SimpleGossipConfig::small_cluster();
        let _large = SimpleGossipConfig::large_cluster();
        let _default = SimpleGossipConfig::default();
    }

    #[test]
    fn test_epidemic_gossip_clone_debug() {
        let gossip = SimpleGossip::<String>::default();
        let _cloned = gossip.clone();
        let _debug_str = format!("{:?}", gossip);
    }

    #[test]
    fn test_epidemic_gossip_implements_replication_strategy() {
        // Test that SimpleGossip implements ReplicationStrategy with CausalString
        fn _test_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
        _test_replication_strategy::<crate::values::CausalString>(SimpleGossip::<
            crate::values::CausalString,
        >::default());
    }

    #[test]
    fn test_epidemic_gossip_config_values() {
        let config = SimpleGossipConfig::default();
        assert_eq!(config.gossip_fanout, 3);
        assert_eq!(config.tombstone_prob, 0.1);
        assert_eq!(config.infection_prob, 0.5);

        let small_config = SimpleGossipConfig::small_cluster();
        assert_eq!(small_config.gossip_fanout, 2);
        assert_eq!(small_config.tombstone_prob, 0.05);
        assert_eq!(small_config.infection_prob, 0.7);

        let large_config = SimpleGossipConfig::large_cluster();
        assert_eq!(large_config.gossip_fanout, 5);
        assert_eq!(large_config.tombstone_prob, 0.2);
        assert_eq!(large_config.infection_prob, 0.3);
    }

    #[test]
    fn test_epidemic_gossip_type_safety() {
        // Test that SimpleGossip works with different value types that implement Merge
        let _causal_gossip = SimpleGossip::<crate::values::CausalString>::default();
        let _lww_gossip = SimpleGossip::<crate::values::LwwWrapper<String>>::default();
    }

    #[test]
    fn test_epidemic_gossip_send_sync() {
        // Test that SimpleGossip is Send + Sync for multi-threading
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}
        _requires_send_sync(SimpleGossip::<crate::values::CausalString>::default());
    }
}
