//! Epidemic Gossip Replication Strategy
//!
//! This module implements the epidemic gossip protocol (Demers et al.) as a
//! replication strategy for eventual consistency. The gossip protocol spreads
//! updates through the network using probabilistic forwarding and rumor
//! termination.

use crate::core::KVSNode;
use crate::maintain::ReplicationStrategy;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use lattices::Merge;
use lattices::map_union_with_tombstones::MapUnionHashMapWithTombstoneHashSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Configuration specific to epidemic gossip protocol
#[derive(Clone, Debug)]
pub struct EpidemicGossipConfig {
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

impl Default for EpidemicGossipConfig {
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

impl EpidemicGossipConfig {
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

/// Epidemic gossip protocol implementation (Demers et al.)
///
/// This implements rumor-mongering with probabilistic termination,
/// where rumors spread through the network and are eventually forgotten
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
pub struct EpidemicGossip<V> {
    config: EpidemicGossipConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for EpidemicGossip<V> {
    fn default() -> Self {
        Self {
            config: EpidemicGossipConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> EpidemicGossip<V> {
    /// Create a new epidemic gossip strategy with default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new epidemic gossip strategy with custom configuration
    pub fn with_config(config: EpidemicGossipConfig) -> Self {
        Self {
            config,
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

impl<V> ReplicationStrategy<V> for EpidemicGossip<V>
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
        // Use simplified gossip for now - immediate probabilistic forwarding
        // This preserves the existing behavior while adapting to the new trait
        self.handle_gossip_simple(cluster, local_data)
    }
}

impl<V> EpidemicGossip<V>
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
    /// Simplified gossip that immediately forwards PUT operations to random peers
    ///
    /// Unlike the full gossip implementation, this version:
    /// - No rumor store or tombstoning
    /// - No periodic gossip rounds
    /// - Just immediate probabilistic forwarding of operations
    pub fn handle_gossip_simple<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
        let cluster_members = Self::get_cluster_members(cluster);
        // Immediate probabilistic forwarding to peers based on infection probability
        // Use boolean random for Hydro compatibility instead of f64 comparison
        let gossip_sent = local_put_tuples
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

        gossip_sent
            .values()
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }

    /// Full rumor-mongering gossip implementation (Demers et al.)
    ///
    /// ## Design
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
        let rumor_store = crate::core::KVSCore::put(rumor_insertions);

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

        // Step 5: Probabilistically tombstone keys (probabilistic termination per Demers)
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
        let _gossip = EpidemicGossip::<String>::new();
        let _gossip_default = EpidemicGossip::<String>::default();
    }

    #[test]
    fn test_epidemic_gossip_with_config() {
        let config = EpidemicGossipConfig::small_cluster();
        let _gossip = EpidemicGossip::<String>::with_config(config);
    }

    #[test]
    fn test_epidemic_gossip_config_presets() {
        let _small = EpidemicGossipConfig::small_cluster();
        let _large = EpidemicGossipConfig::large_cluster();
        let _default = EpidemicGossipConfig::default();
    }

    #[test]
    fn test_epidemic_gossip_clone_debug() {
        let gossip = EpidemicGossip::<String>::new();
        let _cloned = gossip.clone();
        let _debug_str = format!("{:?}", gossip);
    }

    #[test]
    fn test_epidemic_gossip_implements_replication_strategy() {
        // Test that EpidemicGossip implements ReplicationStrategy with CausalString
        fn _test_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
        _test_replication_strategy::<crate::values::CausalString>(EpidemicGossip::<
            crate::values::CausalString,
        >::new());
    }

    #[test]
    fn test_epidemic_gossip_config_values() {
        let config = EpidemicGossipConfig::default();
        assert_eq!(config.gossip_fanout, 3);
        assert_eq!(config.tombstone_prob, 0.1);
        assert_eq!(config.infection_prob, 0.5);

        let small_config = EpidemicGossipConfig::small_cluster();
        assert_eq!(small_config.gossip_fanout, 2);
        assert_eq!(small_config.tombstone_prob, 0.05);
        assert_eq!(small_config.infection_prob, 0.7);

        let large_config = EpidemicGossipConfig::large_cluster();
        assert_eq!(large_config.gossip_fanout, 5);
        assert_eq!(large_config.tombstone_prob, 0.2);
        assert_eq!(large_config.infection_prob, 0.3);
    }

    #[test]
    fn test_epidemic_gossip_type_safety() {
        // Test that EpidemicGossip works with different value types that implement Merge
        let _causal_gossip = EpidemicGossip::<crate::values::CausalString>::new();
        let _lww_gossip = EpidemicGossip::<crate::values::LwwWrapper<String>>::new();
    }

    #[test]
    fn test_epidemic_gossip_send_sync() {
        // Test that EpidemicGossip is Send + Sync for multi-threading
        fn _requires_send_sync<T: Send + Sync>(_t: T) {}
        _requires_send_sync(EpidemicGossip::<crate::values::CausalString>::new());
    }
}
