//! Simple Gossip Replication Strategy (after-storage)

use crate::after_storage::{AfterResponses, ClusterCommunication, ReplicationStrategy};
use crate::kvs_core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use lattices::map_union_with_tombstones::MapUnionWithTombstones;
use lattices::Merge;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for simple gossip replication
#[derive(Clone, Debug)]
pub struct SimpleGossipConfig {
    /// How many random peers to send each hot rumor to per gossip round
    pub gossip_fanout: usize,
    /// Probability of tombstoning (forgetting) a hot key per gossip round
    pub tombstone_prob: f64,
    /// Probability of selecting a peer for initial infection (0.0 to 1.0)
    pub infection_prob: f64,
    /// How often to run gossip rounds (periodic sampling interval)
    pub gossip_interval: std::time::Duration,
}

impl Default for SimpleGossipConfig {
    fn default() -> Self {
        Self {
            gossip_fanout: 3,
            tombstone_prob: 0.1,
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
            tombstone_prob: 0.05,
            infection_prob: 0.7,
            gossip_interval: std::time::Duration::from_millis(500),
        }
    }

    /// Create config optimized for large clusters (> 50 nodes)
    pub fn large_cluster() -> Self {
        Self {
            gossip_fanout: 5,
            tombstone_prob: 0.2,
            infection_prob: 0.3,
            gossip_interval: std::time::Duration::from_secs(2),
        }
    }
}

impl From<usize> for SimpleGossipConfig {
    /// Interpret usize as milliseconds for the gossip interval; other fields defaulted
    fn from(ms: usize) -> Self {
        SimpleGossipConfig {
            gossip_interval: std::time::Duration::from_millis(ms as u64),
            ..Default::default()
        }
    }
}

/// Simple gossip replication with configurable storage types
///
/// Type parameters:
/// - `K`: Key type
/// - `V`: Value type  
/// - `M`: Map implementation (e.g., `HashMap<K, V>`, `BTreeMap<K, V>`)
/// - `T`: Tombstone set implementation (e.g., `FstTombstoneSet<String>`, `RoaringTombstoneSet`, `HashSet<K>`)
#[derive(Clone, Debug)]
pub struct SimpleGossip<K, V, M = HashMap<K, V>, T = std::collections::HashSet<K>> {
    #[allow(dead_code)]
    config: SimpleGossipConfig,
    _phantom: std::marker::PhantomData<(K, V, M, T)>,
}

impl<K, V, M, T> Default for SimpleGossip<K, V, M, T> {
    fn default() -> Self {
        Self {
            config: SimpleGossipConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<K, V, M, T> SimpleGossip<K, V, M, T> {
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
}

impl<K, V, M, T> ReplicationStrategy<K, V> for SimpleGossip<K, V, M, T>
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
        + Merge<V>
        + std::hash::Hash
        + lattices::IsBot
        + lattices::LatticeFrom<V>,
    M: Default
        + Clone
        + std::fmt::Debug
        + FromIterator<(K, V)>
        + IntoIterator<Item = (K, V)>
        + lattices::cc_traits::Keyed<Key = K, Item = V>
        + Extend<(K, V)>
        + for<'a> lattices::cc_traits::GetMut<&'a K, Item = V>
        + for<'b> lattices::cc_traits::Remove<&'b K>
        + 'static,
    T: Default
        + Clone
        + std::fmt::Debug
        + FromIterator<K>
        + IntoIterator<Item = K>
        + lattices::tombstone::TombstoneSet<K>
        + 'static,
    for<'a> &'a M: IntoIterator<Item = (&'a K, &'a V)>,
{
    fn replicate_data<'a, O>(
        &self,
        cluster: &StaticCluster<'a, KVSNode>,
        local_data: Stream<(K, V), StaticCluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<
        (K, V),
        StaticCluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        self.handle_gossip_simple(cluster, local_data)
    }
}

impl<K, V, M, T> ClusterCommunication for SimpleGossip<K, V, M, T> {
    fn requires_cluster_scope() -> bool {
        true
    }
}

impl<K, V, M, T> SimpleGossip<K, V, M, T>
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
        + Merge<V>
        + std::hash::Hash
        + lattices::IsBot
        + lattices::LatticeFrom<V>,
    M: Default
        + Clone
        + std::fmt::Debug
        + FromIterator<(K, V)>
        + IntoIterator<Item = (K, V)>
        + lattices::cc_traits::Keyed<Key = K, Item = V>
        + Extend<(K, V)>
        + for<'a> lattices::cc_traits::GetMut<&'a K, Item = V>
        + for<'b> lattices::cc_traits::Remove<&'b K>
        + 'static,
    T: Default
        + Clone
        + std::fmt::Debug
        + FromIterator<K>
        + IntoIterator<Item = K>
        + lattices::tombstone::TombstoneSet<K>
        + 'static,
    for<'a> &'a M: IntoIterator<Item = (&'a K, &'a V)>,
{
    /// Epidemic gossip: Demers et al. with hot set and probabilistic tombstoning
    /// 
    /// Maintains hot updates that are re-gossiped on each operation. After gossiping,
    /// updates are probabilistically tombstoned via cyclic deletion.
    pub fn handle_gossip_simple<'a, O>(
        &self,
        cluster: &StaticCluster<'a, KVSNode>,
        local_put_tuples: Stream<(K, V), StaticCluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<
        (K, V),
        StaticCluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        // Convert probabilities to integer ratios for use in q!()
        let infection_numerator = (self.config.infection_prob * 1000.0).round() as u64;
        let infection_denominator = 1000u64;
        let tombstone_numerator = (self.config.tombstone_prob * 1000.0).round() as u64;
        let tombstone_denominator = 1000u64;
        
        // Create tick and cycle for hot set with tombstone-based deletes feeding back
        let gossip_tick = cluster.tick();
        let (set_hot_cycle, hot_cycle) =
            gossip_tick.cycle::<Stream<MapUnionWithTombstones<M, T>, _, _, NoOrder>>();

        // Batch inputs into the tick context - convert to PUT deltas
        let new_puts = local_put_tuples
            .clone()
            .map(q!(|(k, v)| {
                MapUnionWithTombstones::new_from(
                    std::iter::once((k, v)).collect::<M>(),
                    T::default(),
                )
            }))
            .batch_same_consistency(&gossip_tick, nondet!(/** new puts can arrive on any tick */));

        // Build hot set: new updates + tombstone deletes from previous tick
        let hot_set = new_puts.chain(hot_cycle).fold(
            q!(|| MapUnionWithTombstones::new(M::default(), T::default())),
            q!(|old, new| {
                lattices::Merge::merge(old, new);
            }, commutative = manual_proof!(/** lattice merge is commutative */), idempotent = manual_proof!(/** lattice merge is idempotent */)),
        );

        // Snapshot hot set for gossiping
        let hot_snapshot = hot_set;

        // Get all live (non-tombstoned) entries and decide whether to tombstone each
        let gossip_and_deletes = hot_snapshot
            .map(q!(|hot_map| {
                let (map, tombstones) = hot_map.as_reveal_ref();
                // Only iterate over keys that are NOT tombstoned
                map.into_iter()
                    .filter(|(k, _v)| !tombstones.contains(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            }))
            .flatten_unordered()
            .map(q!(move |(k, v)| {
                let should_tombstone =
                    (rand::random::<u64>() % tombstone_denominator) < tombstone_numerator;
                (k, v, should_tombstone)
            }));

        // Split into gossip stream and tombstone delete stream
        let to_gossip = gossip_and_deletes
            .clone()
            .filter(q!(|(_k, _v, should_delete)| !should_delete))
            .map(q!(|(k, v, _)| (k, v)));

        let to_delete = gossip_and_deletes
            .filter(q!(|(_k, _v, should_delete)| *should_delete))
            .map(q!(|(k, _v, _)| {
                // Create a tombstone delta: empty map, singleton tombstone set
                MapUnionWithTombstones::new_from(M::default(), std::iter::once(k).collect::<T>())
            }));

        // Complete the cycle - tombstone deletes feed back into next tick
        set_hot_cycle.complete_next_tick(to_delete);
        
        // Get all cluster members
        let all_members = cluster
            .source_cluster_members_static(cluster)
            .map_with_key(q!(|(member_id, _event)| member_id))
            .values();
        
        // Gossip to random peers
        to_gossip
            .all_ticks() // Move from Tick to Cluster location
            .cross_product(all_members)
            .filter(q!(move |((_key, _value), _peer)| {
                (rand::random::<u64>() % infection_denominator) < infection_numerator
            }))
            .map(q!(|((key, value), peer)| (peer, (key, value))))
            .into_keyed()
            .demux(cluster, TCP.fail_stop().bincode())
            .values()
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }
}

// Upward pass hook: Simple gossip doesn't modify responses by default
impl<K, V, M, T> AfterResponses for SimpleGossip<K, V, M, T> {
    fn after_responses<'a>(
        &self,
        _cluster: &StaticCluster<'a, KVSNode>,
        responses: Stream<String, StaticCluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, StaticCluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

/// Type aliases for common gossip configurations

/// Gossip with FST-compressed tombstones for String keys
pub type SimpleGossipFst<V> = SimpleGossip<
    String,
    V,
    HashMap<String, V>,
    lattices::tombstone::FstTombstoneSet<String>,
>;

/// Gossip with Roaring bitmap tombstones for u64 keys
pub type SimpleGossipRoaring<V> =
    SimpleGossip<u64, V, HashMap<u64, V>, lattices::tombstone::RoaringTombstoneSet>;

/// Gossip with HashSet tombstones for generic keys (default)
pub type SimpleGossipHashSet<K, V> =
    SimpleGossip<K, V, HashMap<K, V>, std::collections::HashSet<K>>;

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_epidemic_gossip_creation() {
        let _gossip = SimpleGossipHashSet::<String, String>::default();
        let _gossip_default = SimpleGossipHashSet::<String, String>::default();
    }

    #[test]
    fn test_epidemic_gossip_with_config() {
        let config = SimpleGossipConfig::small_cluster();
        let _gossip = SimpleGossipHashSet::<String, String>::new(config);
    }

    #[test]
    fn test_epidemic_gossip_config_presets() {
        let _small = SimpleGossipConfig::small_cluster();
        let _large = SimpleGossipConfig::large_cluster();
        let _default = SimpleGossipConfig::default();
    }

    #[test]
    fn test_epidemic_gossip_implements_replication_strategy() {
        fn _test_replication_strategy<K, V>(_strategy: impl ReplicationStrategy<K, V>) {}
        _test_replication_strategy::<String, crate::values::CausalString>(
            SimpleGossipHashSet::<String, crate::values::CausalString>::default(),
        );
    }

    #[test]
    fn test_gossip_type_aliases() {
        // Test FST variant for String keys
        let _fst = SimpleGossipFst::<crate::values::CausalString>::default();

        // Test Roaring variant for u64 keys
        let _roaring = SimpleGossipRoaring::<crate::values::CausalString>::default();

        // Test HashSet variant for generic keys
        let _hashset = SimpleGossipHashSet::<String, crate::values::CausalString>::default();
    }

    // Property-based test generators
    fn arb_gossip_fanout() -> impl Strategy<Value = usize> {
        1usize..=10
    }

    fn arb_probability() -> impl Strategy<Value = f64> {
        0.0..=1.0
    }

    fn arb_gossip_interval_ms() -> impl Strategy<Value = u64> {
        100u64..=5000
    }

    fn arb_gossip_config() -> impl Strategy<Value = SimpleGossipConfig> {
        (
            arb_gossip_fanout(),
            arb_probability(),
            arb_probability(),
            arb_gossip_interval_ms(),
        )
            .prop_map(|(fanout, tombstone_prob, infection_prob, interval_ms)| {
                SimpleGossipConfig {
                    gossip_fanout: fanout,
                    tombstone_prob,
                    infection_prob,
                    gossip_interval: std::time::Duration::from_millis(interval_ms),
                }
            })
    }

    mod property_tests {
        use super::*;

        proptest! {
            /// Gossip fanout must be positive
            #[test]
            fn prop_gossip_fanout_positive(config in arb_gossip_config()) {
                prop_assert!(config.gossip_fanout > 0, "Gossip fanout must be positive");
            }

            /// Tombstone probability must be in valid range [0, 1]
            #[test]
            fn prop_tombstone_probability_valid(config in arb_gossip_config()) {
                prop_assert!(config.tombstone_prob >= 0.0 && config.tombstone_prob <= 1.0,
                    "Tombstone probability must be in [0, 1]");
            }

            /// Infection probability must be in valid range [0, 1]
            #[test]
            fn prop_infection_probability_valid(config in arb_gossip_config()) {
                prop_assert!(config.infection_prob >= 0.0 && config.infection_prob <= 1.0,
                    "Infection probability must be in [0, 1]");
            }

            /// Gossip interval must be non-zero
            #[test]
            fn prop_gossip_interval_nonzero(config in arb_gossip_config()) {
                prop_assert!(!config.gossip_interval.is_zero(),
                    "Gossip interval must be non-zero");
            }

            /// Higher tombstone probability means faster hot set cleanup
            #[test]
            fn prop_tombstone_prob_affects_cleanup_rate(
                low_prob in 0.0..0.3f64,
                high_prob in 0.7..1.0f64,
            ) {
                let low_config = SimpleGossipConfig {
                    tombstone_prob: low_prob,
                    ..Default::default()
                };
                let high_config = SimpleGossipConfig {
                    tombstone_prob: high_prob,
                    ..Default::default()
                };

                // Higher tombstone probability should lead to faster cleanup
                prop_assert!(high_config.tombstone_prob > low_config.tombstone_prob);
                
                // Expected number of rounds before tombstoning is 1/p
                let low_expected_rounds = 1.0 / low_config.tombstone_prob;
                let high_expected_rounds = 1.0 / high_config.tombstone_prob;
                
                prop_assert!(high_expected_rounds < low_expected_rounds,
                    "Higher tombstone probability should result in fewer expected rounds");
            }

            /// Higher infection probability means more aggressive spreading
            #[test]
            fn prop_infection_prob_affects_spread_rate(
                low_prob in 0.0..0.3f64,
                high_prob in 0.7..1.0f64,
            ) {
                let low_config = SimpleGossipConfig {
                    infection_prob: low_prob,
                    ..Default::default()
                };
                let high_config = SimpleGossipConfig {
                    infection_prob: high_prob,
                    ..Default::default()
                };

                prop_assert!(high_config.infection_prob > low_config.infection_prob);
            }

            /// Config created from usize milliseconds should have correct interval
            #[test]
            fn prop_config_from_usize_sets_interval(ms in 100usize..5000) {
                let config: SimpleGossipConfig = ms.into();
                prop_assert_eq!(config.gossip_interval.as_millis(), ms as u128,
                    "Config interval should match input milliseconds");
            }

            /// Gossip strategy can be created with any valid config
            #[test]
            fn prop_gossip_creation_with_any_config(config in arb_gossip_config()) {
                let _gossip = SimpleGossipHashSet::<String, String>::new(config.clone());
                // If we get here without panic, the property holds
            }
        }
    }

    #[test]
    fn test_tombstone_probability_extremes() {
        // Zero tombstone probability means updates stay hot forever
        let never_tombstone = SimpleGossipConfig {
            tombstone_prob: 0.0,
            ..Default::default()
        };
        assert_eq!(never_tombstone.tombstone_prob, 0.0);

        // Probability of 1.0 means immediate tombstoning
        let always_tombstone = SimpleGossipConfig {
            tombstone_prob: 1.0,
            ..Default::default()
        };
        assert_eq!(always_tombstone.tombstone_prob, 1.0);
    }

    #[test]
    fn test_infection_probability_extremes() {
        // Zero infection means no spreading
        let no_infection = SimpleGossipConfig {
            infection_prob: 0.0,
            ..Default::default()
        };
        assert_eq!(no_infection.infection_prob, 0.0);

        // Probability of 1.0 means infect all peers
        let full_infection = SimpleGossipConfig {
            infection_prob: 1.0,
            ..Default::default()
        };
        assert_eq!(full_infection.infection_prob, 1.0);
    }

    #[test]
    fn test_small_cluster_more_aggressive_than_large() {
        let small = SimpleGossipConfig::small_cluster();
        let large = SimpleGossipConfig::large_cluster();

        // Small clusters can afford higher infection rates
        assert!(
            small.infection_prob >= large.infection_prob,
            "Small cluster should have higher or equal infection probability"
        );

        // Small clusters gossip more frequently
        assert!(
            small.gossip_interval <= large.gossip_interval,
            "Small cluster should have shorter or equal gossip interval"
        );
    }
}
