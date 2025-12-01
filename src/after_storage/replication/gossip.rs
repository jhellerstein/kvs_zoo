//! Simple Gossip Replication Strategy (after-storage)

use crate::after_storage::{AfterResponses, ClusterCommunication, ReplicationStrategy};
use crate::kvs_core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use lattices::Merge;
use serde::{Deserialize, Serialize};

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

/// Simple gossip replication
#[derive(Clone, Debug)]
pub struct SimpleGossip<K, V> {
    #[allow(dead_code)]
    config: SimpleGossipConfig,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Default for SimpleGossip<K, V> {
    fn default() -> Self {
        Self {
            config: SimpleGossipConfig::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<K, V> SimpleGossip<K, V> {
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

impl<K, V> ReplicationStrategy<K, V> for SimpleGossip<K, V>
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
        + std::hash::Hash,
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
        self.handle_gossip_simple(cluster, local_data)
    }
}

impl<K, V> ClusterCommunication for SimpleGossip<K, V> {
    fn requires_cluster_scope() -> bool {
        true
    }
}

impl<K, V> SimpleGossip<K, V>
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
        + std::hash::Hash,
{
    /// Simplified gossip that immediately forwards PUT operations to all peers
    pub fn handle_gossip_simple<'a, O>(
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
        let cluster_members = Self::get_cluster_members(cluster);

        // Immediate forwarding to all peers for reliable convergence
        let gossip_sent = local_put_tuples
            .clone()
            .cross_product(
                cluster_members
                    .clone()
                    .assume_retries(nondet!(/** member list OK */)),
            )
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        gossip_sent
            .values()
            .weakest_ordering()
            .assume_retries(nondet!(/** gossip retries OK */))
    }
}

// Upward pass hook: Simple gossip doesn't modify responses by default
impl<K, V> AfterResponses for SimpleGossip<K, V> {
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
    fn test_epidemic_gossip_creation() {
        let _gossip = SimpleGossip::<String, String>::default();
        let _gossip_default = SimpleGossip::<String, String>::default();
    }

    #[test]
    fn test_epidemic_gossip_with_config() {
        let config = SimpleGossipConfig::small_cluster();
        let _gossip = SimpleGossip::<String, String>::new(config);
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
        _test_replication_strategy::<String, crate::values::CausalString>(SimpleGossip::<
            String,
            crate::values::CausalString,
        >::default());
    }
}
