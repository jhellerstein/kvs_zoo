//! Simple Gossip Replication Strategy (after-storage, native)
//!
//! Native after_storage implementation ported from legacy maintenance::cluster::gossip.

use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy};
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
        SimpleGossipConfig { gossip_interval: std::time::Duration::from_millis(ms as u64), ..Default::default() }
    }
}

/// Simple gossip replication
#[derive(Clone, Debug)]
pub struct SimpleGossip<V> {
    #[allow(dead_code)]
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
        Self { config: config.into(), _phantom: std::marker::PhantomData }
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
        // Use simplified gossip - immediate forwarding
        self.handle_gossip_simple(cluster, local_data)
    }

    fn replicate_slotted_data<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded> {
        // Forward slotted tuples to all peers preserving the slot
        let cluster_members = Self::get_cluster_members(cluster)
            .assume_retries(nondet!(/** member list OK */));

        let sent = local_slotted_data
            .clone()
            .cross_product(cluster_members)
            .map(q!(|((slot, tuple_k, tuple_v), member_id)| (member_id, (slot, tuple_k, tuple_v))))
            .into_keyed()
            .demux_bincode(cluster);

        sent
            .values()
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
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
            .map(q!(|(tuple, member_id)| (member_id, tuple)))
            .into_keyed()
            .demux_bincode(cluster);

        gossip_sent
            .values()
            .assume_ordering(nondet!(/** gossip messages unordered */))
            .assume_retries(nondet!(/** gossip retries OK */))
    }
}

// Upward pass hook: Simple gossip doesn't modify responses by default
impl<V> MaintenanceAfterResponses for SimpleGossip<V> {
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
    fn test_epidemic_gossip_implements_replication_strategy() {
        fn _test_replication_strategy<V>(_strategy: impl ReplicationStrategy<V>) {}
        _test_replication_strategy::<crate::values::CausalString>(SimpleGossip::<
            crate::values::CausalString,
        >::default());
    }
}
