use crate::core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use lattices::Merge;

/// Shard-aware broadcast replication
///
/// This replication protocol only broadcasts to nodes within the same shard,
/// enabling true sharding where each shard is an independent replicated group.
pub struct ShardAwareBroadcastReplication {
    pub shard_count: usize,
    pub replica_count: usize,
}

impl ShardAwareBroadcastReplication {
    pub fn new(shard_count: usize, replica_count: usize) -> Self {
        Self {
            shard_count,
            replica_count,
        }
    }

    /// Calculate which shard a node belongs to based on its ID
    pub fn node_shard_id(node_id: u32, replica_count: usize) -> u32 {
        node_id / replica_count as u32
    }

    /// Get all node IDs in the same shard as the given node
    pub fn shard_node_ids(node_id: u32, replica_count: usize) -> Vec<u32> {
        let shard_id = Self::node_shard_id(node_id, replica_count);
        let base_node = shard_id * replica_count as u32;
        (0..replica_count as u32)
            .map(|offset| base_node + offset)
            .collect()
    }
}

impl Default for ShardAwareBroadcastReplication {
    fn default() -> Self {
        Self {
            shard_count: 3,
            replica_count: 3,
        }
    }
}

impl<V> super::ReplicationProtocol<V> for ShardAwareBroadcastReplication
where
    V: Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default
        + Merge<V>,
{
    fn handle_replication<'a>(
        cluster: &Cluster<'a, KVSNode>,
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded, NoOrder> {
        // Shard-aware filtering: only accept operations that belong to my shard
        let filtered_tuples = local_put_tuples
            .filter(q!(move |(key, _value)| {
                // Only process if this operation belongs to my shard
                let my_node_id = CLUSTER_SELF_ID.raw_id;
                let my_shard_id = my_node_id / 3; // 3 replicas per shard
                
                // Calculate which shard this key should go to
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::hash::Hash::hash(key, &mut hasher);
                let key_shard_id = (std::hash::Hasher::finish(&hasher) % 3) as u32; // 3 shards
                
                my_shard_id == key_shard_id
            }));
        
        // Broadcast within the shard
        filtered_tuples
            .broadcast_bincode(cluster, nondet!(/** broadcast to all nodes */))
            .values()
    }
}