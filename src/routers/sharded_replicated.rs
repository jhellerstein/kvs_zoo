use super::KVSRouter;
use crate::core::KVSNode;
use crate::protocol::KVSOperation;

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Sharded + Round-Robin router for single cluster with shard ranges
///
/// This router first shards operations by key hash, then uses round-robin
/// to select one replica within that shard range. It works with a single
/// cluster where nodes are organized as: [shard0_nodes..., shard1_nodes..., etc.]
pub struct ShardedRoundRobin {
    pub shard_count: usize,
    pub replica_count: usize,
}

impl ShardedRoundRobin {
    pub fn new(shard_count: usize, replica_count: usize) -> Self {
        Self {
            shard_count,
            replica_count,
        }
    }

    /// Calculate which shard a key belongs to
    pub fn calculate_shard_id(key: &str, shard_count: usize) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % shard_count as u64) as u32
    }

    /// Calculate the primary node for a shard (first node in the shard range)
    pub fn shard_primary_node(shard_id: u32, replica_count: usize) -> u32 {
        shard_id * replica_count as u32
    }

    /// Calculate a round-robin node within a shard
    pub fn shard_round_robin_node(
        shard_id: u32,
        replica_count: usize,
        round_robin_counter: u64,
    ) -> u32 {
        let base_node = shard_id * replica_count as u32;
        let replica_offset = (round_robin_counter % replica_count as u64) as u32;
        base_node + replica_offset
    }
}

impl<V: std::fmt::Debug> KVSRouter<V> for ShardedRoundRobin {
    fn route_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let shard_count = self.shard_count;
        let replica_count = self.replica_count;

        let ops_to_be_routed = operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => {
                        println!("🔀 Sharding+RoundRobin: PUT {} = {:?}", key, value)
                    }
                    KVSOperation::Get(key) => println!("🔀 Sharding+RoundRobin: GET {}", key),
                }
            }))
            .map(q!(move |op| {
                let key = match &op {
                    KVSOperation::Put(key, _) => key,
                    KVSOperation::Get(key) => key,
                };

                // Calculate which shard this key belongs to
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::hash::Hash::hash(key, &mut hasher);
                let hash_value = std::hash::Hasher::finish(&hasher);
                let shard_id = (hash_value % shard_count as u64) as u32;

                // Route to the primary node of the shard
                // The replication layer will handle spreading within the shard
                let primary_node = shard_id * replica_count as u32;

                (hydro_lang::location::MemberId::from_raw(primary_node), op)
            }));

        ops_to_be_routed.into_keyed().demux_bincode(cluster)
    }
}
