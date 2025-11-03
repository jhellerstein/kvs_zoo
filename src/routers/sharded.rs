use super::KVSRouter;
use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Sharded routing - hash-based routing to specific shards
///
/// This router implements consistent hashing to distribute operations
/// across multiple shards based on the key hash.
pub struct ShardedRouter {
    pub shard_count: usize,
}

impl ShardedRouter {
    pub fn new(shard_count: usize) -> Self {
        Self { shard_count }
    }

    /// Calculate shard ID for a key using consistent hashing
    ///
    /// This function provides deterministic key-to-shard mapping using a hash function.
    /// All nodes will map the same key to the same shard ID.
    pub fn calculate_shard_id(key: &str, shard_count: usize) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % shard_count as u64) as u32
    }
}

impl<V: std::fmt::Debug> KVSRouter<V> for ShardedRouter {
    fn route_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let shard_count = self.shard_count;
        let ops_to_be_sharded = operations
            .inspect(q!(|op| {
                match op {
                    KVSOperation::Put(key, value) => {
                        println!("🔀 Sharding: PUT {} = {:?}", key, value)
                    }
                    KVSOperation::Get(key) => println!("🔀 Sharding: GET {}", key),
                }
            }))
            .map(q!(move |op| {
                let key = match &op {
                    KVSOperation::Put(key, _) => key,
                    KVSOperation::Get(key) => key,
                };
                // Use the same hash calculation as the utility function
                let shard_id = Self::calculate_shard_id(key, shard_count);
                (MemberId::from_raw(shard_id), op)
            }));

        ops_to_be_sharded.into_keyed().demux_bincode(cluster)
    }
}
