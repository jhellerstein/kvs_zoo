//! Sharded Router Operation dispatcher

use crate::before_storage::OpDispatch;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use crate::protocol::routing::RoutingKey;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub struct ShardedRouter {
    shard_count: usize,
}

impl ShardedRouter {
    pub fn new(shard_count: usize) -> Self {
        Self { shard_count }
    }
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }
    pub fn calculate_shard_id(key: &str, shard_count: usize) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % shard_count as u64) as u32
    }

    pub fn calculate_shard_id_bytes(key_bytes: &[u8], shard_count: usize) -> u32 {
        let mut hasher = DefaultHasher::new();
        key_bytes.hash(&mut hasher);
        (hasher.finish() % shard_count as u64) as u32
    }
}

impl<V> OpDispatch<V> for ShardedRouter
where
    V: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Default + 'static,
{
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let shard_count = self.shard_count;
        operations
            .map(q!(move |op| {
                let shard_id =
                    ShardedRouter::calculate_shard_id_bytes(op.routing_key(), shard_count);
                (hydro_lang::location::MemberId::from_raw(shard_id), op)
            }))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let shard_count = self.shard_count;
        operations
            .map(q!(move |op| {
                let shard_id =
                    ShardedRouter::calculate_shard_id_bytes(op.routing_key(), shard_count);
                (hydro_lang::location::MemberId::from_raw(shard_id), op)
            }))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
            .assume_ordering(nondet!(/** cluster hop sharded */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharded_router_creation() {
        let router = ShardedRouter::new(3);
        assert_eq!(router.shard_count(), 3);
    }
}
