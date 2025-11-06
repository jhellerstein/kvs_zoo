//! Sharded Router Operation Interceptor
//!
//! The ShardedRouter partitions operations by key hash, distributing them
//! across cluster nodes for horizontal scaling. Each key is consistently
//! routed to the same shard based on its hash value.

use crate::core::KVSNode;
use crate::interception::OpIntercept;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Router that partitions operations by key hash
///
/// ShardedRouter uses consistent hashing to distribute operations across
/// cluster nodes based on the operation's key. This enables horizontal
/// scaling by partitioning the keyspace and is suitable for:
/// - Large-scale systems requiring horizontal partitioning
/// - Systems where data locality is important
/// - Distributed caching scenarios
///
/// ## Usage
///
/// ```rust
/// use kvs_zoo::interception::routing::ShardedRouter;
/// use kvs_zoo::interception::OpIntercept;
///
/// let router = ShardedRouter::new(3);
/// // Operations will be routed to shards based on key hash
/// ```
///
/// ## Behavior
///
/// The router calculates a shard ID for each operation based on its key hash,
/// then routes the operation to the corresponding node. This ensures that
/// operations for the same key always go to the same shard, enabling
/// consistent data partitioning.
#[derive(Clone, Debug)]
pub struct ShardedRouter {
    shard_count: usize,
}

impl ShardedRouter {
    /// Create a new sharded router with the specified number of shards
    ///
    /// The shard count determines how many partitions the keyspace will be
    /// divided into. Each shard corresponds to a node in the cluster.
    pub fn new(shard_count: usize) -> Self {
        Self { shard_count }
    }

    /// Get the number of shards this router manages
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Calculate shard ID for a key using consistent hashing
    ///
    /// This function provides deterministic key-to-shard mapping using a hash function.
    /// All nodes will map the same key to the same shard ID, ensuring consistency.
    pub fn calculate_shard_id(key: &str, shard_count: usize) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % shard_count as u64) as u32
    }
}

impl<V> OpIntercept<V> for ShardedRouter
where
    V: std::fmt::Debug,
{
    fn intercept_operations<'a>(
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
                // Calculate shard ID using consistent hashing
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::hash::Hash::hash(key, &mut hasher);
                let shard_id = (std::hash::Hasher::finish(&hasher) % shard_count as u64) as u32;
                (hydro_lang::location::MemberId::from_raw(shard_id), op)
            }));

        ops_to_be_sharded.into_keyed().demux_bincode(cluster)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interception::OpInterceptExt;

    #[test]
    fn test_sharded_router_creation() {
        let router = ShardedRouter::new(3);
        assert_eq!(router.shard_count(), 3);

        let router5 = ShardedRouter::new(5);
        assert_eq!(router5.shard_count(), 5);
    }

    #[test]
    fn test_sharded_router_implements_interception() {
        let router = ShardedRouter::new(3);

        // This should compile, demonstrating that ShardedRouter implements OpIntercept
        fn _test_interception<V>(_interceptor: impl OpIntercept<V>) {}
        _test_interception::<String>(router);
    }

    #[test]
    fn test_sharded_router_implements_interception_ext() {
        let router = ShardedRouter::new(3);

        // Test that ShardedRouter implements OpInterceptExt for chaining
        fn _test_interception_ext<V>(_interceptor: impl OpInterceptExt<V>) {}
        _test_interception_ext::<String>(router);
    }

    #[test]
    fn test_sharded_router_clone_debug() {
        let router = ShardedRouter::new(3);
        let _cloned = router.clone();
        let _debug_str = format!("{:?}", router);
    }

    #[test]
    fn test_sharded_router_shard_count() {
        let router1 = ShardedRouter::new(1);
        assert_eq!(router1.shard_count(), 1);

        let router10 = ShardedRouter::new(10);
        assert_eq!(router10.shard_count(), 10);

        let router100 = ShardedRouter::new(100);
        assert_eq!(router100.shard_count(), 100);
    }

    #[test]
    fn test_calculate_shard_id_consistency() {
        // Test that the same key always maps to the same shard
        let key = "test_key";
        let shard_count = 5;

        let shard1 = ShardedRouter::calculate_shard_id(key, shard_count);
        let shard2 = ShardedRouter::calculate_shard_id(key, shard_count);
        let shard3 = ShardedRouter::calculate_shard_id(key, shard_count);

        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
        assert!(shard1 < shard_count as u32);
    }

    #[test]
    fn test_calculate_shard_id_distribution() {
        // Test that different keys map to different shards (most of the time)
        let shard_count = 5;
        let keys = [
            "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8",
        ];

        let mut shards = Vec::new();
        for key in &keys {
            let shard = ShardedRouter::calculate_shard_id(key, shard_count);
            assert!(shard < shard_count as u32);
            shards.push(shard);
        }

        // With 8 keys and 5 shards, we should see some distribution
        // (not all keys should map to the same shard)
        let unique_shards: std::collections::HashSet<_> = shards.into_iter().collect();
        assert!(
            unique_shards.len() > 1,
            "Keys should distribute across multiple shards"
        );
    }

    #[test]
    fn test_calculate_shard_id_bounds() {
        // Test various shard counts
        let key = "test_key";

        for shard_count in 1..=10 {
            let shard = ShardedRouter::calculate_shard_id(key, shard_count);
            assert!(
                shard < shard_count as u32,
                "Shard ID should be within bounds for shard_count {}",
                shard_count
            );
        }
    }
}
