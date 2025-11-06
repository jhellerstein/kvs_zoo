use crate::core::KVSNode;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use lattices::Merge;

// Type aliases to reduce complexity warnings
type ReplicatedGetResult<'a, V> =
    Stream<(String, Option<V>), Tick<Cluster<'a, KVSNode>>, Bounded, NoOrder>;

/// Replicated KVS implementation with pluggable replication strategies
///
/// This follows the same pattern as KVSLww:
/// - Core put/get methods that delegate to KVSCore
/// - Additional replication logic for distributed consistency
/// - KVSShardable trait implementation for unified API compatibility
pub struct KVSReplicated<R> {
    _phantom: std::marker::PhantomData<R>,
}

impl<R> KVSReplicated<R> {
    /// Insert with replication using the configured replication strategy
    ///
    /// This delegates to KVSCore but adds replication logic:
    /// 1. Local operations are replicated to other nodes via strategy R
    /// 2. Remote operations are received from other nodes
    /// 3. All operations (local + remote) are merged using lattice semantics
    pub fn put<'a, V>(
        local_put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> KeyedSingleton<String, V, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone
            + Default
            + PartialEq
            + Eq
            + Merge<V>
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + 'static,
        R: crate::replication::ReplicationStrategy<V> + Default,
    {
        // Use replication strategy to get remote operations
        let replication_strategy = R::default();
        let remote_put_tuples = replication_strategy.replicate_data(cluster, local_put_tuples.clone());

        // Combine local and remote operations
        let all_put_tuples = local_put_tuples.interleave(remote_put_tuples);

        // Delegate to KVSCore for actual storage (same as LwwKVS pattern)
        crate::core::KVSCore::put(all_put_tuples)
    }

    /// Query operations from the KVS (delegates to KVSCore, same as LwwKVS)
    pub fn get<'a, V>(
        keys: Stream<String, Tick<Cluster<'a, KVSNode>>, Bounded>,
        ht: KeyedSingleton<String, V, Tick<Cluster<'a, KVSNode>>, Bounded>,
    ) -> ReplicatedGetResult<'a, V>
    where
        V: Clone + std::fmt::Debug,
    {
        // Same delegation pattern as LwwKVS
        crate::core::KVSCore::get(keys, ht)
    }
}
