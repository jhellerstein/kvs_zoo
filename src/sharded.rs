//! Sharded KVS support and implementations

use crate::core::KVSNode;
use crate::values::LwwWrapper;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use lattices::Merge;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Type aliases to reduce complexity warnings
type ShardedGetResult<'a, V> =
    Stream<(String, Option<V>), Tick<Cluster<'a, KVSNode>>, Bounded, NoOrder>;

/// Calculate which shard a key should be assigned to using consistent hashing
pub fn calculate_shard_id(key: &str, num_shards: usize) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % num_shards as u64) as u32
}

/// Trait for KVS implementations that can be used in sharded systems
pub trait KVSShardable<V> {
    /// The internal storage type (might be wrapped, like `LwwWrapper<V>`)
    type StorageType: Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;

    /// Put key-value pairs and return the KVS state
    fn put<'a>(
        put_tuples: hydro_lang::prelude::Stream<
            (String, V),
            hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
            hydro_lang::prelude::Unbounded,
        >,
        cluster: &hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
    ) -> hydro_lang::prelude::KeyedSingleton<
        String,
        Self::StorageType,
        hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>,
        hydro_lang::prelude::Unbounded,
    >
    where
        V: Clone
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + 'static;

    /// Get values for keys from the KVS state
    fn get<'a>(
        get_keys: hydro_lang::prelude::Stream<
            String,
            hydro_lang::prelude::Tick<hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>>,
            hydro_lang::prelude::Bounded,
        >,
        kvs_state: hydro_lang::prelude::KeyedSingleton<
            String,
            Self::StorageType,
            hydro_lang::prelude::Tick<hydro_lang::prelude::Cluster<'a, crate::core::KVSNode>>,
            hydro_lang::prelude::Bounded,
        >,
    ) -> ShardedGetResult<'a, V>
    where
        V: Clone + std::fmt::Debug;
}

// =============================================================================
// ShardableKVS Implementations for All Storage Types
// =============================================================================

/// Implement KVSShardable trait for KVSCore
///
/// This allows KVSCore to work with the unified KVSServer API by providing
/// the required put/get interface with proper type signatures.
impl<V> KVSShardable<V> for crate::core::KVSCore
where
    V: Clone
        + Default
        + PartialEq
        + Eq
        + lattices::Merge<V>
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    type StorageType = V;

    /// Delegate to KVSCore::put with cluster location
    fn put<'a>(
        put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        _cluster: &Cluster<'a, KVSNode>,
    ) -> KeyedSingleton<String, Self::StorageType, Cluster<'a, KVSNode>, Unbounded> {
        crate::core::KVSCore::put(put_tuples)
    }

    /// Delegate to KVSCore::get with cluster location
    fn get<'a>(
        get_keys: Stream<String, Tick<Cluster<'a, KVSNode>>, Bounded>,
        kvs_state: KeyedSingleton<String, Self::StorageType, Tick<Cluster<'a, KVSNode>>, Bounded>,
    ) -> ShardedGetResult<'a, V> {
        crate::core::KVSCore::get(get_keys, kvs_state)
    }
}

/// Implement KVSShardable trait for KVSLww
///
/// This allows KVSLww to work with the unified KVSServer API by providing
/// the required put/get interface with proper type signatures.
impl<V> KVSShardable<V> for crate::lww::KVSLww
where
    V: Clone
        + Default
        + PartialEq
        + Eq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    type StorageType = LwwWrapper<V>;

    /// Delegate to KVSLww::put (cluster parameter is not used)
    fn put<'a>(
        put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        _cluster: &Cluster<'a, KVSNode>,
    ) -> KeyedSingleton<String, Self::StorageType, Cluster<'a, KVSNode>, Unbounded> {
        crate::lww::KVSLww::put(put_tuples)
    }

    /// Delegate to KVSLww::get with cluster location
    fn get<'a>(
        get_keys: Stream<String, Tick<Cluster<'a, KVSNode>>, Bounded>,
        kvs_state: KeyedSingleton<String, Self::StorageType, Tick<Cluster<'a, KVSNode>>, Bounded>,
    ) -> ShardedGetResult<'a, V> {
        crate::lww::KVSLww::get(get_keys, kvs_state)
    }
}

/// Implement KVSShardable trait for KVSReplicated (same pattern as KVSLww)
///
/// This allows KVSReplicated to work with the unified KVSServer API
impl<V, R> KVSShardable<V> for crate::replicated::KVSReplicated<R>
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
    R: crate::maintain::ReplicationStrategy<V> + Default,
{
    type StorageType = V;

    /// Delegate to ReplicatedKVS::put (same pattern as LwwKVS)
    fn put<'a>(
        put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> KeyedSingleton<String, Self::StorageType, Cluster<'a, KVSNode>, Unbounded> {
        crate::replicated::KVSReplicated::<R>::put(put_tuples, cluster)
    }

    /// Delegate to ReplicatedKVS::get (same pattern as LwwKVS)
    fn get<'a>(
        get_keys: Stream<String, Tick<Cluster<'a, KVSNode>>, Bounded>,
        kvs_state: KeyedSingleton<String, Self::StorageType, Tick<Cluster<'a, KVSNode>>, Bounded>,
    ) -> ShardedGetResult<'a, V> {
        crate::replicated::KVSReplicated::<R>::get(get_keys, kvs_state)
    }
}

/// Sharded KVS implementation (follows same pattern as KVSLww and KVSReplicated)
///
/// This is a basic sharded storage that:
/// - Delegates put/get operations to KVSCore (same as KVSLww)
/// - Works with ShardedRouter for hash-based key routing
/// - No background replication (unlike KVSReplicated)
pub struct KVSSharded;

impl KVSSharded {
    /// Insert operations (delegates to KVSCore, same pattern as LwwKVS)
    pub fn put<'a, V>(
        put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
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
    {
        // Same delegation pattern as LwwKVS and ReplicatedKVS
        crate::core::KVSCore::put(put_tuples)
    }

    /// Query operations (delegates to KVSCore, same pattern as LwwKVS)
    pub fn get<'a, V>(
        keys: Stream<String, Tick<Cluster<'a, KVSNode>>, Bounded>,
        ht: KeyedSingleton<String, V, Tick<Cluster<'a, KVSNode>>, Bounded>,
    ) -> ShardedGetResult<'a, V>
    where
        V: Clone + std::fmt::Debug,
    {
        // Same delegation pattern as LwwKVS and ReplicatedKVS
        crate::core::KVSCore::get(keys, ht)
    }
}

/// Implement KVSShardable trait for KVSSharded (same pattern as KVSLww)
///
/// This allows KVSSharded to work with the unified KVSServer API
impl<V> KVSShardable<V> for KVSSharded
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
{
    type StorageType = V;

    /// Delegate to ShardedKVS::put (same pattern as LwwKVS)
    fn put<'a>(
        put_tuples: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
        _cluster: &Cluster<'a, KVSNode>,
    ) -> KeyedSingleton<String, Self::StorageType, Cluster<'a, KVSNode>, Unbounded> {
        Self::put(put_tuples)
    }

    /// Delegate to ShardedKVS::get (same pattern as LwwKVS)
    fn get<'a>(
        get_keys: Stream<String, Tick<Cluster<'a, KVSNode>>, Bounded>,
        kvs_state: KeyedSingleton<String, Self::StorageType, Tick<Cluster<'a, KVSNode>>, Bounded>,
    ) -> ShardedGetResult<'a, V> {
        Self::get(get_keys, kvs_state)
    }
}
