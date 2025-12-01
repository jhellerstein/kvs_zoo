use hydro_lang::prelude::*;
use lattices::set_union::SetUnionHashSet;

use crate::kvs_core::events::{DataEvent, MetaEvent};

/// Lattice wrapper for metadata events using set-union semantics
///
/// Wraps MetaEvent in a SetUnion lattice so metadata can be composed
/// monotonically across the distributed system.
pub type MetaLattice<K> = SetUnionHashSet<MetaEvent<K>>;

use hydro_lang::live_collections::stream::NoOrder;

pub type BackgroundDataStream<'a, K, V> =
    Stream<DataEvent<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded, NoOrder>;
pub type BackgroundMetaStream<'a, K> =
    Stream<MetaLattice<K>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded, NoOrder>;

/// Trait implemented by background stages that wish to consume data/meta events.
///
/// Background stages can:
/// - Index tombstone metadata for compaction
/// - Track statistics and emit summaries
/// - Implement anti-entropy protocols
/// - Perform background maintenance tasks
///
/// Example implementation (future):
/// ```ignore
/// struct TombIndexBackground {
///     log_snapshots: bool,
///     emit_summaries: bool,
/// }
///
/// impl<K, V> MetaBackground<K, V> for TombIndexBackground {
///     fn attach(...) -> (...) {
///         // Transform meta stream to add summaries/digests
///     }
/// }
/// ```
pub trait MetaBackground<K, V> {
    fn attach<'a>(
        &mut self,
        cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>);
}

/// Placeholder implementation: no background processing.
///
/// The unit type `()` is used when no background stages are needed.
/// It simply passes through data and meta streams unchanged.
///
/// To implement actual background processing, create a struct that implements
/// `MetaBackground<K, V>` and use it as the `Bg` type parameter in `KVSCluster`.
impl<K, V> MetaBackground<K, V> for () {
    fn attach<'a>(
        &mut self,
        _cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>) {
        (data, meta)
    }
}

/// Trait that wires background stages for each KVS layer.
///
/// This trait walks the KVS layer tree and gives each layer's background stage
/// a chance to attach to and transform the data/meta event streams.
///
/// Concrete background stages should implement `MetaBackground<K, V>` rather than
/// this trait directly. This trait is automatically implemented for `KVSCluster`
/// and coordinates the attachment of all background stages in the hierarchy.
pub trait BackgroundPlumb<K, V> {
    fn plumb_background<'a>(
        &mut self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>)
    where
        K: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + std::hash::Hash
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static;
}

/// Placeholder implementation: no background processing at this layer.
///
/// Used for terminal/leaf nodes in the KVS layer hierarchy or when
/// no background processing is needed. Simply returns streams unchanged.
impl<K, V> BackgroundPlumb<K, V> for () {
    fn plumb_background<'a>(
        &mut self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>)
    where
        K: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + std::hash::Hash
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        (data, meta)
    }
}

impl<K, V, Name, B, A, Child, Bg> BackgroundPlumb<K, V>
    for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    Child: BackgroundPlumb<K, V>,
    Bg: MetaBackground<K, V>,
{
    fn plumb_background<'a>(
        &mut self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>)
    where
        K: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + std::hash::Hash
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let (data, meta) = self.child.plumb_background(layers, data, meta);
        self.background.attach(my_cluster, data, meta)
    }
}

impl<K, V, Name, B, A> BackgroundPlumb<K, V> for crate::kvs_layer::KVSNode<Name, B, A> {
    fn plumb_background<'a>(
        &mut self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>)
    where
        K: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + std::hash::Hash
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
    {
        (data, meta)
    }
}
