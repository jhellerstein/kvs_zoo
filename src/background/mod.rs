use hydro_lang::prelude::*;
use lattices::set_union::SetUnionHashSet;

use crate::kvs_core::events::{DataEvent, MetaEvent};

/// Lattice wrapper for metadata events using set-union semantics
///
/// Wraps MetaEvent in a SetUnion lattice so metadata can be composed
/// monotonically across the distributed system.
pub type MetaLattice<K> = SetUnionHashSet<MetaEvent<K>>;

use hydro_lang::live_collections::stream::NoOrder;

pub type BackgroundDataStream<'a, K, V, Con> =
    Stream<DataEvent<K, V>, StaticCluster<'a, crate::kvs_core::KVSNode, Con>, Unbounded, NoOrder>;
pub type BackgroundMetaStream<'a, K, Con> =
    Stream<MetaLattice<K>, StaticCluster<'a, crate::kvs_core::KVSNode, Con>, Unbounded, NoOrder>;

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
pub trait MetaBackground<K, V, Con: hydro_lang::location::cluster::Consistency = hydro_lang::location::cluster::Deterministic> {
    fn attach<'a>(
        &mut self,
        cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
        data: BackgroundDataStream<'a, K, V, Con>,
        meta: BackgroundMetaStream<'a, K, Con>,
    ) -> (BackgroundDataStream<'a, K, V, Con>, BackgroundMetaStream<'a, K, Con>);
}

/// Placeholder implementation: no background processing.
///
/// The unit type `()` is used when no background stages are needed.
/// It simply passes through data and meta streams unchanged.
///
/// To implement actual background processing, create a struct that implements
/// `MetaBackground<K, V>` and use it as the `Bg` type parameter in `KVSCluster`.
impl<K, V, Con: hydro_lang::location::cluster::Consistency> MetaBackground<K, V, Con> for () {
    fn attach<'a>(
        &mut self,
        _cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
        data: BackgroundDataStream<'a, K, V, Con>,
        meta: BackgroundMetaStream<'a, K, Con>,
    ) -> (BackgroundDataStream<'a, K, V, Con>, BackgroundMetaStream<'a, K, Con>) {
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
pub trait BackgroundPlumb<K, V, Con: hydro_lang::location::cluster::Consistency = hydro_lang::location::cluster::Deterministic> {
    fn plumb_background<'a>(
        &mut self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V, Con>,
        meta: BackgroundMetaStream<'a, K, Con>,
    ) -> (BackgroundDataStream<'a, K, V, Con>, BackgroundMetaStream<'a, K, Con>)
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
            + Send
            + Sync
            + 'static;
}

/// Placeholder implementation: no background processing at this layer.
///
/// Used for terminal/leaf nodes in the KVS layer hierarchy or when
/// no background processing is needed. Simply returns streams unchanged.
impl<K, V, Con: hydro_lang::location::cluster::Consistency> BackgroundPlumb<K, V, Con> for () {
    fn plumb_background<'a>(
        &mut self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V, Con>,
        meta: BackgroundMetaStream<'a, K, Con>,
    ) -> (BackgroundDataStream<'a, K, V, Con>, BackgroundMetaStream<'a, K, Con>)
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
            + Send
            + Sync
            + 'static,
    {
        (data, meta)
    }
}

impl<K, V, Con: hydro_lang::location::cluster::Consistency, Name, B, A, Child, Bg> BackgroundPlumb<K, V, Con>
    for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    Child: BackgroundPlumb<K, V, Con>,
    Bg: MetaBackground<K, V, Con>,
{
    fn plumb_background<'a>(
        &mut self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V, Con>,
        meta: BackgroundMetaStream<'a, K, Con>,
    ) -> (BackgroundDataStream<'a, K, V, Con>, BackgroundMetaStream<'a, K, Con>)
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
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let (data, meta) = self.child.plumb_background(layers, data, meta);
        self.background.attach(my_cluster, data, meta)
    }
}

impl<K, V, Con: hydro_lang::location::cluster::Consistency, Name, B, A> BackgroundPlumb<K, V, Con> for crate::kvs_layer::KVSNode<Name, B, A> {
    fn plumb_background<'a>(
        &mut self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        data: BackgroundDataStream<'a, K, V, Con>,
        meta: BackgroundMetaStream<'a, K, Con>,
    ) -> (BackgroundDataStream<'a, K, V, Con>, BackgroundMetaStream<'a, K, Con>)
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
            + Send
            + Sync
            + 'static,
    {
        (data, meta)
    }
}
