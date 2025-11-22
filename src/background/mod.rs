use hydro_lang::prelude::*;

use crate::kvs_core::events::{DataEvent, MetaEvent};

pub type BackgroundDataStream<'a, K, V> =
    Stream<DataEvent<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>;
pub type BackgroundMetaStream<'a, K> =
    Stream<MetaEvent<K>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>;



/// Trait implemented by background stages that wish to consume data/meta events.
pub trait MetaBackground<K, V> {
    fn attach<'a>(
        &mut self,
        cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        data: BackgroundDataStream<'a, K, V>,
        meta: BackgroundMetaStream<'a, K>,
    ) -> (BackgroundDataStream<'a, K, V>, BackgroundMetaStream<'a, K>);
}

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
