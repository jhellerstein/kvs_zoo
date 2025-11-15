use hydro_lang::prelude::*;

use crate::events::{DataEvent, MetaEvent};

pub mod tomb_index;
pub use tomb_index::{TombIndexBackground, TombIndexStats};

/// Trait implemented by background stages that wish to consume data/meta events.
pub trait MetaBackground<V> {
    fn attach<'a>(
        &mut self,
        cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    );
}

impl<V> MetaBackground<V> for () {
    fn attach<'a>(
        &mut self,
        _cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) {
        (data, meta)
    }
}

/// Trait that wires background stages for each KVS layer.
pub trait BackgroundPlumb<V> {
    fn plumb_background<'a>(
        &mut self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    )
    where
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

impl<V> BackgroundPlumb<V> for () {
    fn plumb_background<'a>(
        &mut self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    )
    where
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

impl<V, Name, B, A, Child, Bg> BackgroundPlumb<V>
    for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    Child: BackgroundPlumb<V>,
    Bg: MetaBackground<V>,
{
    fn plumb_background<'a>(
        &mut self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    )
    where
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

impl<V, Name, B, A> BackgroundPlumb<V> for crate::kvs_layer::KVSNode<Name, B, A> {
    fn plumb_background<'a>(
        &mut self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    )
    where
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
