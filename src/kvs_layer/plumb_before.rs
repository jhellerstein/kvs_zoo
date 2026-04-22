use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;

use crate::before_storage::Before;

/// Trait to plumb routing across layers using per-layer Before components (routing/ordering).
pub trait KVSPlumb<K, V> {
    /// The ordering guarantee on the output stream.
    type OutputOrder: hydro_lang::live_collections::stream::Ordering;

    fn plumb_from_process<'a, O>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
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
            + std::fmt::Debug
            + Send
            + Sync
            + 'static;

    fn plumb_from_cluster<'a, O>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            StaticCluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
            O,
        >,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
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
            + std::fmt::Debug
            + Send
            + Sync
            + 'static;

    /// Like `plumb_from_process`, but preserves the Before layer's output ordering.
    /// Only meaningful when `OutputOrder` is `TotalOrder`.
    fn plumb_from_process_ordered<'a, O: hydro_lang::live_collections::stream::Ordering>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        Self::OutputOrder,
    >
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
            + std::fmt::Debug
            + Send
            + Sync
            + 'static;
}

impl<K, V> KVSPlumb<K, V> for () {
    type OutputOrder = hydro_lang::live_collections::stream::NoOrder;

    fn plumb_from_process_ordered<'a, O: hydro_lang::live_collections::stream::Ordering>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded, Self::OutputOrder>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static,
    {
        panic!("Terminal () cannot preserve ordering");
    }

    fn plumb_from_process<'a, O>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        panic!("Root layer cannot be terminal '()'; provide at least one KVSCluster.");
    }

    fn plumb_from_cluster<'a, O>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _source_cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            StaticCluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
            O,
        >,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        operations.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }
}

impl<K, V, Name, B, A, Child, Bg> KVSPlumb<K, V>
    for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: Before<K, V> + Clone,
    Child: KVSPlumb<K, V>,
{
    type OutputOrder = B::OutputOrder;

    fn plumb_from_process_ordered<'a, O: hydro_lang::live_collections::stream::Ordering>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded, Self::OutputOrder>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.before.dispatch_from_process_with_layers::<Name, _>(layers, operations, my_cluster)
    }

    fn plumb_from_process<'a, O>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
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
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let routed = self
            .before
            .dispatch_from_process_with_layers::<Name, _>(layers, operations, my_cluster);
        self.child.plumb_from_cluster(layers, my_cluster, routed)
    }

    fn plumb_from_cluster<'a, O>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            StaticCluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
            O,
        >,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
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
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let routed = self.before.dispatch_from_cluster_with_layers::<Name, O>(
            operations,
            source_cluster,
            my_cluster,
            layers,
        );
        self.child.plumb_from_cluster::<NoOrder>(layers, my_cluster, routed.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>())
    }
}

impl<K, V, Name, B, A> KVSPlumb<K, V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
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
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    B: Before<K, V> + Clone,
{
    type OutputOrder = B::OutputOrder;

    fn plumb_from_process_ordered<'a, O: hydro_lang::live_collections::stream::Ordering>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, StaticCluster<'a, crate::kvs_core::KVSNode>, Unbounded, Self::OutputOrder>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.before.dispatch_from_process_with_layers::<Name, _>(layers, operations, my_cluster)
    }

    fn plumb_from_process<'a, O>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded, O>,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.before
            .dispatch_from_process_with_layers::<Name, _>(layers, operations, my_cluster)
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }

    fn plumb_from_cluster<'a, O>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &StaticCluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            StaticCluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
            O,
        >,
    ) -> Stream<
        crate::protocol::KVSOperation<K, V>,
        StaticCluster<'a, crate::kvs_core::KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        self.before.dispatch_from_cluster_with_layers::<Name, O>(
            operations,
            source_cluster,
            source_cluster,
            _layers,
        ).weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
    }
}
