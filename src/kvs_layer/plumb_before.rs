use hydro_lang::prelude::*;

use crate::before_storage::Before;

/// Trait to plumb routing across layers using per-layer Before components (routing/ordering).
pub trait KVSPlumb<K, V> {
    fn plumb_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static;

    fn plumb_from_cluster<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static;
}

impl<K, V> KVSPlumb<K, V> for () {
    fn plumb_from_process<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        panic!("Root layer cannot be terminal '()'; provide at least one KVSCluster.");
    }

    fn plumb_from_cluster<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        operations
    }
}

impl<K, V, Name, B, A, Child, Bg> KVSPlumb<K, V> for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: Before<K, V> + Clone,
    Child: KVSPlumb<K, V>,
{
    fn plumb_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let routed = self
            .before
            .dispatch_from_process_with_layers::<Name>(layers, operations, my_cluster);
        self.child.plumb_from_cluster(layers, my_cluster, routed)
    }

    fn plumb_from_cluster<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let routed = self.before.dispatch_from_cluster_with_layers::<Name>(
            operations,
            source_cluster,
            my_cluster,
            layers,
        );
        self.child.plumb_from_cluster(layers, my_cluster, routed)
    }
}

impl<K, V, Name, B, A> KVSPlumb<K, V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + Eq + std::fmt::Debug + Send + Sync + 'static,
    B: Before<K, V> + Clone,
{
    fn plumb_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<K, V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.before
            .dispatch_from_process_with_layers::<Name>(layers, operations, my_cluster)
    }

    fn plumb_from_cluster<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<K, V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<K, V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        K: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        self.before.dispatch_from_cluster_with_layers::<Name>(
            operations,
            source_cluster,
            source_cluster,
            _layers,
        )
    }
}
