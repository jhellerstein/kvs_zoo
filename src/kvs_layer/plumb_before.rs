use hydro_lang::prelude::*;

use crate::before_storage::Before;

/// Trait to plumb routing across layers using per-layer Before components (routing/ordering).
pub trait KVSPlumb<V> {
    fn plumb_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;

    fn plumb_from_cluster<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> KVSPlumb<V> for () {
    fn plumb_from_process<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        panic!("Root layer cannot be terminal '()'; provide at least one KVSCluster.");
    }

    fn plumb_from_cluster<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<
            crate::protocol::KVSOperation<V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        operations
    }
}

impl<V, Name, B, A, Child> KVSPlumb<V> for crate::kvs_layer::KVSCluster<Name, B, A, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: Before<V> + Clone,
    Child: KVSPlumb<V>,
{
    fn plumb_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
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
            crate::protocol::KVSOperation<V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
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

impl<V, Name, B, A> KVSPlumb<V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: Before<V> + Clone,
{
    fn plumb_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
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
            crate::protocol::KVSOperation<V>,
            Cluster<'a, crate::kvs_core::KVSNode>,
            Unbounded,
        >,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
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
