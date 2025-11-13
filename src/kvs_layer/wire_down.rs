use hydro_lang::prelude::*;

use crate::before_storage::OpDispatch;

/// Trait to wire routing across layers using per-layer before_storage components (dispatchers/ordering).
pub trait KVSWire<V> {
    fn wire_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;

    fn wire_from_cluster<'a>(
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

impl<V> KVSWire<V> for () {
    fn wire_from_process<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        _operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        panic!("Root layer cannot be terminal '()'; provide at least one KVSCluster.");
    }

    fn wire_from_cluster<'a>(
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

impl<V, Name, D, M, Child> KVSWire<V> for crate::kvs_layer::KVSCluster<Name, D, M, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    Child: KVSWire<V>,
{
    fn wire_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let routed = self.dispatch.dispatch_from_process(operations, my_cluster);
        self.child.wire_from_cluster(layers, my_cluster, routed)
    }

    fn wire_from_cluster<'a>(
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
        let routed = self
            .dispatch
            .dispatch_from_cluster(operations, source_cluster, my_cluster);
        self.child.wire_from_cluster(layers, my_cluster, routed)
    }
}

impl<V, Name, D, M> KVSWire<V> for crate::kvs_layer::KVSNode<Name, D, M>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
{
    fn wire_from_process<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        operations: Stream<crate::protocol::KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<crate::protocol::KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.dispatch.dispatch_from_process(operations, my_cluster)
    }

    fn wire_from_cluster<'a>(
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
        self.dispatch
            .dispatch_from_cluster(operations, source_cluster, source_cluster)
    }
}
