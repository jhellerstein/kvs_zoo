use hydro_lang::prelude::*;

use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy};

/// Upward wiring for after_storage: traverse replication/responders chain from leaf upward.
pub trait AfterWire<V> {
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> AfterWire<V> for () {
    fn after_responses<'a>(
        &self,
        _layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        responses
    }
}

impl<V, Name, D, M, Child> AfterWire<V> for crate::kvs_layer::KVSCluster<Name, D, M, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: crate::before_storage::OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + MaintenanceAfterResponses + Clone,
    Child: AfterWire<V>,
{
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let bubbled = self.child.after_responses(layers, responses);
        self.maintenance.after_responses(my_cluster, bubbled)
    }
}

impl<V, Name, D, M> AfterWire<V> for crate::kvs_layer::KVSNode<Name, D, M>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: crate::before_storage::OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + MaintenanceAfterResponses + Clone,
{
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.maintenance.after_responses(my_cluster, responses)
    }
}
