use hydro_lang::prelude::*;

use crate::after_storage::{AfterResponses, ReplicationStrategy};

/// After-stage plumbing: traverse replication/responders chain from leaf upward.
pub trait AfterPlumb<V> {
    fn after_responses<'a>(
        &self,
        layers: &crate::kvs_layer::KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> AfterPlumb<V> for () {
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

impl<V, Name, B, A, Child> AfterPlumb<V> for crate::kvs_layer::KVSCluster<Name, B, A, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: crate::before_storage::Before<V> + Clone,
    A: ReplicationStrategy<V> + AfterResponses + Clone,
    Child: AfterPlumb<V>,
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
        self.after.after_responses(my_cluster, bubbled)
    }
}

impl<V, Name, B, A> AfterPlumb<V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: crate::before_storage::Before<V> + Clone,
    A: ReplicationStrategy<V> + AfterResponses + Clone,
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
        self.after.after_responses(my_cluster, responses)
    }
}
