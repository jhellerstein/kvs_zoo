use hydro_lang::prelude::*;

use crate::after_storage::ReplicationStrategy;
use crate::before_storage::OpDispatch;

/// Trait for KVS specifications that can create and register clusters.
pub trait KVSSpec<V> {
    /// Create clusters for this layer and all child layers, registering them in the layers map.
    /// Returns the cluster that should receive operations FROM this layer (i.e., the child's entry point).
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

// Base case: terminal `()`
impl<V> KVSSpec<V> for () {
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        _layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        flow.cluster::<crate::kvs_core::KVSNode>()
    }
}

// Recursive case: KVSCluster
impl<V, Name, D, M, Child> KVSSpec<V> for crate::kvs_layer::KVSCluster<Name, D, M, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
    Child: KVSSpec<V>,
{
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = flow.cluster::<crate::kvs_core::KVSNode>();
        layers.insert::<Name>(my_cluster.clone());
        let _child_entry = self.child.create_clusters(flow, layers);
        my_cluster
    }
}

// Leaf case: KVSNode (current behavior: creates a cluster; future: reuse parent cluster)
impl<V, Name, D, M> KVSSpec<V> for crate::kvs_layer::KVSNode<Name, D, M>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = flow.cluster::<crate::kvs_core::KVSNode>();
        layers.insert::<Name>(my_cluster.clone());
        my_cluster
    }
}
