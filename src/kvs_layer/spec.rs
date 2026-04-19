use crate::after_storage::{LeafCompatible, ReplicationStrategy};
use crate::before_storage::Before;
use hydro_lang::prelude::*;

/// Trait for KVS specifications that can create and register clusters.
pub trait KVSSpec<V> {
    /// Create clusters for this layer and all child layers, registering them in the layers map.
    /// Returns the cluster that should receive operations FROM this layer (i.e., the child's entry point).
    fn create_clusters<'a>(
        &self,
        flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
    
    /// Returns true if this KVS configuration requires linearizable processing.
    /// Default implementation returns false (coordination-free processing).
    fn requires_linearizable(&self) -> bool {
        false
    }
}

// Base case: terminal `()`
impl<V> KVSSpec<V> for () {
    fn create_clusters<'a>(
        &self,
        flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
        _layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        flow.cluster::<crate::kvs_core::KVSNode>()
    }
}

// Recursive case: KVSCluster
impl<V, Name, B, A, Child, Bg> KVSSpec<V> for crate::kvs_layer::KVSCluster<Name, B, A, Child, Bg>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: Before<String, V> + Clone,
    A: ReplicationStrategy<String, V> + Clone,
    Child: KVSSpec<V>,
{
    fn create_clusters<'a>(
        &self,
        flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut crate::kvs_layer::KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = flow.cluster::<crate::kvs_core::KVSNode>();
        layers.insert::<Name>(my_cluster.clone());
        // Allow the dispatcher to register any role-specific sub-clusters for this layer.
        self.before.register_role_clusters::<Name>(flow, layers);
        let _child_entry = self.child.create_clusters(flow, layers);
        my_cluster
    }
    
    fn requires_linearizable(&self) -> bool {
        // Check if this layer's Before component or any child requires linearizable processing
        B::requires_linearizable() || self.child.requires_linearizable()
    }
}

// Leaf case: KVSNode (current behavior: creates a cluster; future: reuse parent cluster)
impl<V, Name, B, A> KVSSpec<V> for crate::kvs_layer::KVSNode<Name, B, A>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    B: Before<String, V> + Clone,
    A: ReplicationStrategy<String, V> + Clone + LeafCompatible,
{
    fn create_clusters<'a>(
        &self,
        flow: &mut hydro_lang::compile::builder::FlowBuilder<'a>,
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
