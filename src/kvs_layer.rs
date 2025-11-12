//! KVS architectural layering pattern.
//!
//! This module provides the core pattern for composing KVS architectures from
//! layers of dispatch and maintenance strategies. Each layer pairs a dispatch
//! strategy (how to route operations) with a maintenance strategy (how to keep
//! replicas in sync), and can nest recursively.
//!
//! ## Example
//!
//! ```ignore
//! struct Shard;
//! struct Replica;
//!
//! type MyKVS = KVSCluster<
//!     Shard,
//!     ShardedRouter,
//!     (),
//!     KVSCluster<Replica, RoundRobinRouter, BroadcastReplication, ()>
//! >;
//! ```

use crate::before_storage::OpDispatch;
use crate::after_storage::{MaintenanceAfterResponses, ReplicationStrategy};
use crate::protocol::KVSOperation;
use hydro_lang::location::cluster::Cluster;
use hydro_lang::prelude::*;
use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;

/// A layer in the KVS architecture, pairing dispatch and maintenance strategies.
///
/// - `Name`: marker type naming this layer (shared with Hydro location types)
/// - `D`: dispatch strategy at this level
/// - `M`: maintenance strategy at this level
/// - `Child`: either another `KVSCluster` or `()` for terminal
#[derive(Clone)]
pub struct KVSCluster<Name, D, M, Child> {
    _name: PhantomData<Name>,
    pub dispatch: D,
    pub maintenance: M,
    pub child: Child,
}

impl<Name, D, M, Child> KVSCluster<Name, D, M, Child> {
    pub fn new(dispatch: D, maintenance: M, child: Child) -> Self {
        Self {
            _name: PhantomData,
            dispatch,
            maintenance,
            child,
        }
    }
}

impl<Name, D: Default, M: Default, Child: Default> Default for KVSCluster<Name, D, M, Child> {
    fn default() -> Self {
        Self::new(D::default(), M::default(), Child::default())
    }
}

/// Collection of named cluster handles created during KVS wiring.
///
/// Allows type-safe lookup of cluster handles by layer name.
pub struct KVSClusters<'a> {
    clusters: HashMap<TypeId, Cluster<'a, crate::kvs_core::KVSNode>>,
}

impl<'a> KVSClusters<'a> {
    pub fn new() -> Self {
        Self {
            clusters: HashMap::new(),
        }
    }

    /// Insert a cluster handle for a named layer.
    pub fn insert<Name: 'static>(&mut self, cluster: Cluster<'a, crate::kvs_core::KVSNode>) {
        self.clusters.insert(TypeId::of::<Name>(), cluster);
    }

    /// Get the cluster handle for a named layer.
    ///
    /// Panics if the layer name was not registered during wiring.
    pub fn get<Name: 'static>(&self) -> &Cluster<'a, crate::kvs_core::KVSNode> {
        self.clusters.get(&TypeId::of::<Name>()).unwrap_or_else(|| {
            panic!(
                "No cluster found for layer type {}",
                std::any::type_name::<Name>()
            )
        })
    }

    /// Try to get the cluster handle for a named layer.
    pub fn try_get<Name: 'static>(&self) -> Option<&Cluster<'a, crate::kvs_core::KVSNode>> {
        self.clusters.get(&TypeId::of::<Name>())
    }

    // Auxiliary cluster support intentionally removed to keep Paxos specifics
    // out of generic KVS layering. Paxos clusters are created and wired in
    // example-specific code instead of leaking here.
}

impl<'a> Default for KVSClusters<'a> {
    fn default() -> Self {
        Self::new()
    }
}

/// Leaf-level (per-member) layer placeholder.
///
/// Note: In the current iteration this behaves similarly to a terminal
/// layer and will create its own cluster when used at the top level.
/// The intent is to evolve this to reuse the parent cluster (true node-level),
/// but we expose the type now to align naming and allow examples to adopt it.
#[derive(Clone)]
pub struct KVSNode<Name, D, M> {
    _name: PhantomData<Name>,
    pub dispatch: D,
    pub maintenance: M,
}

impl<Name, D, M> KVSNode<Name, D, M> {
    pub fn new(dispatch: D, maintenance: M) -> Self {
        Self {
            _name: PhantomData,
            dispatch,
            maintenance,
        }
    }
}

impl<Name, D: Default, M: Default> Default for KVSNode<Name, D, M> {
    fn default() -> Self {
        Self::new(D::default(), M::default())
    }
}

/// Trait for KVS specifications that can create and register clusters.
pub trait KVSSpec<V> {
    /// Create clusters for this layer and all child layers, registering them in the layers map.
    /// Returns the cluster that should receive operations FROM this layer (i.e., the child's entry point).
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

/// Trait to wire routing across layers using per-layer dispatchers.
///
/// Provides methods to route operations from a proxy process into the first
/// cluster, and then recursively from cluster to cluster for nested layers.
pub trait KVSWire<V> {
    fn wire_from_process<'a>(
        &self,
        layers: &KVSClusters<'a>,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;

    fn wire_from_cluster<'a>(
        &self,
        layers: &KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> KVSWire<V> for () {
    fn wire_from_process<'a>(
        &self,
        _layers: &KVSClusters<'a>,
        _operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        panic!("Root layer cannot be terminal '()'; provide at least one KVSCluster.");
    }

    fn wire_from_cluster<'a>(
        &self,
        _layers: &KVSClusters<'a>,
        _source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        operations
    }
}

// (Leaf case handled by the recursive impl via Child = () implementing KVSWire)

// Recursive (non-leaf) implementation.
impl<V, Name, D, M, Child> KVSWire<V> for KVSCluster<Name, D, M, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
    Child: KVSWire<V>,
{
    fn wire_from_process<'a>(
        &self,
        layers: &KVSClusters<'a>,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        let routed = self.dispatch.dispatch_from_process(operations, my_cluster);
        self.child.wire_from_cluster(layers, my_cluster, routed)
    }

    fn wire_from_cluster<'a>(
        &self,
        layers: &KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
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

// Leaf case: KVSNode (current behavior: creates a cluster; future: reuse parent cluster)
impl<V, Name, D, M> KVSSpec<V> for KVSNode<Name, D, M>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        layers: &mut KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = flow.cluster::<crate::kvs_core::KVSNode>();
        layers.insert::<Name>(my_cluster.clone());
        my_cluster
    }
}

// Leaf wiring: apply dispatch at the current (leaf) cluster boundary.
impl<V, Name, D, M> KVSWire<V> for KVSNode<Name, D, M>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + Clone,
{
    fn wire_from_process<'a>(
        &self,
        layers: &KVSClusters<'a>,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.dispatch.dispatch_from_process(operations, my_cluster)
    }

    fn wire_from_cluster<'a>(
        &self,
        _layers: &KVSClusters<'a>,
        source_cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        operations: Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        // Leaf dispatch runs within the same (parent) cluster, not a separate cluster.
        // This makes KVSNode truly represent "per-member" behavior.
        self.dispatch
            .dispatch_from_cluster(operations, source_cluster, source_cluster)
    }
}

/// Upward wiring for maintenance (After storage): traverse maintenance chain from leaf upward.
pub trait AfterWire<V> {
    fn after_responses<'a>(
        &self,
        layers: &KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
}

impl<V> AfterWire<V> for () {
    fn after_responses<'a>(
        &self,
        _layers: &KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        responses
    }
}

impl<V, Name, D, M, Child> AfterWire<V> for KVSCluster<Name, D, M, Child>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + MaintenanceAfterResponses + Clone,
    Child: AfterWire<V>,
{
    fn after_responses<'a>(
        &self,
        layers: &KVSClusters<'a>,
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

impl<V, Name, D, M> AfterWire<V> for KVSNode<Name, D, M>
where
    Name: 'static,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    D: OpDispatch<V> + Clone,
    M: ReplicationStrategy<V> + MaintenanceAfterResponses + Clone,
{
    fn after_responses<'a>(
        &self,
        layers: &KVSClusters<'a>,
        responses: Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let my_cluster = layers.get::<Name>();
        self.maintenance.after_responses(my_cluster, responses)
    }
}

// Base case: terminal `()`
impl<V> KVSSpec<V> for () {
    fn create_clusters<'a>(
        &self,
        flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
        _layers: &mut KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        // Terminal case: create a single cluster for the final storage layer
        flow.cluster::<crate::kvs_core::KVSNode>()
    }
}

// Recursive case: KVSCluster
impl<V, Name, D, M, Child> KVSSpec<V> for KVSCluster<Name, D, M, Child>
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
        layers: &mut KVSClusters<'a>,
    ) -> Cluster<'a, crate::kvs_core::KVSNode>
    where
        V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        // Create this layer's own physical cluster and register under its Name.
        let my_cluster = flow.cluster::<crate::kvs_core::KVSNode>();
        layers.insert::<Name>(my_cluster.clone());
        // Recursively create child clusters (side effects register them) and ignore returned value.
        let _child_entry = self.child.create_clusters(flow, layers);
        // Return this layer's cluster as entry point for upstream operations.
        my_cluster
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::after_storage::NoReplication;
    use crate::before_storage::routing::SingleNodeRouter;

    struct TestLayer;

    #[test]
    fn layer_creation() {
        let _layer = KVSCluster::<TestLayer, SingleNodeRouter, NoReplication, ()>::new(
            SingleNodeRouter,
            NoReplication,
            (),
        );
    }

    #[test]
    fn layer_default() {
        let _layer = KVSCluster::<TestLayer, SingleNodeRouter, NoReplication, ()>::default();
    }

    #[test]
    fn layers_lookup() {
        let layers = KVSClusters::new();
        // Can't easily test insert/get without a real FlowBuilder
        // Just verify the API compiles
        assert!(layers.try_get::<TestLayer>().is_none());
    }
}
