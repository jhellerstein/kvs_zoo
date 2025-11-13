use hydro_lang::location::cluster::Cluster;
use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;

/// A layer in the KVS architecture, pairing before_storage and after_storage strategies.
///
/// - `Name`: marker type naming this layer (shared with Hydro location types)
/// - `D`: before_storage strategy (routing/ordering) at this level
/// - `M`: after_storage strategy (replication/responders) at this level
/// - `Child`: either another `KVSCluster` or `()` for terminal
#[derive(Clone)]
pub struct KVSCluster<Name, D, M, Child> {
    _name: PhantomData<Name>,
    // Field names retain historical naming; dispatch = before_storage, maintenance = after_storage.
    pub dispatch: D,
    pub maintenance: M,
    pub child: Child,
}

impl<Name, D, M, Child> KVSCluster<Name, D, M, Child> {
    pub fn new(dispatch: D, maintenance: M, child: Child) -> Self {
        Self { _name: PhantomData, dispatch, maintenance, child }
    }
}

impl<Name, D: Default, M: Default, Child: Default> Default for KVSCluster<Name, D, M, Child> {
    fn default() -> Self { Self::new(D::default(), M::default(), Child::default()) }
}

/// Collection of named cluster handles created during KVS wiring.
///
/// Allows type-safe lookup of cluster handles by layer name.
pub struct KVSClusters<'a> {
    clusters: HashMap<TypeId, Cluster<'a, crate::kvs_core::KVSNode>>,
}

impl<'a> KVSClusters<'a> {
    pub fn new() -> Self { Self { clusters: HashMap::new() } }

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
}

impl<'a> Default for KVSClusters<'a> { fn default() -> Self { Self::new() } }

/// Leaf-level (per-member) layer placeholder.
#[derive(Clone)]
pub struct KVSNode<Name, D, M> {
    _name: PhantomData<Name>,
    pub dispatch: D,
    pub maintenance: M,
}

impl<Name, D, M> KVSNode<Name, D, M> {
    pub fn new(dispatch: D, maintenance: M) -> Self { Self { _name: PhantomData, dispatch, maintenance } }
}

impl<Name, D: Default, M: Default> Default for KVSNode<Name, D, M> {
    fn default() -> Self { Self::new(D::default(), M::default()) }
}
