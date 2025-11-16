use hydro_lang::location::cluster::Cluster;
use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;

/// A layer in the KVS architecture, pairing generic Before and After strategies.
///
/// - `Name`: marker type naming this layer (shared with Hydro location types)
/// - `B`: before_storage strategy (routing/ordering) at this level
/// - `A`: after_storage strategy (replication/responders) at this level
/// - `Child`: either another `KVSCluster` or `()` for terminal
#[derive(Clone)]
pub struct KVSCluster<Name, B, A, Child, Bg = ()> {
    _name: PhantomData<Name>,
    // Generic names: before = before_storage stage (e.g., routing/ordering); after = after_storage (e.g., replication/responders)
    pub before: B,
    pub after: A,
    pub child: Child,
    /// Optional background maintenance pipeline (tomb indexing, anti-entropy, etc.)
    pub background: Bg,
}

impl<Name, B, A, Child> KVSCluster<Name, B, A, Child> {
    /// Construct a cluster without a background pipeline (defaults to `()`).
    pub fn new(before: B, after: A, child: Child) -> Self {
        Self {
            _name: PhantomData,
            before,
            after,
            child,
            background: (),
        }
    }

    /// Construct a cluster while explicitly providing a background pipeline.
    pub fn with_background<Bg>(
        before: B,
        after: A,
        child: Child,
        background: Bg,
    ) -> KVSCluster<Name, B, A, Child, Bg> {
        KVSCluster {
            _name: PhantomData,
            before,
            after,
            child,
            background,
        }
    }
}

impl<Name, B, A, Child, Bg> KVSCluster<Name, B, A, Child, Bg> {
    pub fn new_with_background(before: B, after: A, child: Child, background: Bg) -> Self {
        Self {
            _name: PhantomData,
            before,
            after,
            child,
            background,
        }
    }
}

impl<Name, B: Default, A: Default, Child: Default, Bg: Default> Default
    for KVSCluster<Name, B, A, Child, Bg>
{
    fn default() -> Self {
        Self {
            _name: PhantomData,
            before: B::default(),
            after: A::default(),
            child: Child::default(),
            background: Bg::default(),
        }
    }
}

/// Collection of named cluster handles created during KVS wiring.
///
/// Supports optional role-specialized clusters per layer. Roles are identified by
/// Rust types so that layers like Paxos can register multiple internal clusters
/// (e.g., Proposer, Acceptor) under a single layer `Name` without the wiring
/// infrastructure knowing anything about Paxos.
pub struct KVSClusters<'a> {
    /// Mapping from (LayerName, Role) to Cluster handle.
    /// The default/no-role entries use `Role = ()`.
    clusters: HashMap<(TypeId, TypeId), Cluster<'a, crate::kvs_core::KVSNode>>,
}

impl<'a> KVSClusters<'a> {
    pub fn new() -> Self {
        Self {
            clusters: HashMap::new(),
        }
    }

    /// Insert a cluster handle for a named layer (no role).
    pub fn insert<Name: 'static>(&mut self, cluster: Cluster<'a, crate::kvs_core::KVSNode>) {
        self.insert_role::<Name, ()>(cluster);
    }

    /// Insert a cluster handle for a named layer and role.
    pub fn insert_role<Name: 'static, Role: 'static>(
        &mut self,
        cluster: Cluster<'a, crate::kvs_core::KVSNode>,
    ) {
        let key = (TypeId::of::<Name>(), TypeId::of::<Role>());
        self.clusters.insert(key, cluster);
    }

    /// Get the cluster handle for a named layer (no role).
    ///
    /// Panics if the layer name was not registered during wiring.
    pub fn get<Name: 'static>(&self) -> &Cluster<'a, crate::kvs_core::KVSNode> {
        self.get_role::<Name, ()>()
    }

    /// Get the cluster handle for a named layer and role.
    ///
    /// Panics if the (name, role) pair was not registered during wiring.
    pub fn get_role<Name: 'static, Role: 'static>(&self) -> &Cluster<'a, crate::kvs_core::KVSNode> {
        let key = (TypeId::of::<Name>(), TypeId::of::<Role>());
        self.clusters.get(&key).unwrap_or_else(|| {
            panic!(
                "No cluster found for layer type {} and role {}",
                std::any::type_name::<Name>(),
                std::any::type_name::<Role>()
            )
        })
    }

    /// Try to get the cluster handle for a named layer (no role).
    pub fn try_get<Name: 'static>(&self) -> Option<&Cluster<'a, crate::kvs_core::KVSNode>> {
        self.try_get_role::<Name, ()>()
    }

    /// Try to get the cluster handle for a named layer and role.
    pub fn try_get_role<Name: 'static, Role: 'static>(
        &self,
    ) -> Option<&Cluster<'a, crate::kvs_core::KVSNode>> {
        let key = (TypeId::of::<Name>(), TypeId::of::<Role>());
        self.clusters.get(&key)
    }
}

impl<'a> Default for KVSClusters<'a> {
    fn default() -> Self {
        Self::new()
    }
}

/// Leaf-level (per-member) layer placeholder.
#[derive(Clone)]
pub struct KVSNode<Name, B, A> {
    _name: PhantomData<Name>,
    pub before: B,
    pub after: A,
}

impl<Name, B, A> KVSNode<Name, B, A> {
    pub fn new(before: B, after: A) -> Self {
        Self {
            _name: PhantomData,
            before,
            after,
        }
    }
}

impl<Name, B: Default, A: Default> Default for KVSNode<Name, B, A> {
    fn default() -> Self {
        Self::new(B::default(), A::default())
    }
}
