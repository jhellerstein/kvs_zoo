//! Declarative cluster specification with recursive nesting support
//!
//! Supports arbitrary-depth nesting: Cluster → Cluster → ... → Node
//!
//! ## Design
//!
//! The specification allows arbitrary nesting of clusters:
//! - `KVSCluster<D, M, Child>` where `Child` can be another `KVSCluster` or a `KVSNode`
//! - `KVSNode<D, M>` is the leaf (actual storage replicas)
//!
//! Dispatch and maintenance chains are built recursively:
//! - **Dispatch**: `Pipeline<D1, Pipeline<D2, D3>>` for nested routing
//! - **Maintenance**: `CombinedMaintenance<M1, CombinedMaintenance<M2, M3>>` for layered sync
//!
//! ## Examples
//!
//! ### Single node (1 level)
//! ```ignore
//! KVSNode {
//!     dispatch: SingleNodeRouter,
//!     maintenance: (),
//!     count: 1
//! }
//! ```
//!
//! ### Replicated (2 levels)
//! ```ignore
//! KVSCluster {
//!     dispatch: RoundRobinRouter::new(),
//!     maintenance: SimpleGossip::new(100),
//!     count: 3,  // 3 replicas
//!     each: KVSNode {
//!         dispatch: SingleNodeRouter,
//!         maintenance: TombstoneCleanup::new(5000),
//!         count: 1
//!     }
//! }
//! ```
//!
//! ### Sharded + Replicated (2 levels)
//! ```ignore
//! KVSCluster {
//!     dispatch: ShardedRouter::new(3),
//!     maintenance: (),  // No cross-shard sync
//!     count: 3,  // 3 shards
//!     each: KVSNode {
//!         dispatch: RoundRobinRouter::new(),
//!         maintenance: BroadcastReplication::default(),
//!         count: 3  // 3 replicas per shard
//!     }
//! }
//! ```
//!
//! ### Multi-region (3 levels)
//! ```ignore
//! KVSCluster {
//!     dispatch: RegionRouter::new(3),
//!     maintenance: (),
//!     count: 3,  // 3 regions
//!     each: KVSCluster {
//!         dispatch: DatacenterRouter::new(),
//!         maintenance: SimpleGossip::new(100),
//!         count: 2,  // 2 datacenters per region
//!         each: KVSNode {
//!             dispatch: (),
//!             maintenance: TombstoneCleanup::new(5000),
//!             count: 5  // 5 nodes per datacenter
//!         }
//!     }
//! }
//! // Total: 3 × 2 × 5 = 30 nodes
//! ```

use std::fmt;
use crate::dispatch::{OpDispatch, Pipeline};
use crate::maintenance::{ReplicationStrategy, CombinedMaintenance};
use crate::server::{KVSBuilder, BuiltKVS};

/// Cluster specification supporting recursive nesting
///
/// - `D`: Dispatch at this level
/// - `M`: Maintenance at this level
/// - `Child`: Either another `KVSCluster` or `KVSNode` (leaf)
#[derive(Clone)]
pub struct KVSCluster<D, M, Child> {
    pub dispatch: D,
    pub maintenance: M,
    pub count: usize,
    pub each: Child,
    /// Optional auxiliary clusters (e.g., for Paxos proposers/acceptors)
    /// These are deployed alongside but separate from the main cluster hierarchy.
    /// Use `.with_aux1()` and `.with_aux2()` to set these.
    aux_clusters: Option<AuxiliaryClusters>,
}

impl<D, M, Child> KVSCluster<D, M, Child> {
    /// Create a new cluster specification
    pub fn new(dispatch: D, maintenance: M, count: usize, each: Child) -> Self {
        Self {
            dispatch,
            maintenance,
            count,
            each,
            aux_clusters: None,
        }
    }
}

/// Auxiliary clusters for protocols that need additional node groups
/// (e.g., Paxos needs proposers and acceptors in addition to storage replicas)
#[derive(Clone, Default)]
pub struct AuxiliaryClusters {
    /// First auxiliary cluster with optional name
    pub aux1: Option<(usize, Option<String>)>, // (count, name)
    /// Second auxiliary cluster with optional name
    pub aux2: Option<(usize, Option<String>)>, // (count, name)
}

impl AuxiliaryClusters {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Add first auxiliary cluster with count
    pub fn with_aux1(mut self, count: usize) -> Self {
        self.aux1 = Some((count, None));
        self
    }
    
    /// Add first auxiliary cluster with count and name
    pub fn with_aux1_named(mut self, count: usize, name: impl Into<String>) -> Self {
        self.aux1 = Some((count, Some(name.into())));
        self
    }
    
    /// Add second auxiliary cluster with count
    pub fn with_aux2(mut self, count: usize) -> Self {
        self.aux2 = Some((count, None));
        self
    }
    
    /// Add second auxiliary cluster with count and name
    pub fn with_aux2_named(mut self, count: usize, name: impl Into<String>) -> Self {
        self.aux2 = Some((count, Some(name.into())));
        self
    }
}

/// Leaf node specification (storage replicas)
#[derive(Clone)]
pub struct KVSNode<D, M> {
    pub dispatch: D,
    pub maintenance: M,
    pub count: usize,
}

// =============================================================================
// ClusterSpec trait - enables recursive operations on any nesting depth
// =============================================================================

/// Core trait for recursive cluster specifications
///
/// This trait allows both `KVSCluster` and `KVSNode` to be used interchangeably
/// as children in recursive cluster definitions. It provides methods for
/// calculating total nodes and building dispatch/maintenance chains recursively.
pub trait ClusterSpec {
    /// The type of the dispatch chain for this spec level and below
    type DispatchChain;
    
    /// The type of the maintenance chain for this spec level and below
    type MaintenanceChain;
    
    /// Total number of leaf nodes in this spec (recursive)
    fn total_nodes(&self) -> usize;
    
    /// Build the dispatch chain recursively
    fn build_dispatch_chain(self) -> Self::DispatchChain;
    
    /// Build the maintenance chain recursively
    fn build_maintenance_chain(self) -> Self::MaintenanceChain;
}

// =============================================================================
// Base case: KVSNode (leaf)
// =============================================================================

impl<D, M> ClusterSpec for KVSNode<D, M> {
    type DispatchChain = D;
    type MaintenanceChain = M;
    
    fn total_nodes(&self) -> usize {
        self.count
    }
    
    fn build_dispatch_chain(self) -> Self::DispatchChain {
        self.dispatch
    }
    
    fn build_maintenance_chain(self) -> Self::MaintenanceChain {
        self.maintenance
    }
}

// =============================================================================
// Recursive case: KVSCluster<D, M, Child>
// =============================================================================

impl<D, M, Child> ClusterSpec for KVSCluster<D, M, Child>
where
    Child: ClusterSpec,
{
    /// Dispatch chain is Pipeline<D, Child::DispatchChain>
    type DispatchChain = Pipeline<D, Child::DispatchChain>;
    
    /// Maintenance chain is CombinedMaintenance<M, Child::MaintenanceChain>
    type MaintenanceChain = CombinedMaintenance<M, Child::MaintenanceChain>;
    
    fn total_nodes(&self) -> usize {
        self.count * self.each.total_nodes()
    }
    
    fn build_dispatch_chain(self) -> Self::DispatchChain {
        Pipeline::new(self.dispatch, self.each.build_dispatch_chain())
    }
    
    fn build_maintenance_chain(self) -> Self::MaintenanceChain {
        CombinedMaintenance::new(self.maintenance, self.each.build_maintenance_chain())
    }
}

// =============================================================================
// Ergonomic builder methods for KVSCluster
// =============================================================================

impl<D, M, Child> KVSCluster<D, M, Child>
where
    Child: ClusterSpec,
    Self: ClusterSpec,
    D: Clone,
    M: Clone,
    Child: Clone,
{
    /// Total number of leaf nodes in the cluster (recursive)
    pub fn total_nodes(&self) -> usize {
        ClusterSpec::total_nodes(self)
    }

    /// Add auxiliary clusters for protocols that need them (e.g., Paxos)
    pub fn with_aux_clusters(mut self, aux: AuxiliaryClusters) -> Self {
        self.aux_clusters = Some(aux);
        self
    }
    
    /// Set first auxiliary cluster count (e.g., Paxos proposers)
    pub fn with_aux1(mut self, count: usize) -> Self {
        self.aux_clusters.get_or_insert_with(AuxiliaryClusters::new).aux1 = Some((count, None));
        self
    }
    
    /// Set first auxiliary cluster with count and name
    pub fn with_aux1_named(mut self, count: usize, name: impl Into<String>) -> Self {
        self.aux_clusters.get_or_insert_with(AuxiliaryClusters::new).aux1 = Some((count, Some(name.into())));
        self
    }
    
    /// Set second auxiliary cluster count (e.g., Paxos acceptors)
    pub fn with_aux2(mut self, count: usize) -> Self {
        self.aux_clusters.get_or_insert_with(AuxiliaryClusters::new).aux2 = Some((count, None));
        self
    }
    
    /// Set second auxiliary cluster with count and name
    pub fn with_aux2_named(mut self, count: usize, name: impl Into<String>) -> Self {
        self.aux_clusters.get_or_insert_with(AuxiliaryClusters::new).aux2 = Some((count, Some(name.into())));
        self
    }

    /// Create a KVSBuilder from this spec, automatically building dispatch and maintenance chains.
    ///
    /// Supply only the value type `V`; the dispatch and maintenance types are inferred from the spec.
    /// Works with any nesting depth - dispatch and maintenance are recursively composed.
    pub fn builder_for<V>(
        self,
    ) -> KVSBuilder<
        V,
        <Self as ClusterSpec>::DispatchChain,
        <Self as ClusterSpec>::MaintenanceChain,
    >
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        <Self as ClusterSpec>::DispatchChain: OpDispatch<V> + Clone,
        <Self as ClusterSpec>::MaintenanceChain: ReplicationStrategy<V> + Clone,
    {
        let total = self.total_nodes();
        let aux_clusters = self.aux_clusters.clone();
        let dispatch = self.clone().build_dispatch_chain();
        let maintenance = self.build_maintenance_chain();

        KVSBuilder {
            num_nodes: total,
            num_aux1: aux_clusters.as_ref().and_then(|a| a.aux1.as_ref().map(|(count, _)| *count)),
            num_aux2: aux_clusters.as_ref().and_then(|a| a.aux2.as_ref().map(|(count, _)| *count)),
            dispatch: Some(dispatch),
            maintenance: Some(maintenance),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Build a server directly from this spec by specifying only the value type `V`.
    ///
    /// Works with any nesting depth - dispatch and maintenance are recursively composed.
    pub async fn build_server<V>(
        self,
    ) -> Result<
        BuiltKVS<V, <Self as ClusterSpec>::DispatchChain, <Self as ClusterSpec>::MaintenanceChain>,
        Box<dyn std::error::Error>,
    >
    where
        V: Clone
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + PartialEq
            + Eq
            + Default
            + std::fmt::Debug
            + std::fmt::Display
            + lattices::Merge<V>
            + Send
            + Sync
            + 'static,
        <Self as ClusterSpec>::DispatchChain: OpDispatch<V> + Clone,
        <Self as ClusterSpec>::MaintenanceChain: ReplicationStrategy<V> + Clone,
    {
        self.builder_for::<V>().build().await
    }
}

// =============================================================================
// Debug implementations
// =============================================================================

impl<D: fmt::Debug, M: fmt::Debug, Child: fmt::Debug> fmt::Debug for KVSCluster<D, M, Child> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("KVSCluster");
        s.field("dispatch", &self.dispatch)
            .field("maintenance", &self.maintenance)
            .field("count", &self.count)
            .field("each", &self.each);
        if let Some(ref aux) = self.aux_clusters {
            s.field("aux_clusters", aux);
        }
        s.finish()
    }
}

impl<D: fmt::Debug, M: fmt::Debug> fmt::Debug for KVSNode<D, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KVSNode")
            .field("dispatch", &self.dispatch)
            .field("maintenance", &self.maintenance)
            .field("count", &self.count)
            .finish()
    }
}

impl fmt::Debug for AuxiliaryClusters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("AuxiliaryClusters");
        if let Some((count, name)) = &self.aux1 {
            if let Some(n) = name {
                s.field("aux1", &format!("{} ({})", count, n));
            } else {
                s.field("aux1", count);
            }
        }
        if let Some((count, name)) = &self.aux2 {
            if let Some(n) = name {
                s.field("aux2", &format!("{} ({})", count, n));
            } else {
                s.field("aux2", count);
            }
        }
        s.finish()
    }
}
