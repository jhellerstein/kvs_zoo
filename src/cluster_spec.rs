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
        let dispatch = self.clone().build_dispatch_chain();
        let maintenance = self.build_maintenance_chain();

        KVSBuilder {
            num_nodes: total,
            num_aux1: None,
            num_aux2: None,
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
        f.debug_struct("KVSCluster")
            .field("dispatch", &self.dispatch)
            .field("maintenance", &self.maintenance)
            .field("count", &self.count)
            .field("each", &self.each)
            .finish()
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
