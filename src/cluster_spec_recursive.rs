//! Recursive declarative cluster specification
//!
//! Supports arbitrary-depth nesting: Cluster → Cluster → ... → Node

use std::fmt;
use crate::dispatch::{OpDispatch, ClusterLevelDispatch, Pipeline};
use crate::maintenance::{ReplicationStrategy, CombinedMaintenance};
use crate::server::{KVSBuilder, BuiltKVS};

/// Recursive cluster specification
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

/// Leaf node specification
#[derive(Clone)]
pub struct KVSNode<D, M> {
    pub dispatch: D,
    pub maintenance: M,
    pub count: usize,
}

// Debug impls
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

// Helper traits for recursive operations
pub trait ClusterSpec {
    fn total_nodes(&self) -> usize;
}

impl<D, M> ClusterSpec for KVSNode<D, M> {
    fn total_nodes(&self) -> usize {
        self.count
    }
}

impl<D, M, Child: ClusterSpec> ClusterSpec for KVSCluster<D, M, Child> {
    fn total_nodes(&self) -> usize {
        self.count * self.each.total_nodes()
    }
}

// 2-level spec (base case): KVSCluster<CD, CM, KVSNode<ND, NM>>
impl<CD, CM, ND, NM> KVSCluster<CD, CM, KVSNode<ND, NM>> {
    pub fn build_server<V>(self) -> KVSBuilder<V, Pipeline<CD, ND>, CombinedMaintenance<CM, NM>>
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
        CD: OpDispatch<V> + ClusterLevelDispatch + Clone,
        ND: OpDispatch<V> + Clone,
        CM: ReplicationStrategy<V> + Clone,
        NM: ReplicationStrategy<V> + Clone,
        Pipeline<CD, ND>: OpDispatch<V>,
    {
        KVSBuilder {
            num_nodes: self.total_nodes(),
            num_aux1: None,
            num_aux2: None,
            dispatch: Some(Pipeline::new(self.dispatch, self.each.dispatch)),
            maintenance: Some(CombinedMaintenance::new(self.maintenance, self.each.maintenance)),
            _phantom: std::marker::PhantomData,
        }
    }
}

// TODO: 3+ level recursive impl - needs more complex Pipeline and CombinedMaintenance chaining
