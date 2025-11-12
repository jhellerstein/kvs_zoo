//! Before-storage stages (routing, ordering, etc.)

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

pub mod routing;
pub mod ordering;

/* ------------------------------------------------------------------------- */
/* Dispatch Core (moved from crate::dispatch)                                */
/* ------------------------------------------------------------------------- */

/// Dispatcher for routing operations from proxy to cluster.
pub trait OpDispatch<V> {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    fn dispatch_slotted_from_process<'a>(
        &self,
        slotted_operations: Stream<(usize, KVSOperation<V>), Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let bare_ops = slotted_operations.map(q!(|(_slot, op)| op));
        let routed = self.dispatch_from_process(bare_ops, target_cluster);
        routed.map(q!(|op| (0usize, op)))
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
            .assume_ordering(nondet!(/** cluster hop routed */))
    }
}

/// Unit (No‑Op) Dispatch for Leaf Nodes
impl<V> OpDispatch<V> for () {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
            .values()
            .assume_ordering(nondet!(/** cluster hop routed */))
    }
}

/// Identity (broadcast) dispatcher placeholder.
#[derive(Clone, Debug, Default)]
pub struct IdentityDispatch;
impl IdentityDispatch {
    pub fn new() -> Self {
        Self
    }
}

impl<V> OpDispatch<V> for IdentityDispatch {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations.broadcast_bincode(target_cluster, nondet!(/** identity */))
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations
            .broadcast_bincode(target_cluster, nondet!(/** identity layer hop */))
            .values()
            .assume_ordering(nondet!(/** cluster hop broadcast */))
    }
}

/// Static composition of two dispatchers.
#[derive(Clone, Debug)]
pub struct Pipeline<A, B> {
    pub first: A,
    pub second: B,
}
impl<A, B> Pipeline<A, B> {
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}
impl<A: Default, B: Default> Default for Pipeline<A, B> {
    fn default() -> Self {
        Self {
            first: A::default(),
            second: B::default(),
        }
    }
}

impl<A, B, V> OpDispatch<V> for Pipeline<A, B>
where
    A: OpDispatch<V>,
    B: OpDispatch<V>,
{
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let first_out = self.first.dispatch_from_process(operations, target_cluster);
        self.second
            .dispatch_from_cluster(first_out, target_cluster, target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let mid = self
            .first
            .dispatch_from_cluster(operations, source_cluster, target_cluster);
        self.second
            .dispatch_from_cluster(mid, target_cluster, target_cluster)
    }
}

/// Fluent extension for chaining.
pub trait OpDispatchExt<V>: OpDispatch<V> + Sized {
    fn then<N: OpDispatch<V>>(self, next: N) -> Pipeline<Self, N> {
        Pipeline::new(self, next)
    }
}
impl<V, T: OpDispatch<V>> OpDispatchExt<V> for T {}

/* ------------------------------------------------------------------------- */
/* Marker trait to forbid `()` at cluster level                               */
/* ------------------------------------------------------------------------- */
pub trait ClusterLevelDispatch {}

impl ClusterLevelDispatch for routing::SingleNodeRouter {}
impl ClusterLevelDispatch for routing::ShardedRouter {}
impl ClusterLevelDispatch for routing::RoundRobinRouter {}
impl<V> ClusterLevelDispatch for ordering::PaxosDispatcher<V> {}
impl ClusterLevelDispatch for IdentityDispatch {}
impl<A, B> ClusterLevelDispatch for Pipeline<A, B>
where
    A: ClusterLevelDispatch,
    B: ClusterLevelDispatch,
{
}

// Convenient root re-exports for common ordering types
pub use ordering::{PaxosConfig, PaxosDispatcher, SlotOrderEnforcer};
