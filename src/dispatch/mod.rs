//! Dispatch layer: operation interception, routing, ordering composition.
//!
//! This is a cleaned, minimal version restored after earlier merge/refactor
//! corruption. It keeps public names (`OpDispatch`, `Pipeline`, `Deployment`)
//! needed by `server.rs` and examples, while removing half‑deleted fragments.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

pub mod filtering; // (reserved)
pub mod ordering; // consensus / total order (e.g. Paxos)
pub mod routing; // key → node / cluster selection

pub use ordering::SlotOrderEnforcer;
pub use ordering::{PaxosConfig, PaxosDispatcher};
pub use routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};

/* ------------------------------------------------------------------------- */
/* Marker trait to forbid `()` at cluster level                               */
/* ------------------------------------------------------------------------- */
/// Marker trait for dispatchers that are allowed at the cluster (outer) level.
///
/// We intentionally do NOT implement this for `()` so that `dispatch: ()` is
/// accepted for `KVSNode` leaves but rejected for `KVSCluster` in `from_spec`.
pub trait ClusterLevelDispatch {}

impl ClusterLevelDispatch for SingleNodeRouter {}
impl ClusterLevelDispatch for ShardedRouter {}
impl ClusterLevelDispatch for RoundRobinRouter {}
impl<V> ClusterLevelDispatch for crate::dispatch::ordering::PaxosDispatcher<V> {}
impl ClusterLevelDispatch for IdentityDispatch {}
impl<A, B> ClusterLevelDispatch for Pipeline<A, B>
where
    A: ClusterLevelDispatch,
    B: ClusterLevelDispatch,
{
}

/* ------------------------------------------------------------------------- */
/* (Deprecated deployment abstractions removed)                               */
/* ------------------------------------------------------------------------- */

/* ------------------------------------------------------------------------- */
/* Dispatch Core                                                          */
/* ------------------------------------------------------------------------- */

/// Dispatcher for routing operations from proxy to cluster.
///
/// Dispatch strategies determine how operations from the proxy process
/// are routed to members of the target cluster. This is the first hop
/// in the data flow: External → Proxy (Process) → Layer Cluster.
///
/// For inter-layer communication (Cluster→Cluster), see `wire_kvs_dataflow`.
pub trait OpDispatch<V> {
    /// Route operations from a proxy process to the target cluster (first hop).
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Route slotted operations from a proxy process to the target cluster, preserving slots.
    ///
    /// Default implementation strips slots, routes bare operations, then re-attaches dummy slot 0.
    /// Dispatchers that need to preserve slot ordering (e.g., for Paxos) should override this.
    fn dispatch_slotted_from_process<'a>(
        &self,
        slotted_operations: Stream<(usize, KVSOperation<V>), Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Default: strip slots, route, re-attach dummy slot 0 (loses slot ordering)
        let bare_ops = slotted_operations.map(q!(|(_slot, op)| op));
        let routed = self.dispatch_from_process(bare_ops, target_cluster);
        routed.map(q!(|op| (0usize, op)))
    }

    /// Route operations originating at a source cluster into a target cluster (inter-layer hop).
    /// Default implementation falls back to per-member local forwarding via member 0.
    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Default: treat all ops as if sent from member 0 of source cluster.
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

/* ------------------------------------------------------------------------- */
/* Unit (No‑Op) Dispatch for Leaf Nodes                                      */
/* ------------------------------------------------------------------------- */
/// `()` implements `OpDispatch` as a no‑op router for leaf (non‑replicated) nodes.
///
/// Rationale: A terminal storage node does not perform routing; the pipeline
/// abstraction originally forced a degenerate dispatcher (`SingleNodeRouter`).
/// Allowing `dispatch: ()` in a `KVSNode` makes examples clearer: no routing
/// happens at this level. For `KVSCluster` we still require a real dispatcher
/// (sharding, replica selection, consensus ordering, etc.).
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
        // Chain the second stage at the cluster hop
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
/* Tests (basic structural)                                                   */
/* ------------------------------------------------------------------------- */
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};

    #[test]
    fn identity_construction() {
        let _i = IdentityDispatch::new();
        let _d = IdentityDispatch;
    }

    #[test]
    fn pipeline_shape() {
        let p = Pipeline::new(IdentityDispatch::new(), SingleNodeRouter::new());
        let _ = format!("{:?}", p);
    }

    #[test]
    fn chaining_types() {
        fn _assert<V, T: OpDispatchExt<V>>(_: T) {}
        _assert::<String, _>(IdentityDispatch::new());
        _assert::<String, _>(SingleNodeRouter::new());
        _assert::<String, _>(RoundRobinRouter::new());
        _assert::<String, _>(ShardedRouter::new(3));
        let _nested = Pipeline::new(SingleNodeRouter::new(), RoundRobinRouter::new());
    }
}
