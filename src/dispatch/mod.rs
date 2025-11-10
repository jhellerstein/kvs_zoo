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
{}

/* ------------------------------------------------------------------------- */
/* Deployment Abstractions                                                    */
/* ------------------------------------------------------------------------- */

/// Trait abstraction so different deployment layouts can be consumed uniformly.
pub trait KVSDeployment<'a> {
    fn kvs_cluster(&self) -> &Cluster<'a, KVSNode>;
    fn kvs_clusters(&self) -> Option<&[Cluster<'a, KVSNode>]> {
        None
    }
}

/// Basic deployment patterns used by dispatchers.
#[derive(Clone, Debug)]
pub enum Deployment<'a> {
    SingleCluster(Cluster<'a, KVSNode>),
    ShardedClusters(Vec<Cluster<'a, KVSNode>>),
}

impl<'a> KVSDeployment<'a> for Deployment<'a> {
    fn kvs_cluster(&self) -> &Cluster<'a, KVSNode> {
        match self {
            Deployment::SingleCluster(c) => c,
            Deployment::ShardedClusters(v) => &v[0],
        }
    }
    fn kvs_clusters(&self) -> Option<&[Cluster<'a, KVSNode>]> {
        match self {
            Deployment::ShardedClusters(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl<'a> KVSDeployment<'a> for Cluster<'a, KVSNode> {
    fn kvs_cluster(&self) -> &Cluster<'a, KVSNode> {
        self
    }
}

/* ------------------------------------------------------------------------- */
/* Dispatch Core                                                          */
/* ------------------------------------------------------------------------- */

/// Dispatcher transforming a process‑originated stream into a cluster stream.
pub trait OpDispatch<V> {
    type Deployment<'a>: KVSDeployment<'a>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a>;

    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
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
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        flow.cluster::<KVSNode>()
    }

    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Forward everything directly to member 0 (same semantics as SingleNodeRouter).
        operations
            .map(q!(|op| (hydro_lang::location::MemberId::from_raw(0u32), op)))
            .into_keyed()
            .demux_bincode(deployment)
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
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        flow.cluster::<KVSNode>()
    }

    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        operations.broadcast_bincode(deployment, nondet!(/** identity */))
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
    type Deployment<'a> = A::Deployment<'a>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        self.first.create_deployment(flow)
    }

    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        self.first.dispatch_operations(operations, deployment)
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
