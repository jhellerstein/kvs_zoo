//! Dispatch layer: operation interception, routing, ordering composition.
//!
//! This is a cleaned, minimal version restored after earlier merge/refactor
//! corruption. It keeps public names (`OpIntercept`, `Pipeline`, `Deployment`)
//! needed by `server.rs` and examples, while removing half‑deleted fragments.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

pub mod filtering; // (reserved)
pub mod ordering;  // consensus / total order (e.g. Paxos)
pub mod routing;   // key → node / cluster selection
pub mod spec_interpreter; // cluster spec → dispatch strategy

pub use ordering::{PaxosConfig, PaxosInterceptor};
pub use routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};
pub use spec_interpreter::{DispatchStrategy, InferDispatch};

/* ------------------------------------------------------------------------- */
/* Deployment Abstractions                                                    */
/* ------------------------------------------------------------------------- */

/// Trait abstraction so different deployment layouts can be consumed uniformly.
pub trait KVSDeployment<'a> {
    fn kvs_cluster(&self) -> &Cluster<'a, KVSNode>;
    fn kvs_clusters(&self) -> Option<&[Cluster<'a, KVSNode>]> { None }
}

/// Basic deployment patterns used by dispatchers.
#[derive(Clone, Debug)]
pub enum Deployment<'a> {
    SingleCluster(Cluster<'a, KVSNode>),
    ShardedClusters(Vec<Cluster<'a, KVSNode>>),
}

impl<'a> KVSDeployment<'a> for Deployment<'a> {
    fn kvs_cluster(&self) -> &Cluster<'a, KVSNode> {
        match self { Deployment::SingleCluster(c) => c, Deployment::ShardedClusters(v) => &v[0] }
    }
    fn kvs_clusters(&self) -> Option<&[Cluster<'a, KVSNode>]> {
        match self { Deployment::ShardedClusters(v) => Some(v.as_slice()), _ => None }
    }
}

impl<'a> KVSDeployment<'a> for Cluster<'a, KVSNode> {
    fn kvs_cluster(&self) -> &Cluster<'a, KVSNode> { self }
}

/* ------------------------------------------------------------------------- */
/* Interception Core                                                          */
/* ------------------------------------------------------------------------- */

/// Interceptor transforming a process‑originated stream into a cluster stream.
pub trait OpIntercept<V> {
    type Deployment<'a>: KVSDeployment<'a>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a>;

    fn intercept_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
}

/// Identity (broadcast) interceptor placeholder.
#[derive(Clone, Debug, Default)]
pub struct IdentityIntercept;
impl IdentityIntercept { pub fn new() -> Self { Self } }

impl<V> OpIntercept<V> for IdentityIntercept {
    type Deployment<'a> = Cluster<'a, KVSNode>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        flow.cluster::<KVSNode>()
    }

    fn intercept_operations<'a>(
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

/// Static composition of two interceptors.
#[derive(Clone, Debug)]
pub struct Pipeline<A, B> { pub first: A, pub second: B }
impl<A, B> Pipeline<A, B> { pub fn new(first: A, second: B) -> Self { Self { first, second } } }
impl<A: Default, B: Default> Default for Pipeline<A, B> { fn default() -> Self { Self { first: A::default(), second: B::default() } } }

impl<A, B, V> OpIntercept<V> for Pipeline<A, B>
where
    A: OpIntercept<V>,
    B: OpIntercept<V>,
{
    type Deployment<'a> = A::Deployment<'a>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        self.first.create_deployment(flow)
    }

    fn intercept_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        self.first.intercept_operations(operations, deployment)
    }
}

/// Fluent extension for chaining.
pub trait OpInterceptExt<V>: OpIntercept<V> + Sized { fn then<N: OpIntercept<V>>(self, next: N) -> Pipeline<Self, N> { Pipeline::new(self, next) } }
impl<V, T: OpIntercept<V>> OpInterceptExt<V> for T {}

/* ------------------------------------------------------------------------- */
/* Tests (basic structural)                                                   */
/* ------------------------------------------------------------------------- */
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::routing::{RoundRobinRouter, ShardedRouter, SingleNodeRouter};

    #[test]
    fn identity_construction() {
        let _i = IdentityIntercept::new();
        let _d = IdentityIntercept::default();
    }

    #[test]
    fn pipeline_shape() {
        let p = Pipeline::new(IdentityIntercept::new(), SingleNodeRouter::new());
        let _ = format!("{:?}", p);
    }

    #[test]
    fn chaining_types() {
        fn _assert<V, T: OpInterceptExt<V>>(_: T) {}
        _assert::<String, _>(IdentityIntercept::new());
        _assert::<String, _>(SingleNodeRouter::new());
        _assert::<String, _>(RoundRobinRouter::new());
        _assert::<String, _>(ShardedRouter::new(3));
        let _nested = Pipeline::new(SingleNodeRouter::new(), RoundRobinRouter::new());
    }
}
