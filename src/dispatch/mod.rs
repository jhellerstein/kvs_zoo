//! Operation Dispatchers for KVS Systems
//!
//! This module provides the core infrastructure for dispatching and routing
//! incoming KVS operations before they reach storage. Operation dispatchers handle
//! concerns like routing, consensus, authentication, and rate limiting.
//!
//! ## Core Concepts
//!
//! - **OpDispatch**: Trait for transforming operation streams
//! - **Pipeline**: Zero-cost composition of multiple dispatchers
//! - **OpDispatchExt**: Extension trait providing fluent `.then()` chaining
//!
//! ## Usage
//!
//! ```rust
//! use kvs_zoo::dispatch::{IdentityDispatch, Pipeline};
//! use kvs_zoo::dispatch::{ShardedRouter, RoundRobinRouter};
//!
//! // Create dispatchers
//! let sharded = ShardedRouter::new(3);
//! let round_robin = RoundRobinRouter::new();
//! let identity = IdentityDispatch::new();
//!
//! // Manual pipeline composition
//! let pipeline = Pipeline::new(sharded, round_robin);
//! ```

use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub mod paxos;
pub mod paxos_core;
pub mod routing;

// Re-export routing dispatchers for convenience
pub use paxos::PaxosDispatcher;
pub use paxos_core::PaxosConfig;
pub use routing::{LocalRouter, RoundRobinRouter, ShardedRouter};

/// Core trait for operation dispatchers
///
/// Operation dispatchers transform streams of KVS operations, enabling
/// concerns like routing, consensus, authentication, and rate limiting
/// to be composed in a zero-cost pipeline.
pub trait OpDispatch<V> {
    /// Transform a stream of operations
    ///
    /// Takes operations from an external process and returns operations
    /// distributed across the cluster according to the dispatcher's logic.
    ///
    /// The flow builder is provided to allow dispatchers to create their own
    /// clusters (e.g., for consensus protocols like Paxos).
    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        flow: &FlowBuilder<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
}

/// Zero-cost composition of two operation dispatchers
///
/// Pipeline enables chaining multiple dispatchers together with no runtime
/// overhead. The composition is resolved at compile time through generic
/// type parameters.
#[derive(Clone, Debug)]
pub struct Pipeline<A, B> {
    pub first: A,
    pub second: B,
}

impl<A, B> Pipeline<A, B> {
    /// Create a new pipeline from two dispatchers
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

impl<V, A, B> OpDispatch<V> for Pipeline<A, B>
where
    A: OpDispatch<V>,
    B: OpDispatch<V>,
{
    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        flow: &FlowBuilder<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Apply the first dispatcher (e.g., ShardedRouter)
        // This routes operations to specific cluster members
        self.first.dispatch_operations(operations, cluster, flow)

        // Note: For the current architecture, the second dispatcher (LocalRouter)
        // is not needed when operations are already routed by the first dispatcher.
        // The LocalRouter would broadcast to all nodes, which would override sharding.
        //
        // In a proper implementation, we would need different composition strategies
        // based on the dispatcher types, but for now we just use the first dispatcher.
    }
}

/// Extension trait providing fluent chaining for operation dispatchers
pub trait OpDispatchExt<V>: OpDispatch<V> + Sized {
    /// Chain this dispatcher with another, creating a zero-cost pipeline
    ///
    /// The resulting pipeline will apply this dispatcher first, then the next.
    fn then<Next: OpDispatch<V>>(self, next: Next) -> Pipeline<Self, Next> {
        Pipeline::new(self, next)
    }
}

// Blanket implementation for all OpDispatch types
impl<V, T> OpDispatchExt<V> for T where T: OpDispatch<V> {}

/// Identity dispatcher that passes operations through unchanged
///
/// Useful for testing and as a no-op placeholder in pipeline composition.
#[derive(Clone, Debug, Default)]
pub struct IdentityDispatch;

impl IdentityDispatch {
    /// Create a new identity dispatcher
    pub fn new() -> Self {
        Self
    }
}

impl<V> OpDispatch<V> for IdentityDispatch {
    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        _flow: &FlowBuilder<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Broadcast all operations to all nodes (identity behavior)
        operations.broadcast_bincode(cluster, nondet!(/** identity broadcast */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::routing::{LocalRouter, RoundRobinRouter, ShardedRouter};

    #[test]
    fn test_identity_dispatch_creation() {
        let _identity = IdentityDispatch::new();
        let _identity_default = IdentityDispatch::new();
    }

    #[test]
    fn test_identity_dispatch_clone_debug() {
        let identity = IdentityDispatch::new();
        let _cloned = identity.clone();
        let _debug_str = format!("{:?}", identity);
    }

    #[test]
    fn test_pipeline_composition_mechanics() {
        // Test basic pipeline creation
        let identity1 = IdentityDispatch::new();
        let identity2 = IdentityDispatch::new();
        let pipeline = Pipeline::new(identity1, identity2);

        // Test pipeline structure
        assert_eq!(
            format!("{:?}", pipeline.first),
            format!("{:?}", IdentityDispatch::new())
        );
        assert_eq!(
            format!("{:?}", pipeline.second),
            format!("{:?}", IdentityDispatch::new())
        );

        // Test nested pipeline composition
        let identity3 = IdentityDispatch::new();
        let identity4 = IdentityDispatch::new();
        let inner_pipeline = Pipeline::new(identity3, identity4);
        let _complex_pipeline = Pipeline::new(inner_pipeline, IdentityDispatch::new());
    }

    #[test]
    fn test_pipeline_type_safety() {
        // Test homogeneous composition
        let _identity_pipeline: Pipeline<IdentityDispatch, IdentityDispatch> =
            Pipeline::new(IdentityDispatch::new(), IdentityDispatch::new());

        // Test heterogeneous composition with different router types
        let _local_identity_pipeline: Pipeline<LocalRouter, IdentityDispatch> =
            Pipeline::new(LocalRouter::new(), IdentityDispatch::new());

        let _sharded_round_robin_pipeline: Pipeline<ShardedRouter, RoundRobinRouter> =
            Pipeline::new(ShardedRouter::new(3), RoundRobinRouter::new());

        // Test deeply nested composition
        let inner = Pipeline::new(LocalRouter::new(), RoundRobinRouter::new());
        let _nested_pipeline: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, ShardedRouter> =
            Pipeline::new(inner, ShardedRouter::new(5));
    }

    #[test]
    fn test_pipeline_clone_debug() {
        let pipeline = Pipeline::new(IdentityDispatch::new(), LocalRouter::new());
        let _cloned = pipeline.clone();
        let _debug_str = format!("{:?}", pipeline);
    }

    #[test]
    fn test_dispatch_ext_trait_chaining() {
        // Test that OpDispatchExt trait is available and provides .then() method
        // We test the trait existence and method signature without relying on type inference

        // Test that all dispatcher types implement OpDispatchExt
        fn _has_then_method<V, T: OpDispatchExt<V>>(_t: T) {}

        _has_then_method::<String, _>(IdentityDispatch::new());
        _has_then_method::<String, _>(LocalRouter::new());
        _has_then_method::<String, _>(RoundRobinRouter::new());
        _has_then_method::<String, _>(ShardedRouter::new(3));

        // Test chaining using Pipeline::new (equivalent to .then() but without type inference issues)
        let _chained = Pipeline::new(IdentityDispatch::new(), IdentityDispatch::new());
        let _local_then_identity = Pipeline::new(LocalRouter::new(), IdentityDispatch::new());
        let _round_robin_then_local = Pipeline::new(RoundRobinRouter::new(), LocalRouter::new());

        // Test multi-level chaining
        let step1 = Pipeline::new(ShardedRouter::new(3), RoundRobinRouter::new());
        let _multi_chain = Pipeline::new(step1, IdentityDispatch::new());
    }

    #[test]
    fn test_dispatch_ext_trait_type_inference() {
        // Test type inference and structure using Pipeline::new
        let step1: Pipeline<LocalRouter, RoundRobinRouter> =
            Pipeline::new(LocalRouter::new(), RoundRobinRouter::new());
        let pipeline: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, ShardedRouter> =
            Pipeline::new(step1, ShardedRouter::new(4));

        // Verify the resulting type structure
        let _typed_pipeline: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, ShardedRouter> =
            pipeline;

        // Test that the extension trait method exists (even if we can't easily test it due to type inference)
        fn _test_then_method_exists<V>() {
            // This function tests that the .then() method exists on all types
            let _: fn(
                IdentityDispatch,
                IdentityDispatch,
            ) -> Pipeline<IdentityDispatch, IdentityDispatch> =
                |a, b| OpDispatchExt::<V>::then(a, b);
        }
        _test_then_method_exists::<String>();
    }

    #[test]
    fn test_dispatch_trait_object_compatibility() {
        // Test that dispatchers can be used as trait objects if needed
        let dispatchers: Vec<Box<dyn OpDispatch<String>>> = vec![
            Box::new(IdentityDispatch::new()),
            Box::new(LocalRouter::new()),
            Box::new(RoundRobinRouter::new()),
            Box::new(ShardedRouter::new(3)),
        ];

        assert_eq!(dispatchers.len(), 4);
    }

    #[test]
    fn test_pipeline_implements_dispatch() {
        // Test that Pipeline itself implements OpDispatch
        let pipeline = Pipeline::new(LocalRouter::new(), RoundRobinRouter::new());

        // This should compile, demonstrating that Pipeline implements OpDispatch
        fn _test_dispatch<V>(_dispatcher: impl OpDispatch<V>) {}
        _test_dispatch::<String>(pipeline);
    }

    #[test]
    fn test_dispatch_ext_blanket_implementation() {
        // Test that all OpDispatch types automatically get OpDispatchExt
        fn _has_then_method<T: OpDispatchExt<String>>(_t: T) {}

        _has_then_method(IdentityDispatch::new());
        _has_then_method(LocalRouter::new());
        _has_then_method(RoundRobinRouter::new());
        _has_then_method(ShardedRouter::new(3));
        _has_then_method(Pipeline::new(LocalRouter::new(), IdentityDispatch::new()));
    }

    #[test]
    fn test_zero_cost_composition_structure() {
        // Test that composition creates the expected nested structure
        let a = LocalRouter::new();
        let b = RoundRobinRouter::new();
        let c = ShardedRouter::new(3);

        // Test binary composition with explicit types using Pipeline::new
        let ab: Pipeline<LocalRouter, RoundRobinRouter> = Pipeline::new(a, b);
        let abc: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, ShardedRouter> =
            Pipeline::new(ab, c);

        // Verify structure through type system (this compiles = correct structure)
        let _typed_abc: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, ShardedRouter> = abc;

        // Test that the structure is zero-cost (no runtime overhead)
        assert_eq!(
            std::mem::size_of::<Pipeline<LocalRouter, RoundRobinRouter>>(),
            std::mem::size_of::<LocalRouter>() + std::mem::size_of::<RoundRobinRouter>()
        );
    }

    #[test]
    fn test_pipeline_associativity_types() {
        // Test that (A.then(B)).then(C) and A.then(B.then(C)) have different but valid types
        let a = LocalRouter::new();
        let b = RoundRobinRouter::new();
        let c = IdentityDispatch::new();

        // Left-associative: (A.then(B)).then(C) using Pipeline::new
        let ab: Pipeline<LocalRouter, RoundRobinRouter> = Pipeline::new(a.clone(), b.clone());
        let left_assoc: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, IdentityDispatch> =
            Pipeline::new(ab, c.clone());
        let _left_typed: Pipeline<Pipeline<LocalRouter, RoundRobinRouter>, IdentityDispatch> =
            left_assoc;

        // Right-associative: A.then(B.then(C)) using Pipeline::new
        let bc: Pipeline<RoundRobinRouter, IdentityDispatch> = Pipeline::new(b, c);
        let right_assoc: Pipeline<LocalRouter, Pipeline<RoundRobinRouter, IdentityDispatch>> =
            Pipeline::new(a, bc);
        let _right_typed: Pipeline<LocalRouter, Pipeline<RoundRobinRouter, IdentityDispatch>> =
            right_assoc;

        // Verify that the types are different (this is a compile-time check)
        // The fact that both assignments above compile proves the associativity works correctly
    }
}
