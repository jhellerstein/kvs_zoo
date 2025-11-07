//! Routing Operation Interceptors
//!
//! This module provides operation interceptors that handle routing of KVS operations
//! to appropriate nodes in the cluster. Routing interceptors determine how operations
//! are distributed across nodes based on different strategies.
//!
//! ## Available Routers
//!
//! - **SingleNodeRouter**: Routes all operations to a single node (member 0). Formerly LocalRouter.
//! - **RoundRobinRouter**: Distributes operations across replicas using round-robin
//! - **ShardedRouter**: Partitions operations by key hash for horizontal scaling
//!
//! ## Usage
//!
//! ```rust
//! use kvs_zoo::interception::routing::{SingleNodeRouter, RoundRobinRouter, ShardedRouter};
//! use kvs_zoo::interception::OpInterceptExt;
//!
//! // Simple single-node routing
//! let local = SingleNodeRouter::new();
//!
//! // Load balancing across replicas
//! let round_robin = RoundRobinRouter::new();
//!
//! // Sharded distribution
//! let sharded = ShardedRouter::new(3);
//!
//! // Manual pipeline composition
//! let pipeline = kvs_zoo::interception::Pipeline::new(sharded, round_robin);
//! ```

// Module declarations for routing interceptors
pub mod round_robin;
pub mod sharded;
pub mod single_node;

// Re-export routing interceptor types for convenience
pub use single_node::SingleNodeRouter;
pub use round_robin::RoundRobinRouter;
pub use sharded::ShardedRouter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interception::{OpInterceptExt, Pipeline};

    #[test]
    fn test_router_composition_with_pipeline() {
        // Test that routers can be composed using Pipeline
        let local = SingleNodeRouter::new();
        let round_robin = RoundRobinRouter::new();
        let sharded = ShardedRouter::new(3);

        // Test binary composition
        let _local_then_round_robin: Pipeline<SingleNodeRouter, RoundRobinRouter> =
            Pipeline::new(local.clone(), round_robin.clone());

        let _sharded_then_round_robin: Pipeline<ShardedRouter, RoundRobinRouter> =
            Pipeline::new(sharded.clone(), round_robin.clone());

        let _round_robin_then_local: Pipeline<RoundRobinRouter, SingleNodeRouter> =
            Pipeline::new(round_robin, local);
    }

    #[test]
    fn test_router_composition_type_safety() {
        // Test that composition creates the expected nested types
        let sharded = ShardedRouter::new(5);
        let round_robin = RoundRobinRouter::new();
        let local = SingleNodeRouter::new();

        // Test two-level composition
        let sharded_round_robin: Pipeline<ShardedRouter, RoundRobinRouter> =
            Pipeline::new(sharded, round_robin);

        // Test three-level composition
        let complex_pipeline: Pipeline<Pipeline<ShardedRouter, RoundRobinRouter>, SingleNodeRouter> =
            Pipeline::new(sharded_round_robin, local);

        // Verify the type structure is correct (this compiles = correct types)
        let _typed_pipeline: Pipeline<Pipeline<ShardedRouter, RoundRobinRouter>, SingleNodeRouter> =
            complex_pipeline;
    }

    #[test]
    fn test_router_composition_symmetric_patterns() {
        // Test symmetric composition patterns as described in the design

        // Pattern: ShardedRouter.then(RoundRobinRouter)
        // This matches ShardedKVSServer<ReplicatedKVSServer<V>>
        let _sharded_then_replicated: Pipeline<ShardedRouter, RoundRobinRouter> =
            Pipeline::new(ShardedRouter::new(3), RoundRobinRouter::new());

        // Pattern: LocalRouter (single interceptor)
        // This matches LocalKVSServer<V>
        let _local_only = SingleNodeRouter::new();

        // Pattern: RoundRobinRouter (single interceptor)
        // This matches ReplicatedKVSServer<V>
        let _replicated_only = RoundRobinRouter::new();
    }

    #[test]
    fn test_all_routers_implement_required_traits() {
        // Test that all routers implement the required traits for composition

        fn _test_traits<V>(_router: impl OpInterceptExt<V> + Clone + std::fmt::Debug) {}

        _test_traits::<String>(SingleNodeRouter::new());
        _test_traits::<String>(RoundRobinRouter::new());
        _test_traits::<String>(ShardedRouter::new(3));
    }

    #[test]
    fn test_router_composition_with_different_orderings() {
        // Test different ordering possibilities as mentioned in requirements
        let sharded = ShardedRouter::new(3);
        let round_robin = RoundRobinRouter::new();
        let local = SingleNodeRouter::new();

        // Different valid orderings
        let _order1: Pipeline<ShardedRouter, RoundRobinRouter> =
            Pipeline::new(sharded.clone(), round_robin.clone());
        let _order2: Pipeline<RoundRobinRouter, ShardedRouter> =
            Pipeline::new(round_robin.clone(), sharded.clone());
        let _order3: Pipeline<SingleNodeRouter, ShardedRouter> = Pipeline::new(local.clone(), sharded);
        let _order4: Pipeline<RoundRobinRouter, SingleNodeRouter> = Pipeline::new(round_robin, local);

        // All should compile, demonstrating flexible ordering
    }
}
