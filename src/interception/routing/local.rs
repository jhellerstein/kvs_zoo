//! Local Router Operation Interceptor
//!
//! The LocalRouter is intended for usage over a single node.

use crate::core::KVSNode;
use crate::interception::OpIntercept;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// LocalRouter is the simplest routing strategy, sending every operation
/// to every node in the cluster. This is appropriate for:
/// - Single-node deployments (where the cluster contains just one node)
/// - Fully replicated systems where all nodes have all data
/// - Development and testing scenarios
/// - Simple broadcast scenarios where all nodes should receive all operations
///
/// ## Usage
///
/// ```rust
/// use kvs_zoo::interception::routing::LocalRouter;
/// use kvs_zoo::interception::OpIntercept;
///
/// let router = LocalRouter::new();
/// // Operations will be broadcast to all nodes in the cluster
/// ```
///
/// ## Behavior
///
/// For local development, the "cluster" typically contains just one node,
/// making this effectively a single-node router. In multi-node scenarios,
/// all nodes receive all operations, which is suitable for fully replicated
/// architectures.
#[derive(Clone, Debug, Default)]
pub struct LocalRouter;

impl LocalRouter {
    /// Create a new local router
    ///
    /// The router will broadcast all operations to all nodes in the cluster.
    /// For single-node deployments, this effectively routes to the single node.
    pub fn new() -> Self {
        Self
    }
}

impl<V> OpIntercept<V> for LocalRouter {
    fn intercept_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        _flow: &FlowBuilder<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Broadcast all operations to all nodes in the cluster
        // For local development, this is typically just one node
        operations.broadcast_bincode(cluster, nondet!(/** broadcast to all nodes */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interception::OpInterceptExt;

    #[test]
    fn test_local_router_creation() {
        let _router = LocalRouter::new();
        let _router_default = LocalRouter::new();
    }

    #[test]
    fn test_local_router_implements_interception() {
        let router = LocalRouter::new();

        // This should compile, demonstrating that LocalRouter implements OpIntercept
        fn _test_interception<V>(_interceptor: impl OpIntercept<V>) {}
        _test_interception::<String>(router);
    }

    #[test]
    fn test_local_router_implements_interception_ext() {
        let router = LocalRouter::new();

        // Test that LocalRouter implements OpInterceptExt for chaining
        fn _test_interception_ext<V>(_interceptor: impl OpInterceptExt<V>) {}
        _test_interception_ext::<String>(router);
    }

    #[test]
    fn test_local_router_clone_debug() {
        let router = LocalRouter::new();
        let _cloned = router.clone();
        let _debug_str = format!("{:?}", router);
    }

    #[test]
    fn test_local_router_default_trait() {
        let router1 = LocalRouter::new();
        let router2 = LocalRouter::new();

        // Both should be equivalent (both are empty structs)
        assert_eq!(format!("{:?}", router1), format!("{:?}", router2));
    }
}
