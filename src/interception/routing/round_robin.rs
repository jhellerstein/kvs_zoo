//! Round Robin Router Operation Interceptor
//!
//! The RoundRobinRouter distributes operations across cluster nodes using
//! round-robin load balancing, ensuring even distribution of work across
//! all available replicas.

use crate::core::KVSNode;
use crate::interception::OpIntercept;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Router that distributes operations using round-robin load balancing
///
/// RoundRobinRouter cycles through cluster nodes, sending each operation
/// to the next node in sequence. This provides even load distribution
/// across replicas and is suitable for:
/// - Replicated systems where any node can handle any operation
/// - Load balancing across identical replicas
/// - Systems where operation order is not critical
///
/// ## Usage
///
/// ```rust
/// use kvs_zoo::interception::routing::RoundRobinRouter;
/// use kvs_zoo::interception::OpIntercept;
///
/// let router = RoundRobinRouter::new();
/// // Operations will be distributed round-robin across all replica nodes
/// ```
///
/// ## Behavior
///
/// The router uses Hydro's built-in `round_robin_bincode` method to distribute
/// operations evenly across all nodes in the cluster. This ensures load balancing
/// while maintaining the streaming nature of the operation processing.
#[derive(Clone, Debug, Default)]
pub struct RoundRobinRouter;

impl RoundRobinRouter {
    /// Create a new round-robin router
    ///
    /// The router will distribute operations round-robin across all nodes
    /// in the cluster for load balancing.
    pub fn new() -> Self {
        Self
    }
}

impl<V> OpIntercept<V> for RoundRobinRouter {
    fn intercept_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
        _flow: &FlowBuilder<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Distribute operations round-robin across all replica nodes
        // This provides load balancing while ensuring all nodes can handle operations
        operations.round_robin_bincode(cluster, nondet!(/** distribute operations round robin */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interception::OpInterceptExt;

    #[test]
    fn test_round_robin_router_creation() {
        let _router = RoundRobinRouter::new();
        let _router_default = RoundRobinRouter::new();
    }

    #[test]
    fn test_round_robin_router_implements_interception() {
        let router = RoundRobinRouter::new();

        // This should compile, demonstrating that RoundRobinRouter implements OpIntercept
        fn _test_interception<V>(_interceptor: impl OpIntercept<V>) {}
        _test_interception::<String>(router);
    }

    #[test]
    fn test_round_robin_router_implements_interception_ext() {
        let router = RoundRobinRouter::new();

        // Test that RoundRobinRouter implements OpInterceptExt for chaining
        fn _test_interception_ext<V>(_interceptor: impl OpInterceptExt<V>) {}
        _test_interception_ext::<String>(router);
    }

    #[test]
    fn test_round_robin_router_clone_debug() {
        let router = RoundRobinRouter::new();
        let _cloned = router.clone();
        let _debug_str = format!("{:?}", router);
    }

    #[test]
    fn test_round_robin_router_default_trait() {
        let router1 = RoundRobinRouter::new();
        let router2 = RoundRobinRouter::new();

        // Both should be equivalent (both are empty structs)
        assert_eq!(format!("{:?}", router1), format!("{:?}", router2));
    }
}
