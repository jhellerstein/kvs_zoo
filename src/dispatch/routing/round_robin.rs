//! Round Robin Router Operation dispatcher
//!
//! The RoundRobinRouter distributes operations across cluster nodes using
//! round-robin load balancing, ensuring even distribution of work across
//! all available replicas.

use crate::dispatch::{Deployment, KVSDeployment, OpDispatch};
use crate::kvs_core::KVSNode;
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
/// use kvs_zoo::dispatch::routing::RoundRobinRouter;
/// use kvs_zoo::dispatch::OpDispatch;
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

impl<V> OpDispatch<V> for RoundRobinRouter {
    type Deployment<'a> = Deployment<'a>;

    fn create_deployment<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Deployment<'a> {
        Deployment::SingleCluster(flow.cluster::<KVSNode>())
    }

    fn dispatch_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        deployment: &Self::Deployment<'a>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let cluster = deployment.kvs_cluster();
        // Distribute operations round-robin across all replica nodes
        // This provides load balancing while ensuring all nodes can handle operations
        operations.round_robin_bincode(cluster, nondet!(/** distribute operations round robin */))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::OpDispatchExt;

    #[test]
    fn test_round_robin_router_creation() {
        let _router = RoundRobinRouter::new();
        let _router_default = RoundRobinRouter::new();
    }

    #[test]
    fn test_round_robin_router_implements_dispatch() {
        let router = RoundRobinRouter::new();

        // This should compile, demonstrating that RoundRobinRouter implements OpDispatch
        fn _test_dispatch<V>(_dispatcher: impl OpDispatch<V>) {}
        _test_dispatch::<String>(router);
    }

    #[test]
    fn test_round_robin_router_implements_dispatch_ext() {
        let router = RoundRobinRouter::new();

        // Test that RoundRobinRouter implements OpDispatchExt for chaining
        fn _test_dispatch_ext<V>(_dispatcher: impl OpDispatchExt<V>) {}
        _test_dispatch_ext::<String>(router);
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
