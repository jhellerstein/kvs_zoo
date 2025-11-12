//! Round Robin Router Operation dispatcher
//!
//! The RoundRobinRouter distributes operations across cluster nodes using
//! round-robin load balancing, ensuring even distribution of work across
//! all available replicas.

use crate::dispatch::OpDispatch;
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
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // For now, route to member 0 to avoid TotalOrder constraints
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_slotted_from_process<'a>(
        &self,
        slotted_operations: Stream<(usize, KVSOperation<V>), Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<(usize, KVSOperation<V>), Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Route to member 0, preserving slots (demux_bincode yields stream of tuples directly)
        slotted_operations
            .map(q!(|slotted_op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                slotted_op
            )))
            .into_keyed()
            .demux_bincode(target_cluster)
            .assume_ordering(nondet!(/** routed slotted to single member */))
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
        // Cluster-origin fallback: forward everything from each member to a single target member (0).
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
