use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use super::KVSRouter;

/// Round-robin routing - distributes operations via round-robin across all nodes
/// 
/// This router distributes operations evenly across all nodes in the cluster:
/// - Operations are distributed round-robin for load balancing
/// - All nodes are capable of handling any operation
/// - No assumptions about replication or consistency (handled by storage layer)
/// 
/// Commonly used with:
/// - Replicated architectures (where all nodes have the same data)
/// - Load balancing scenarios
/// - Any architecture where operations can go to any node
pub struct RoundRobinRouter;

impl<V> KVSRouter<V> for RoundRobinRouter {
    fn route_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Distribute operations round-robin across all replica nodes
        // This provides load balancing while ensuring all nodes can handle operations
        operations.round_robin_bincode(cluster, nondet!(/** distribute operations round robin */))
    }
}