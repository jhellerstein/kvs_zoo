use crate::core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use super::KVSRouter;

/// Local routing - broadcasts to all nodes (typically just one for local development)
///
/// This router is primarily used for:
/// - Single-node development and testing
/// - Simple broadcast scenarios where all nodes should receive all operations
///
/// Note: For true single-node scenarios, the "cluster" typically contains just one node.
pub struct LocalRouter;

impl<V> KVSRouter<V> for LocalRouter {
    fn route_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Broadcast all operations to all nodes in the cluster
        // For local development, this is typically just one node
        operations.broadcast_bincode(cluster, nondet!(/** broadcast to all nodes */))
    }
}
