//! Cluster specification interpreter
//!
//! Converts declarative cluster specs into concrete dispatch strategies.

use crate::cluster_spec::KVSCluster;
use crate::dispatch::{Pipeline, RoundRobinRouter, ShardedRouter, SingleNodeRouter};

/// Infer the appropriate dispatch strategy from a cluster specification
///
/// This is a compile-time decision based on the topology:
/// - Single node → SingleNodeRouter
/// - Replicated (1 shard, N replicas) → RoundRobinRouter  
/// - Sharded (N shards, 1 replica each) → ShardedRouter
/// - Sharded + Replicated (N shards, M replicas) → Pipeline<ShardedRouter, RoundRobinRouter>
pub trait InferDispatch {
    /// The dispatch strategy type for this configuration
    type Dispatch;
    
    /// Create the dispatch strategy from the spec
    fn create_dispatch(&self) -> Self::Dispatch;
}

impl InferDispatch for KVSCluster {
    type Dispatch = DispatchStrategy;
    
    fn create_dispatch(&self) -> Self::Dispatch {
        match (self.is_sharded(), self.is_replicated()) {
            (false, false) => DispatchStrategy::SingleNode(SingleNodeRouter::new()),
            (false, true) => DispatchStrategy::RoundRobin(RoundRobinRouter::new()),
            (true, false) => DispatchStrategy::Sharded(ShardedRouter::new(self.shard_count())),
            (true, true) => DispatchStrategy::ShardedReplicated(Pipeline::new(
                ShardedRouter::new(self.shard_count()),
                RoundRobinRouter::new(),
            )),
        }
    }
}

/// Runtime dispatch strategy enum
///
/// Allows dynamic selection of dispatch based on cluster spec at runtime.
/// For most use cases, prefer static typing with concrete dispatch types.
#[derive(Clone, Debug)]
pub enum DispatchStrategy {
    SingleNode(SingleNodeRouter),
    RoundRobin(RoundRobinRouter),
    Sharded(ShardedRouter),
    ShardedReplicated(Pipeline<ShardedRouter, RoundRobinRouter>),
}

impl DispatchStrategy {
    /// Create from a cluster specification
    pub fn from_spec(spec: &KVSCluster) -> Self {
        spec.create_dispatch()
    }
    
    /// Get the total number of nodes required
    pub fn node_count(&self, spec: &KVSCluster) -> usize {
        spec.total_nodes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_single_node() {
        let spec = KVSCluster::single_node();
        let dispatch = spec.create_dispatch();
        matches!(dispatch, DispatchStrategy::SingleNode(_));
    }

    #[test]
    fn infer_round_robin() {
        let spec = KVSCluster::replicated(3);
        let dispatch = spec.create_dispatch();
        matches!(dispatch, DispatchStrategy::RoundRobin(_));
    }

    #[test]
    fn infer_sharded() {
        let spec = KVSCluster::sharded(3);
        let dispatch = spec.create_dispatch();
        matches!(dispatch, DispatchStrategy::Sharded(_));
    }

    #[test]
    fn infer_sharded_replicated() {
        let spec = KVSCluster::sharded_replicated(3, 3);
        let dispatch = spec.create_dispatch();
        matches!(dispatch, DispatchStrategy::ShardedReplicated(_));
    }
}
