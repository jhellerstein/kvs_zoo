//! Declarative cluster specification
//!
//! This module provides a clean, declarative API for specifying KVS cluster
//! topologies without ambiguity about node counts, sharding, or replication.
//!
//! ## Design
//!
//! Instead of imperative builder methods like `.with_nodes(9)` on a sharded+replicated
//! config (does that mean 9 total nodes? 9 per shard? 9 shards?), we use:
//!
//! ```ignore
//! KVSCluster {
//!     members: ClusterMembers {
//!         count: 3,           // 3 shards
//!         each: KVSNode {
//!             members: NodeMembers {
//!                 count: 3    // 3 replicas per shard
//!             }
//!         }
//!     }
//! }
//! // = 9 total nodes (3 shards × 3 replicas)
//! ```

use serde::{Deserialize, Serialize};

/// Top-level cluster specification
///
/// Describes the complete topology: how many shards/groups, and what each contains.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSCluster {
    pub members: ClusterMembers,
}

/// Members of a cluster (shards or groups)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterMembers {
    /// Number of shards/groups
    pub count: usize,
    /// Configuration for each shard/group
    pub each: KVSNode,
}

/// Node-level specification
///
/// Describes a single shard/group: how many replicas it contains.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSNode {
    pub members: NodeMembers,
}

/// Members within a single shard/group (replicas)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeMembers {
    /// Number of replicas in this shard/group
    pub count: usize,
}

impl KVSCluster {
    /// Create a simple single-node cluster (no sharding, no replication)
    pub fn single_node() -> Self {
        Self {
            members: ClusterMembers {
                count: 1,
                each: KVSNode {
                    members: NodeMembers { count: 1 },
                },
            },
        }
    }

    /// Create a replicated cluster (no sharding, N replicas)
    pub fn replicated(replica_count: usize) -> Self {
        Self {
            members: ClusterMembers {
                count: 1,
                each: KVSNode {
                    members: NodeMembers { count: replica_count },
                },
            },
        }
    }

    /// Create a sharded cluster (N shards, 1 node per shard)
    pub fn sharded(shard_count: usize) -> Self {
        Self {
            members: ClusterMembers {
                count: shard_count,
                each: KVSNode {
                    members: NodeMembers { count: 1 },
                },
            },
        }
    }

    /// Create a sharded + replicated cluster (N shards × M replicas)
    pub fn sharded_replicated(shard_count: usize, replicas_per_shard: usize) -> Self {
        Self {
            members: ClusterMembers {
                count: shard_count,
                each: KVSNode {
                    members: NodeMembers { count: replicas_per_shard },
                },
            },
        }
    }

    /// Total number of nodes in the cluster
    pub fn total_nodes(&self) -> usize {
        self.members.count * self.members.each.members.count
    }

    /// Number of shards
    pub fn shard_count(&self) -> usize {
        self.members.count
    }

    /// Number of replicas per shard
    pub fn replicas_per_shard(&self) -> usize {
        self.members.each.members.count
    }

    /// Is this a sharded configuration?
    pub fn is_sharded(&self) -> bool {
        self.members.count > 1
    }

    /// Is this a replicated configuration?
    pub fn is_replicated(&self) -> bool {
        self.members.each.members.count > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_spec() {
        let spec = KVSCluster::single_node();
        assert_eq!(spec.total_nodes(), 1);
        assert_eq!(spec.shard_count(), 1);
        assert_eq!(spec.replicas_per_shard(), 1);
        assert!(!spec.is_sharded());
        assert!(!spec.is_replicated());
    }

    #[test]
    fn replicated_spec() {
        let spec = KVSCluster::replicated(3);
        assert_eq!(spec.total_nodes(), 3);
        assert_eq!(spec.shard_count(), 1);
        assert_eq!(spec.replicas_per_shard(), 3);
        assert!(!spec.is_sharded());
        assert!(spec.is_replicated());
    }

    #[test]
    fn sharded_spec() {
        let spec = KVSCluster::sharded(3);
        assert_eq!(spec.total_nodes(), 3);
        assert_eq!(spec.shard_count(), 3);
        assert_eq!(spec.replicas_per_shard(), 1);
        assert!(spec.is_sharded());
        assert!(!spec.is_replicated());
    }

    #[test]
    fn sharded_replicated_spec() {
        let spec = KVSCluster::sharded_replicated(3, 3);
        assert_eq!(spec.total_nodes(), 9);
        assert_eq!(spec.shard_count(), 3);
        assert_eq!(spec.replicas_per_shard(), 3);
        assert!(spec.is_sharded());
        assert!(spec.is_replicated());
    }

    #[test]
    fn manual_construction() {
        let spec = KVSCluster {
            members: ClusterMembers {
                count: 2,
                each: KVSNode {
                    members: NodeMembers { count: 4 },
                },
            },
        };
        assert_eq!(spec.total_nodes(), 8); // 2 shards × 4 replicas
    }
}
