//! Cluster-level maintenance strategies
//!
//! This module contains maintenance strategies that coordinate data consistency
//! across multiple nodes in a cluster.

pub mod broadcast;
pub mod gossip;
pub mod logbased;

pub use broadcast::{BroadcastReplication, BroadcastReplicationConfig};
pub use gossip::{SimpleGossip, SimpleGossipConfig};
// Intentionally do not re-export LogBased here to steer users to after_storage::replication::LogBasedDelivery
