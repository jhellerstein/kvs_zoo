//! Control channel types and handlers for background maintenance operations.
//!
//! This module defines control messages (CtrlMsg) that can be replicated and handled
//! independently from data replication. Control messages enable operations like
//! tombstone reclamation and maintenance scheduling without routing through the
//! main KVSOperation stream.

use serde::{Deserialize, Serialize};

/// Metadata for a tombstone control message.
///
/// Represents a key that has been deleted and may be reclaimed after sufficient
/// replication. Includes an optional sequence number for ordering.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TombMeta {
    /// The key that was deleted
    pub key: String,
    /// Optional sequence number for ordering tombstones
    pub seq: Option<u64>,
}

impl TombMeta {
    /// Create a new tombstone metadata entry.
    pub fn new(key: String) -> Self {
        Self { key, seq: None }
    }

    /// Create a new tombstone metadata entry with a sequence number.
    pub fn with_seq(key: String, seq: u64) -> Self {
        Self {
            key,
            seq: Some(seq),
        }
    }
}

/// Metadata for a reclamation frontier control message.
///
/// Represents a frontier up to which tombstones can be safely reclaimed.
/// Includes an epoch for distinguishing reclamation rounds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReclaimMeta {
    /// The frontier sequence number up to which reclamation is safe
    pub frontier_seq: Option<u64>,
    /// Epoch identifier for this reclamation round
    pub epoch: u64,
}

impl ReclaimMeta {
    /// Create a new reclamation metadata entry.
    pub fn new(epoch: u64) -> Self {
        Self {
            frontier_seq: None,
            epoch,
        }
    }

    /// Create a new reclamation metadata entry with a frontier sequence number.
    pub fn with_frontier(frontier_seq: u64, epoch: u64) -> Self {
        Self {
            frontier_seq: Some(frontier_seq),
            epoch,
        }
    }
}

/// Control message types for background maintenance operations.
///
/// Control messages are replicated and handled independently from data replication,
/// enabling operations like tombstone reclamation without routing through the main
/// KVSOperation stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CtrlMsg {
    /// Tombstone metadata for a deleted key
    Tomb(TombMeta),
    /// Reclamation frontier metadata
    Reclaim(ReclaimMeta),
}

impl CtrlMsg {
    /// Create a new Tomb control message.
    pub fn tomb(key: String) -> Self {
        CtrlMsg::Tomb(TombMeta::new(key))
    }

    /// Create a new Tomb control message with a sequence number.
    pub fn tomb_with_seq(key: String, seq: u64) -> Self {
        CtrlMsg::Tomb(TombMeta::with_seq(key, seq))
    }

    /// Create a new Reclaim control message.
    pub fn reclaim(epoch: u64) -> Self {
        CtrlMsg::Reclaim(ReclaimMeta::new(epoch))
    }

    /// Create a new Reclaim control message with a frontier sequence number.
    pub fn reclaim_with_frontier(frontier_seq: u64, epoch: u64) -> Self {
        CtrlMsg::Reclaim(ReclaimMeta::with_frontier(frontier_seq, epoch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tomb_meta_creation() {
        let tomb = TombMeta::new("key1".to_string());
        assert_eq!(tomb.key, "key1");
        assert_eq!(tomb.seq, None);
    }

    #[test]
    fn test_tomb_meta_with_seq() {
        let tomb = TombMeta::with_seq("key1".to_string(), 42);
        assert_eq!(tomb.key, "key1");
        assert_eq!(tomb.seq, Some(42));
    }

    #[test]
    fn test_reclaim_meta_creation() {
        let reclaim = ReclaimMeta::new(1);
        assert_eq!(reclaim.frontier_seq, None);
        assert_eq!(reclaim.epoch, 1);
    }

    #[test]
    fn test_reclaim_meta_with_frontier() {
        let reclaim = ReclaimMeta::with_frontier(100, 1);
        assert_eq!(reclaim.frontier_seq, Some(100));
        assert_eq!(reclaim.epoch, 1);
    }

    #[test]
    fn test_ctrl_msg_tomb() {
        let msg = CtrlMsg::tomb("key1".to_string());
        match msg {
            CtrlMsg::Tomb(meta) => {
                assert_eq!(meta.key, "key1");
                assert_eq!(meta.seq, None);
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_ctrl_msg_tomb_with_seq() {
        let msg = CtrlMsg::tomb_with_seq("key1".to_string(), 42);
        match msg {
            CtrlMsg::Tomb(meta) => {
                assert_eq!(meta.key, "key1");
                assert_eq!(meta.seq, Some(42));
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_ctrl_msg_reclaim() {
        let msg = CtrlMsg::reclaim(1);
        match msg {
            CtrlMsg::Reclaim(meta) => {
                assert_eq!(meta.frontier_seq, None);
                assert_eq!(meta.epoch, 1);
            }
            _ => panic!("Expected Reclaim variant"),
        }
    }

    #[test]
    fn test_ctrl_msg_reclaim_with_frontier() {
        let msg = CtrlMsg::reclaim_with_frontier(100, 1);
        match msg {
            CtrlMsg::Reclaim(meta) => {
                assert_eq!(meta.frontier_seq, Some(100));
                assert_eq!(meta.epoch, 1);
            }
            _ => panic!("Expected Reclaim variant"),
        }
    }

    #[test]
    fn test_ctrl_msg_equality() {
        let msg1 = CtrlMsg::tomb("key1".to_string());
        let msg2 = CtrlMsg::tomb("key1".to_string());
        assert_eq!(msg1, msg2);

        let msg3 = CtrlMsg::tomb("key2".to_string());
        assert_ne!(msg1, msg3);
    }

    #[test]
    fn test_ctrl_msg_serialization() {
        let msg = CtrlMsg::tomb_with_seq("key1".to_string(), 42);
        let json = serde_json::to_string(&msg).expect("serialization failed");
        let deserialized: CtrlMsg =
            serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(msg, deserialized);
    }
}
