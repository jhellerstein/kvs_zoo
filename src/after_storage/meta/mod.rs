//! Legacy metadata surface retained for compatibility while the After pipeline migrates.
//!
//! Production metadata handling lives on `events::MetaEvent`; these `MetaMessage` types stick
//! around so earlier hook implementations still compile during the transition.

use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// Metadata describing a tombstone event carried on the metadata channel.
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

/// Metadata for a reclamation frontier carried on the metadata channel.
///
/// Represents a frontier up to which tombstones can be safely reclaimed.
/// Includes an epoch for distinguishing reclamation rounds.
///
/// XXX
/// This is a placeholder implementation--a scalar epoch value would only
/// work in a linearizable system; a vector clock or matrix clock
/// would be needed for a coordination-free system (e.g. causal, Lww).
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

/// Metadata message types dispatched through the After pipeline channel.
///
/// Metadata messages are replicated and handled independently from data replication,
/// enabling operations like tombstone reclamation or compaction hints without routing through the
/// main `KVSOperation` stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetaMessage {
    /// Tombstone metadata for a deleted key
    Tomb(TombMeta),
    /// Reclamation frontier metadata
    Reclaim(ReclaimMeta),
}

// Vector frontier (merged per-key VCs) built from background VC digests
pub mod vector_frontier;
pub use vector_frontier::{FrontierState, build_frontier, new_frontier_state};

impl MetaMessage {
    /// Create a new tomb metadata message.
    pub fn tomb(key: String) -> Self {
        MetaMessage::Tomb(TombMeta::new(key))
    }

    /// Create a new tomb metadata message with a sequence number.
    pub fn tomb_with_seq(key: String, seq: u64) -> Self {
        MetaMessage::Tomb(TombMeta::with_seq(key, seq))
    }

    /// Create a new reclaim metadata message.
    pub fn reclaim(epoch: u64) -> Self {
        MetaMessage::Reclaim(ReclaimMeta::new(epoch))
    }

    /// Create a new reclaim metadata message with a frontier sequence number.
    pub fn reclaim_with_frontier(frontier_seq: u64, epoch: u64) -> Self {
        MetaMessage::Reclaim(ReclaimMeta::with_frontier(frontier_seq, epoch))
    }
}

/// Trait for metadata message local handlers.
///
/// Implementers define how metadata messages are processed locally without network
/// transmission. Handlers can perform maintenance operations, scheduling, or
/// enrichment of metadata messages.
///
/// # Local Handling Flow
///
/// ```text
/// Metadata Messages (from replication or local)
///        ↓
///   [`MetaLocalHandler`]
///        ↓
///   Local Processing (no network)
///        ↓
///   Metadata Operations
/// ```
///
/// # Implementations
///
/// - **PassThroughMetaHandler**: Forwards messages unchanged
/// - **EnrichmentMetaHandler**: Adds metadata to messages
/// - Custom handlers: Implement tombstone reclamation, scheduling, etc.
pub trait MetaLocalHandler {
    /// Handle metadata messages locally.
    ///
    /// Takes a stream of metadata messages and returns a stream of processed messages.
    /// The handler can pass messages through unchanged, enrich them with metadata,
    /// or perform local maintenance operations.
    ///
    /// # Arguments
    /// - `cluster`: The cluster context for local operations
    /// - `meta_in`: Stream of metadata messages to handle
    ///
    /// # Returns
    /// Stream of processed metadata messages
    fn handle_meta<'a>(
        &self,
        cluster: &Cluster<'a, KVSNode>,
        meta_in: Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded>;
}

/// Default pass-through metadata local handler.
///
/// This handler forwards all metadata messages unchanged without any processing.
/// Useful as a no-op handler or as a base for custom handlers.
#[derive(Clone, Debug, Default)]
pub struct PassThroughMetaHandler;

impl MetaLocalHandler for PassThroughMetaHandler {
    fn handle_meta<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        meta_in: Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded> {
        meta_in
    }
}

/// Metadata local handler that enriches messages with metadata.
///
/// This handler adds annotations to metadata messages, such as timestamps or
/// processing information, without modifying the core message content.
#[derive(Clone, Debug, Default)]
pub struct EnrichmentMetaHandler;

impl MetaLocalHandler for EnrichmentMetaHandler {
    fn handle_meta<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        meta_in: Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded> {
        // For now, enrichment is a pass-through. In a full implementation,
        // this would add metadata like timestamps or processing information.
        meta_in
    }
}

/// Tombstone processing handler that tracks deleted keys.
///
/// This handler processes tomb metadata messages to maintain a set of deleted keys.
/// In a full implementation, this would be used to reclaim storage and prevent
/// resurrection of deleted values. The handler forwards all messages unchanged
/// while maintaining local state about tombstones.
#[derive(Clone, Debug, Default)]
pub struct TombTrackingHandler;

impl MetaLocalHandler for TombTrackingHandler {
    fn handle_meta<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        meta_in: Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<MetaMessage, Cluster<'a, KVSNode>, Unbounded> {
        // In a full implementation, this would:
        // 1. Track Tomb messages to maintain a set of deleted keys
        // 2. Process Reclaim messages to clean up old tombstones
        // 3. Coordinate with the storage layer to prevent resurrection
        //
        // For now, forward messages unchanged. The actual tombstone processing
        // happens at the storage layer via MapUnionWithTombstones.
        meta_in
    }
}

/// Operation splitting utilities for separating data and metadata streams.
///
/// These utilities enable extracting metadata messages from `KVSOperation` streams,
/// allowing operations to be routed to appropriate handlers (data replication vs
/// metadata replication).
///
/// # Operation Splitting Flow
///
/// ```text
/// KVSOperation Stream
///        ↓
///   [Splitting Logic]
///    ↙         ↘
/// Data Ops    Metadata Msgs
/// (Put/Get)   (Tomb/Reclaim)
///    ↓         ↓
/// Data Path   Metadata Path
/// ```
///
/// # Routing Rules
///
/// - **Put**: Routes to both data and metadata (produces Tomb)
/// - **Delete**: Routes to both data and metadata (produces Tomb)
/// - **Get**: Routes to data only (no metadata message)
pub mod splitting {
    use super::*;

    /// Extract metadata messages from a `KVSOperation`.
    ///
    /// Produces a tomb metadata message for Put and Delete operations,
    /// and None for Get operations (which don't affect state).
    ///
    /// # Arguments
    /// - `op`: The `KVSOperation` to extract metadata messages from
    ///
    /// # Returns
    /// `Some(MetaMessage)` if the operation produces a metadata message, `None` otherwise
    pub fn extract_meta_from_operation<V>(op: &KVSOperation<V>) -> Option<MetaMessage> {
        match op {
            KVSOperation::Put(key, _) => Some(MetaMessage::tomb(key.clone())),
            KVSOperation::Delete(key) => Some(MetaMessage::tomb(key.clone())),
            KVSOperation::Get(_) => None,
        }
    }

    /// Extract metadata messages from a `KVSOperation` with sequence number.
    ///
    /// Produces a tomb metadata message with sequence number for Put and Delete operations.
    /// This is useful for ordered tombstone reclamation.
    ///
    /// # Arguments
    /// - `op`: The `KVSOperation` to extract metadata messages from
    /// - `seq`: The sequence number to attach to the metadata message
    ///
    /// # Returns
    /// `Some(MetaMessage)` if the operation produces a metadata message, `None` otherwise
    pub fn extract_meta_with_seq<V>(op: &KVSOperation<V>, seq: u64) -> Option<MetaMessage> {
        match op {
            KVSOperation::Put(key, _) => Some(MetaMessage::tomb_with_seq(key.clone(), seq)),
            KVSOperation::Delete(key) => Some(MetaMessage::tomb_with_seq(key.clone(), seq)),
            KVSOperation::Get(_) => None,
        }
    }

    /// Extract the key from a KVSOperation if it's a data operation.
    ///
    /// Returns Some(key) for Put and Delete operations, None for Get operations.
    /// This is useful for routing operations to the data stream.
    ///
    /// # Arguments
    /// - `op`: The KVSOperation to extract the key from
    ///
    /// # Returns
    /// Some(key) if the operation is a data operation, None otherwise
    pub fn extract_key_from_operation<V>(op: &KVSOperation<V>) -> Option<String> {
        match op {
            KVSOperation::Put(key, _) => Some(key.clone()),
            KVSOperation::Delete(key) => Some(key.clone()),
            KVSOperation::Get(key) => Some(key.clone()),
        }
    }

    /// Check if a `KVSOperation` produces a metadata message.
    ///
    /// Returns true for Put and Delete operations, false for Get operations.
    ///
    /// # Arguments
    /// - `op`: The `KVSOperation` to check
    ///
    /// # Returns
    /// `true` if the operation produces a metadata message, `false` otherwise
    pub fn operation_produces_meta<V>(op: &KVSOperation<V>) -> bool {
        matches!(op, KVSOperation::Put(_, _) | KVSOperation::Delete(_))
    }

    /// Create a reclamation frontier metadata message.
    ///
    /// Produces a Reclaim maintenance message with the given frontier sequence number
    /// and epoch. This is used to signal that tombstones up to the frontier can
    /// be safely reclaimed.
    ///
    /// # Arguments
    /// - `frontier_seq`: The frontier sequence number
    /// - `epoch`: The reclamation epoch
    ///
    /// # Returns
    /// A reclaim metadata message
    pub fn create_reclaim_message(frontier_seq: u64, epoch: u64) -> MetaMessage {
        MetaMessage::reclaim_with_frontier(frontier_seq, epoch)
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
    fn test_meta_msg_tomb() {
        let msg = MetaMessage::tomb("key1".to_string());
        match msg {
            MetaMessage::Tomb(meta) => {
                assert_eq!(meta.key, "key1");
                assert_eq!(meta.seq, None);
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_meta_msg_tomb_with_seq() {
        let msg = MetaMessage::tomb_with_seq("key1".to_string(), 42);
        match msg {
            MetaMessage::Tomb(meta) => {
                assert_eq!(meta.key, "key1");
                assert_eq!(meta.seq, Some(42));
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_meta_msg_reclaim() {
        let msg = MetaMessage::reclaim(1);
        match msg {
            MetaMessage::Reclaim(meta) => {
                assert_eq!(meta.frontier_seq, None);
                assert_eq!(meta.epoch, 1);
            }
            _ => panic!("Expected Reclaim variant"),
        }
    }

    #[test]
    fn test_meta_msg_reclaim_with_frontier() {
        let msg = MetaMessage::reclaim_with_frontier(100, 1);
        match msg {
            MetaMessage::Reclaim(meta) => {
                assert_eq!(meta.frontier_seq, Some(100));
                assert_eq!(meta.epoch, 1);
            }
            _ => panic!("Expected Reclaim variant"),
        }
    }

    #[test]
    fn test_meta_msg_equality() {
        let msg1 = MetaMessage::tomb("key1".to_string());
        let msg2 = MetaMessage::tomb("key1".to_string());
        assert_eq!(msg1, msg2);

        let msg3 = MetaMessage::tomb("key2".to_string());
        assert_ne!(msg1, msg3);
    }

    #[test]
    fn test_meta_msg_serialization() {
        let msg = MetaMessage::tomb_with_seq("key1".to_string(), 42);
        let json = serde_json::to_string(&msg).expect("serialization failed");
        let deserialized: MetaMessage =
            serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_pass_through_handler_creation() {
        let _handler = PassThroughMetaHandler;
    }

    #[test]
    fn test_enrichment_handler_creation() {
        let _handler = EnrichmentMetaHandler;
    }

    #[test]
    fn test_tombstone_processing_handler_creation() {
        let _handler = TombTrackingHandler;
    }

    #[test]
    fn test_extract_meta_from_put_operation() {
        use crate::protocol::KVSOperation;
        use splitting::extract_meta_from_operation;

        let op: KVSOperation<String> = KVSOperation::Put("key1".to_string(), "value1".to_string());
        let meta = extract_meta_from_operation(&op);

        assert!(meta.is_some());
        match meta.unwrap() {
            MetaMessage::Tomb(meta) => {
                assert_eq!(meta.key, "key1");
                assert_eq!(meta.seq, None);
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_extract_meta_from_delete_operation() {
        use crate::protocol::KVSOperation;
        use splitting::extract_meta_from_operation;

        let op: KVSOperation<String> = KVSOperation::Delete("key2".to_string());
        let meta = extract_meta_from_operation(&op);

        assert!(meta.is_some());
        match meta.unwrap() {
            MetaMessage::Tomb(meta) => {
                assert_eq!(meta.key, "key2");
                assert_eq!(meta.seq, None);
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_extract_meta_from_get_operation() {
        use crate::protocol::KVSOperation;
        use splitting::extract_meta_from_operation;

        let op: KVSOperation<String> = KVSOperation::Get("key3".to_string());
        let meta = extract_meta_from_operation(&op);

        assert!(meta.is_none());
    }

    #[test]
    fn test_extract_meta_with_seq_put() {
        use crate::protocol::KVSOperation;
        use splitting::extract_meta_with_seq;

        let op: KVSOperation<String> = KVSOperation::Put("key1".to_string(), "value1".to_string());
        let meta = extract_meta_with_seq(&op, 42);

        assert!(meta.is_some());
        match meta.unwrap() {
            MetaMessage::Tomb(meta) => {
                assert_eq!(meta.key, "key1");
                assert_eq!(meta.seq, Some(42));
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_extract_meta_with_seq_delete() {
        use crate::protocol::KVSOperation;
        use splitting::extract_meta_with_seq;

        let op: KVSOperation<String> = KVSOperation::Delete("key2".to_string());
        let meta = extract_meta_with_seq(&op, 100);

        assert!(meta.is_some());
        match meta.unwrap() {
            MetaMessage::Tomb(meta) => {
                assert_eq!(meta.key, "key2");
                assert_eq!(meta.seq, Some(100));
            }
            _ => panic!("Expected Tomb variant"),
        }
    }

    #[test]
    fn test_extract_meta_with_seq_get() {
        use crate::protocol::KVSOperation;
        use splitting::extract_meta_with_seq;

        let op: KVSOperation<String> = KVSOperation::Get("key3".to_string());
        let meta = extract_meta_with_seq(&op, 50);

        assert!(meta.is_none());
    }

    #[test]
    fn test_extract_key_from_put_operation() {
        use crate::protocol::KVSOperation;
        use splitting::extract_key_from_operation;

        let op: KVSOperation<String> = KVSOperation::Put("key1".to_string(), "value1".to_string());
        let key = extract_key_from_operation(&op);

        assert_eq!(key, Some("key1".to_string()));
    }

    #[test]
    fn test_extract_key_from_delete_operation() {
        use crate::protocol::KVSOperation;
        use splitting::extract_key_from_operation;

        let op: KVSOperation<String> = KVSOperation::Delete("key2".to_string());
        let key = extract_key_from_operation(&op);

        assert_eq!(key, Some("key2".to_string()));
    }

    #[test]
    fn test_extract_key_from_get_operation() {
        use crate::protocol::KVSOperation;
        use splitting::extract_key_from_operation;

        let op: KVSOperation<String> = KVSOperation::Get("key3".to_string());
        let key = extract_key_from_operation(&op);

        assert_eq!(key, Some("key3".to_string()));
    }

    #[test]
    fn test_operation_produces_meta_put() {
        use crate::protocol::KVSOperation;
        use splitting::operation_produces_meta;

        let op: KVSOperation<String> = KVSOperation::Put("key1".to_string(), "value1".to_string());
        assert!(operation_produces_meta(&op));
    }

    #[test]
    fn test_operation_produces_meta_delete() {
        use crate::protocol::KVSOperation;
        use splitting::operation_produces_meta;

        let op: KVSOperation<String> = KVSOperation::Delete("key2".to_string());
        assert!(operation_produces_meta(&op));
    }

    #[test]
    fn test_operation_produces_meta_get() {
        use crate::protocol::KVSOperation;
        use splitting::operation_produces_meta;

        let op: KVSOperation<String> = KVSOperation::Get("key3".to_string());
        assert!(!operation_produces_meta(&op));
    }

    #[test]
    fn test_create_reclaim_message() {
        use splitting::create_reclaim_message;

        let msg = create_reclaim_message(100, 1);

        match msg {
            MetaMessage::Reclaim(meta) => {
                assert_eq!(meta.frontier_seq, Some(100));
                assert_eq!(meta.epoch, 1);
            }
            _ => panic!("Expected Reclaim variant"),
        }
    }
}
