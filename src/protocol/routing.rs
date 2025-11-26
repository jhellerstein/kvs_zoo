//! Generic routing and sequencing traits for messages.
//!
//! - `RoutingKey`: expose a stable byte slice used for placement (e.g., sharding).
//! - `HasSequence`: optionally expose a sequence/slot for strong ordering.

/// Return a stable byte view used for routing decisions (e.g., sharding).
///
/// Implementations should return only the intended routing key bytes (e.g.,
/// the key string), not the entire message, to avoid accidental re-sharding on
/// value changes.
pub trait RoutingKey {
    fn routing_key(&self) -> &[u8];
}

/// Optionally expose a sequence number (e.g., Paxos slot) for strong ordering.
pub trait HasSequence {
    type Seq: Copy + Ord;
    fn sequence(&self) -> Option<Self::Seq>;
}

// -----------------------------------------------------------------------------
// Impl for core KVS message types
// -----------------------------------------------------------------------------

use crate::protocol::KVSOperation;

impl<K, V> RoutingKey for KVSOperation<K, V>
where
    K: AsRef<[u8]>,
{
    fn routing_key(&self) -> &[u8] {
        match self {
            KVSOperation::Put(k, _, _, _) | KVSOperation::Get(k, _, _) | KVSOperation::Delete(k, _, _) => {
                k.as_ref()
            }
        }
    }
}

impl<K, V> HasSequence for KVSOperation<K, V> {
    type Seq = usize;
    fn sequence(&self) -> Option<Self::Seq> {
        None
    }
}
