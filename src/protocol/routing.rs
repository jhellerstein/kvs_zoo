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

use crate::protocol::{Envelope, KVSOperation};

impl<V> RoutingKey for KVSOperation<V> {
    fn routing_key(&self) -> &[u8] {
        match self {
            KVSOperation::Put(k, _) | KVSOperation::Get(k) => k.as_bytes(),
        }
    }
}

impl<V> HasSequence for KVSOperation<V> {
    type Seq = usize;
    fn sequence(&self) -> Option<Self::Seq> { None }
}

impl<Meta, V> RoutingKey for Envelope<Meta, KVSOperation<V>> {
    fn routing_key(&self) -> &[u8] { self.operation.routing_key() }
}

impl<V> HasSequence for Envelope<usize, KVSOperation<V>> {
    type Seq = usize;
    fn sequence(&self) -> Option<Self::Seq> { Some(self.metadata) }
}
