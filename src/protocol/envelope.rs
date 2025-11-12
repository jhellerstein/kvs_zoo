//! Envelope type for wrapping operations with metadata
//!
//! The Envelope pattern allows operations to carry metadata (like slot numbers,
//! timestamps, or causality info) through dispatch and replication while keeping
//! routers and maintenance strategies generic.

use serde::{Deserialize, Serialize};

/// Trait for types that contain a KVS operation
///
/// This allows routers to work generically over bare operations or enveloped operations.
pub trait HasOperation<V> {
    /// Extract the operation for routing decisions
    fn operation(&self) -> &crate::protocol::KVSOperation<V>;
}

// Bare operations implement HasOperation trivially
impl<V> HasOperation<V> for crate::protocol::KVSOperation<V> {
    fn operation(&self) -> &crate::protocol::KVSOperation<V> {
        self
    }
}

/// Generic envelope wrapping an operation with metadata
///
/// This allows routers to dispatch based on the operation content while
/// preserving associated metadata through the routing process.
///
/// # Type Parameters
/// - `Meta`: Metadata type (e.g., `usize` for slots, `VectorClock` for causality)
/// - `Op`: The wrapped operation type
///
/// # Examples
/// ```
/// use kvs_zoo::protocol::{Envelope, KVSOperation};
///
/// // Slot-numbered operation
/// let slotted: Envelope<usize, KVSOperation<String>> =
///     Envelope::new(42, KVSOperation::Put("key".into(), "value".into()));
///
/// // Extract operation for routing
/// let op = slotted.operation();
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope<Meta, Op> {
    pub metadata: Meta,
    pub operation: Op,
}

impl<Meta, Op> Envelope<Meta, Op> {
    pub fn new(metadata: Meta, operation: Op) -> Self {
        Self {
            metadata,
            operation,
        }
    }

    /// Get a reference to the wrapped operation
    pub fn operation(&self) -> &Op {
        &self.operation
    }

    /// Get a reference to the metadata
    pub fn metadata(&self) -> &Meta {
        &self.metadata
    }

    /// Destructure into metadata and operation
    pub fn into_parts(self) -> (Meta, Op) {
        (self.metadata, self.operation)
    }

    /// Map the operation while preserving metadata
    pub fn map_operation<Op2, F>(self, f: F) -> Envelope<Meta, Op2>
    where
        F: FnOnce(Op) -> Op2,
    {
        Envelope {
            metadata: self.metadata,
            operation: f(self.operation),
        }
    }

    /// Map the metadata while preserving the operation
    pub fn map_metadata<Meta2, F>(self, f: F) -> Envelope<Meta2, Op>
    where
        F: FnOnce(Meta) -> Meta2,
    {
        Envelope {
            metadata: f(self.metadata),
            operation: self.operation,
        }
    }
}

// Envelopes implement HasOperation by delegating to the wrapped operation
impl<Meta, V> HasOperation<V> for Envelope<Meta, crate::protocol::KVSOperation<V>> {
    fn operation(&self) -> &crate::protocol::KVSOperation<V> {
        &self.operation
    }
}

/// Type alias for slot-numbered operations (most common use case)
pub type SlottedOperation<V> = Envelope<usize, crate::protocol::KVSOperation<V>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::KVSOperation;

    #[test]
    fn envelope_construction() {
        let env = Envelope::new(42usize, KVSOperation::Get::<String>("key".into()));
        assert_eq!(env.metadata, 42);
        assert_eq!(env.operation, KVSOperation::Get("key".into()));
    }

    #[test]
    fn envelope_destructure() {
    let env = Envelope::new(10usize, KVSOperation::Put::<String>("k".into(), "v".into()));
        let (slot, op) = env.into_parts();
        assert_eq!(slot, 10);
    assert_eq!(op, KVSOperation::Put::<String>("k".into(), "v".into()));
    }

    #[test]
    fn envelope_map() {
        let env = Envelope::new(5usize, KVSOperation::Get::<String>("key".into()));
        let mapped = env.map_metadata(|s| s * 2);
        assert_eq!(mapped.metadata, 10);
    }
}
