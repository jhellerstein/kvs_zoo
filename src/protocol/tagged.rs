//! Tagged operations for response handling
//!
//! When operations flow through multiple layers (cluster dispatch → replication → leaf processing),
//! we need to track which operations should generate client responses (local operations from the proxy)
//! versus which should not (replicated operations from other cluster members).
//!
//! `TaggedOperation` wraps an operation with a `should_respond` flag that flows through the entire
//! pipeline, including slot enforcement and other ordering mechanisms.

use serde::{Deserialize, Serialize};

/// An operation tagged with response metadata.
///
/// The `should_respond` flag indicates whether this operation should generate a response
/// back to the client. Typically:
/// - `true` for operations originating from the local proxy (client requests)
/// - `false` for operations replicated from other cluster members
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaggedOperation<Op> {
    /// Whether this operation should generate a client response
    pub should_respond: bool,
    /// The operation itself
    pub operation: Op,
}

impl<Op> TaggedOperation<Op> {
    /// Create a new tagged operation
    pub fn new(should_respond: bool, operation: Op) -> Self {
        Self {
            should_respond,
            operation,
        }
    }

    /// Create a local operation (should respond)
    pub fn local(operation: Op) -> Self {
        Self::new(true, operation)
    }

    /// Create a replicated operation (should not respond)
    pub fn replicated(operation: Op) -> Self {
        Self::new(false, operation)
    }

    /// Map the operation while preserving the tag
    pub fn map_operation<Op2>(self, f: impl FnOnce(Op) -> Op2) -> TaggedOperation<Op2> {
        TaggedOperation {
            should_respond: self.should_respond,
            operation: f(self.operation),
        }
    }

    /// Convert into components
    pub fn into_parts(self) -> (bool, Op) {
        (self.should_respond, self.operation)
    }

    /// Convert from tuple format (for backward compatibility with existing code)
    pub fn from_tuple(tuple: (bool, Op)) -> Self {
        Self::new(tuple.0, tuple.1)
    }

    /// Convert to tuple format (for processing APIs that expect tuples)
    pub fn into_tuple(self) -> (bool, Op) {
        (self.should_respond, self.operation)
    }
}

/// Type alias for slotted, tagged operations
pub type SlottedTaggedOperation<Op> = TaggedOperation<(usize, Op)>;

impl<Op> SlottedTaggedOperation<Op> {
    /// Extract the slot number
    pub fn slot(&self) -> usize {
        self.operation.0
    }

    /// Extract a reference to the inner operation
    pub fn inner_operation(&self) -> &Op {
        &self.operation.1
    }

    /// Strip the slot, keeping just the tagged operation
    pub fn strip_slot(self) -> TaggedOperation<Op> {
        TaggedOperation {
            should_respond: self.should_respond,
            operation: self.operation.1,
        }
    }
}
