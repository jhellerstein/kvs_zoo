//! Last-Writer-Wins (LWW) value wrapper
//!
//! Provides last-writer-wins semantics using a Lamport timestamp to determine
//! which write is "last." The merge operation always keeps the value with the
//! higher timestamp, making it a proper lattice (commutative, associative,
//! idempotent).

use lattices::{IsBot, LatticeFrom, Merge};
use serde::{Deserialize, Serialize};

/// A proper LWW register: a (timestamp, value) pair where merge keeps the
/// higher timestamp. This is a valid lattice under the max-timestamp order.
///
/// The timestamp must be set by the writer. In a distributed system, use a
/// Lamport clock or HLC to ensure timestamps are unique and monotonically
/// increasing per writer.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, Hash)]
pub struct LwwWrapper<T> {
    pub timestamp: u64,
    pub value: T,
}

impl<T> LwwWrapper<T> {
    /// Create a new LWW wrapper with timestamp 0.
    pub fn new(value: T) -> Self {
        LwwWrapper { timestamp: 0, value }
    }

    /// Create a new LWW wrapper with an explicit timestamp.
    pub fn with_timestamp(value: T, timestamp: u64) -> Self {
        LwwWrapper { timestamp, value }
    }

    /// Get a reference to the wrapped value.
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Get a mutable reference to the wrapped value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Extract the wrapped value.
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: PartialEq> Merge<LwwWrapper<T>> for LwwWrapper<T> {
    fn merge(&mut self, other: LwwWrapper<T>) -> bool {
        // Keep the value with the higher timestamp. Ties go to `other`
        // to ensure idempotence (merging identical values is a no-op).
        if other.timestamp > self.timestamp {
            self.timestamp = other.timestamp;
            self.value = other.value;
            true
        } else {
            false
        }
    }
}

impl<T: Default> IsBot for LwwWrapper<T> {
    fn is_bot(&self) -> bool {
        self.timestamp == 0
    }
}

impl<T> From<T> for LwwWrapper<T> {
    fn from(value: T) -> Self {
        LwwWrapper::new(value)
    }
}

impl<T: std::fmt::Display> std::fmt::Display for LwwWrapper<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.value, self.timestamp)
    }
}

impl<T> LatticeFrom<LwwWrapper<T>> for LwwWrapper<T> {
    fn lattice_from(other: LwwWrapper<T>) -> Self {
        other
    }
}
