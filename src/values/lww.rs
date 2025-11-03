//! Last-Writer-Wins (LWW) value wrapper
//! 
//! Provides simple overwrite semantics where the most recent write always wins.
//! This is the simplest conflict resolution strategy but provides no guarantees
//! about which "write" is actually more recent in distributed systems.

use lattices::Merge;
use serde::{Deserialize, Serialize};

/// Wrapper type that implements last-writer-wins semantics via the Merge trait
/// 
/// This allows us to use the same Merge-based core for both conflict resolution
/// and simple overwrite semantics. The LWW wrapper always accepts the "other"
/// value during merge operations.
/// 
/// ## Warning
/// 
/// LWW is a non-deterministic Merge implementation in distributed systems!
/// The "last" writer depends on message ordering, which is not guaranteed
/// in asynchronous networks. Use with caution in production systems.
/// 
/// ## Usage
/// 
/// ```rust
/// use kvs_zoo::values::{LwwWrapper, Merge};
/// 
/// let mut lww1 = LwwWrapper::new("first".to_string());
/// let lww2 = LwwWrapper::new("second".to_string());
/// 
/// lww1.merge(lww2); // lww1 now contains "second"
/// assert_eq!(lww1.as_ref(), "second");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LwwWrapper<T>(pub T);

impl<T> LwwWrapper<T> {
    /// Create a new LWW wrapper around a value
    pub fn new(value: T) -> Self {
        LwwWrapper(value)
    }
    
    /// Get a reference to the wrapped value
    pub fn as_ref(&self) -> &T {
        &self.0
    }
    
    /// Get a mutable reference to the wrapped value
    pub fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
    
    /// Extract the wrapped value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: PartialEq> Merge<LwwWrapper<T>> for LwwWrapper<T> {
    fn merge(&mut self, other: LwwWrapper<T>) -> bool {
        // Always overwrite with the "other" value (last writer wins)
        let changed = self.0 != other.0;
        self.0 = other.0;
        changed
    }
}

impl<T> From<T> for LwwWrapper<T> {
    fn from(value: T) -> Self {
        LwwWrapper(value)
    }
}

impl<T> std::ops::Deref for LwwWrapper<T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for LwwWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: std::fmt::Display> std::fmt::Display for LwwWrapper<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}