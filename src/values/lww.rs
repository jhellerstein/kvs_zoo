//! Last-Writer-Wins (LWW) value wrapper
//!
//! Provides simple overwrite semantics where the most recent write always wins.
//! This is the simplest conflict resolution strategy but provides no guarantees
//! about which "write" is actually more recent in distributed systems.
//!
//! **This is NOT a lattice.** The merge operation is not commutative: the
//! "other" argument always wins, so `a.merge(b)` ≠ `b.merge(a)`. In a
//! distributed system with concurrent writes, the result depends on message
//! arrival order — which is non-deterministic.
//!
//! For a proper lattice-based alternative, use [`CausalWrapper`](super::CausalWrapper)
//! which wraps values with vector clocks to provide causal consistency.

use lattices::{IsBot, LatticeFrom, Merge};
use serde::{Deserialize, Serialize};

/// Wrapper type that implements last-writer-wins semantics via the Merge trait.
///
/// The merge always accepts the "other" value, making it suitable for
/// single-node or totally-ordered (e.g. Paxos-sequenced) settings where
/// "last" is well-defined. In unordered replicated settings, this is
/// non-deterministic — use [`CausalWrapper`](super::CausalWrapper) instead.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, Hash)]
pub struct LwwWrapper<T>(pub T);

impl<T> LwwWrapper<T> {
    pub fn new(value: T) -> Self {
        LwwWrapper(value)
    }

    pub fn get(&self) -> &T {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: PartialEq> Merge<LwwWrapper<T>> for LwwWrapper<T> {
    fn merge(&mut self, other: LwwWrapper<T>) -> bool {
        let changed = self.0 != other.0;
        self.0 = other.0;
        changed
    }
}

impl<T: Default> IsBot for LwwWrapper<T> {
    fn is_bot(&self) -> bool {
        false
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

impl<T> LatticeFrom<LwwWrapper<T>> for LwwWrapper<T> {
    fn lattice_from(other: LwwWrapper<T>) -> Self {
        other
    }
}
