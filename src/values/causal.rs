//! Causal consistency value wrapper
//!
//! Provides causal consistency semantics using vector clocks to track
//! causality relationships between operations. Values are merged based
//! on their causal relationships rather than simple overwrite.

use super::VCWrapper;
use lattices::{DomPair, Merge, set_union::SetUnionHashSet};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

/// Simple hasher for ordering purposes - much cheaper than DefaultHasher
struct SimpleHasher<'a>(&'a mut u64);

impl<'a> Hasher for SimpleHasher<'a> {
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            *self.0 = self.0.wrapping_mul(31).wrapping_add(byte as u64);
        }
    }
    
    fn finish(&self) -> u64 {
        *self.0
    }
}

/// A causally consistent value wrapper that tracks causality with vector clocks
///
/// This wrapper combines a vector clock (for causality tracking) with a set of
/// values (for concurrent updates). When two causal values are merged:
///
/// - If one causally dominates the other, the dominating value wins
/// - If they are concurrent, their value sets are merged (union)
///
/// ## Usage
///
/// ```rust
/// use kvs_zoo::values::{CausalWrapper, VCWrapper, Merge};
///
/// let mut vc1 = VCWrapper::new();
/// vc1.bump("node1".to_string());
/// let causal1 = CausalWrapper::new(vc1, "value1".to_string());
///
/// let mut vc2 = VCWrapper::new();
/// vc2.bump("node2".to_string());
/// let causal2 = CausalWrapper::new(vc2, "value2".to_string());
///
/// // These are concurrent, so values will be merged into a set
/// let mut merged = causal1;
/// merged.merge(causal2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausalWrapper<T>
where
    T: Clone + Hash + Eq + std::fmt::Debug,
{
    inner: DomPair<VCWrapper, SetUnionHashSet<T>>,
}

impl<T> CausalWrapper<T>
where
    T: Clone + Hash + Eq + std::fmt::Debug,
{
    /// Create a new causal value with the given vector clock and initial value
    pub fn new(vc: VCWrapper, value: T) -> Self {
        let mut hash_set = HashSet::new();
        hash_set.insert(value);
        let set = SetUnionHashSet::new(hash_set);

        Self {
            inner: DomPair::new(vc, set),
        }
    }

    /// Create a new causal value with the given vector clock and set of values
    pub fn new_with_set(vc: VCWrapper, values: HashSet<T>) -> Self {
        let set = SetUnionHashSet::new(values);

        Self {
            inner: DomPair::new(vc, set),
        }
    }

    /// Get references to the vector clock and value set
    pub fn as_parts(&self) -> (&VCWrapper, &SetUnionHashSet<T>) {
        self.inner.as_reveal_ref()
    }

    /// Get the vector clock
    pub fn vector_clock(&self) -> &VCWrapper {
        self.inner.as_reveal_ref().0
    }

    /// Get the set of values
    pub fn values(&self) -> &HashSet<T> {
        self.inner.as_reveal_ref().1.as_reveal_ref()
    }

    /// Get a single value if there's only one, otherwise None
    pub fn single_value(&self) -> Option<&T> {
        let values = self.values();
        if values.len() == 1 {
            values.iter().next()
        } else {
            None
        }
    }

    /// Extract the inner DomPair (for compatibility with existing code)
    pub fn into_inner(self) -> DomPair<VCWrapper, SetUnionHashSet<T>> {
        self.inner
    }
}

impl<T> Hash for CausalWrapper<T>
where
    T: Clone + Hash + Eq + std::fmt::Debug,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let (vc, set) = self.inner.as_reveal_ref();
        vc.hash(state);

        // Hash the set by iterating in a consistent order
        // Use a simple hash for ordering - cheaper than Debug formatting but deterministic
        let mut items_with_hashes: Vec<_> = set.as_reveal_ref().iter().map(|item| {
            // Use a simple FNV-like hash for ordering (cheaper than DefaultHasher)
            let mut simple_hash = 0u64;
            item.hash(&mut SimpleHasher(&mut simple_hash));
            (simple_hash, item)
        }).collect();
        
        items_with_hashes.sort_by_key(|(hash_val, _)| *hash_val);

        for (_, item) in items_with_hashes {
            item.hash(state);
        }
    }
}

impl<T> Merge<CausalWrapper<T>> for CausalWrapper<T>
where
    T: Clone + Hash + Eq + std::fmt::Debug,
{
    fn merge(&mut self, other: CausalWrapper<T>) -> bool {
        self.inner.merge(other.inner)
    }
}

impl<T> Default for CausalWrapper<T>
where
    T: Clone + Hash + Eq + std::fmt::Debug,
{
    fn default() -> Self {
        Self {
            inner: DomPair::default(),
        }
    }
}

/// Type alias for causal strings (commonly used in examples)
pub type CausalString = CausalWrapper<String>;
