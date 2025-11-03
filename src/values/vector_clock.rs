//! Vector clock implementation for causal consistency
//!
//! Vector clocks track causality relationships in distributed systems by
//! maintaining logical timestamps for each node. They enable detection of
//! concurrent vs. causally ordered events.

use lattices::map_union::MapUnionBTreeMap;
use lattices::{Max, Merge};
use serde::{Deserialize, Serialize};

/// Vector clock lattice for tracking causality across distributed nodes.
///
/// Maps node IDs to logical timestamps (counters). When merging vector clocks,
/// takes the maximum counter for each node ID, preserving causal ordering.
///
/// ## Causal Relationships
///
/// - **Happens-before**: VC1 < VC2 if VC1\[i\] ≤ VC2\[i\] for all i, and VC1\[j\] < VC2\[j\] for some j
/// - **Concurrent**: Neither VC1 < VC2 nor VC2 < VC1
/// - **Identical**: VC1 = VC2 if VC1\[i\] = VC2\[i\] for all i
///
/// ## Example
/// ```rust
/// use kvs_zoo::values::{VCWrapper, Merge};
///
/// let mut vc1 = VCWrapper::new();
/// vc1.bump("node1".to_string());
///
/// let mut vc2 = VCWrapper::new();
/// vc2.bump("node2".to_string());
///
/// vc1.merge(vc2); // vc1 now has entries for both node1 and node2
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VCWrapper {
    inner: MapUnionBTreeMap<String, Max<usize>>,
}

impl VCWrapper {
    /// Create a new empty vector clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter for the given node ID.
    ///
    /// If the node ID doesn't exist, it will be initialized to 0 and then incremented to 1.
    /// If it does exist, the counter will be incremented by 1.
    ///
    /// # Arguments
    /// * `node_id` - The identifier for the node whose counter should be bumped
    pub fn bump(&mut self, node_id: String) {
        let current = self
            .inner
            .as_reveal_mut()
            .entry(node_id.clone())
            .or_insert(Max::new(0))
            .into_reveal();

        let new_value = Max::new(current + 1);
        self.inner.as_reveal_mut().insert(node_id, new_value);
    }

    /// Get the current counter value for a specific node ID.
    ///
    /// Returns `Some(counter)` if the node exists in the vector clock,
    /// or `None` if it doesn't. Note that a missing entry is semantically
    /// equivalent to having a value of 0 (the bottom value for usize).
    pub fn get(&self, node_id: &str) -> Option<usize> {
        self.inner
            .as_reveal_ref()
            .get(node_id)
            .map(|max| max.into_reveal())
    }

    /// Check if this vector clock happened before another (proper causal ordering).
    ///
    /// Returns `true` if this clock is strictly less than or equal to `other` in all
    /// components, and strictly less in at least one component.
    pub fn happened_before(&self, other: &VCWrapper) -> bool {
        let self_map = self.inner.as_reveal_ref();
        let other_map = other.inner.as_reveal_ref();

        let mut strictly_less = false;

        // Check all entries in self
        for (node_id, self_count) in self_map.iter() {
            let self_val = self_count.into_reveal();
            let other_val = other_map.get(node_id).map(|c| c.into_reveal()).unwrap_or(0);

            if self_val > other_val {
                return false; // self has a component greater than other
            }
            if self_val < other_val {
                strictly_less = true;
            }
        }

        // Check entries only in other (self implicitly has 0 for these)
        for (node_id, other_count) in other_map.iter() {
            if !self_map.contains_key(node_id) && other_count.into_reveal() > 0 {
                strictly_less = true;
            }
        }

        strictly_less
    }

    /// Check if two vector clocks are concurrent (neither happened before the other).
    pub fn is_concurrent(&self, other: &VCWrapper) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }

    /// Get the inner MapUnionBTreeMap for advanced operations.
    pub fn as_inner(&self) -> &MapUnionBTreeMap<String, Max<usize>> {
        &self.inner
    }

    /// Get a mutable reference to the inner MapUnionBTreeMap.
    pub fn as_inner_mut(&mut self) -> &mut MapUnionBTreeMap<String, Max<usize>> {
        &mut self.inner
    }

    /// Consume self and return the inner MapUnionBTreeMap.
    pub fn into_inner(self) -> MapUnionBTreeMap<String, Max<usize>> {
        self.inner
    }
}

impl Merge<VCWrapper> for VCWrapper {
    fn merge(&mut self, other: VCWrapper) -> bool {
        self.inner.merge(other.inner)
    }
}

impl From<MapUnionBTreeMap<String, Max<usize>>> for VCWrapper {
    fn from(inner: MapUnionBTreeMap<String, Max<usize>>) -> Self {
        Self { inner }
    }
}

impl PartialOrd for VCWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        // Check if self <= other and other <= self
        let self_le_other = self.happened_before(other) || self == other;
        let other_le_self = other.happened_before(self) || self == other;

        match (self_le_other, other_le_self) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => None, // Concurrent
        }
    }
}

impl std::hash::Hash for VCWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the BTreeMap by iterating in order (BTreeMap guarantees consistent iteration order)
        for (key, value) in self.inner.as_reveal_ref().iter() {
            key.hash(state);
            value.into_reveal().hash(state);
        }
    }
}

impl lattices::LatticeFrom<VCWrapper> for VCWrapper {
    fn lattice_from(other: VCWrapper) -> Self {
        other
    }
}
