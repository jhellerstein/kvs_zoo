use lattices::map_union::MapUnionHashMap;
use lattices::{Max, Merge};
use serde::{Deserialize, Serialize};

/// Vector clock lattice for tracking causality across distributed nodes.
///
/// Maps node IDs to logical timestamps (counters). When merging vector clocks,
/// takes the maximum counter for each node ID, preserving causal ordering.
///
/// # Example
/// ```ignore
/// use ide_test::vector_clock::VectorClock;
/// use lattices::Merge;
///
/// let mut vc1 = VectorClock::default();
/// vc1.bump("node1".to_string());
/// 
/// let mut vc2 = VectorClock::default();
/// vc2.bump("node2".to_string());
///
/// vc1.merge(vc2); // vc1 now has entries for both node1 and node2
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    inner: MapUnionHashMap<String, Max<usize>>,
}

impl VectorClock {
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
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let self_map = self.inner.as_reveal_ref();
        let other_map = other.inner.as_reveal_ref();

        let mut strictly_less = false;

        // Check all entries in self
        for (node_id, self_count) in self_map.iter() {
            let self_val = self_count.into_reveal();
            let other_val = other_map
                .get(node_id)
                .map(|c| c.into_reveal())
                .unwrap_or(0);

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
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }

    /// Get the inner MapUnionHashMap for advanced operations.
    pub fn as_inner(&self) -> &MapUnionHashMap<String, Max<usize>> {
        &self.inner
    }

    /// Get a mutable reference to the inner MapUnionHashMap.
    pub fn as_inner_mut(&mut self) -> &mut MapUnionHashMap<String, Max<usize>> {
        &mut self.inner
    }

    /// Consume self and return the inner MapUnionHashMap.
    pub fn into_inner(self) -> MapUnionHashMap<String, Max<usize>> {
        self.inner
    }
}

impl Merge<VectorClock> for VectorClock {
    fn merge(&mut self, other: VectorClock) -> bool {
        self.inner.merge(other.inner)
    }
}

impl From<MapUnionHashMap<String, Max<usize>>> for VectorClock {
    fn from(inner: MapUnionHashMap<String, Max<usize>>) -> Self {
        Self { inner }
    }
}

impl PartialOrd for VectorClock {
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

impl lattices::LatticeFrom<VectorClock> for VectorClock {
    fn lattice_from(other: VectorClock) -> Self {
        other
    }
}
