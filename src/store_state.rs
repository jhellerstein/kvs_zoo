//! Store state wrapper using `MapUnionWithTombstones` so core remains oblivious to
//! tombstone mechanics. Keys are always `String`, so we unconditionally use
//! the compressed `FstTombstoneSet<String>` implementation for tombstones.
//! If numeric keys are introduced later we can add a Roaring bitmap variant.
//!
//! Helper delta constructors produce lattice fragments for PUT and DELETE without
//! requiring `KVSCore` to understand tombstones.

use std::collections::HashMap;

use lattices::{IsBot, LatticeFrom, Merge, map_union_with_tombstones::MapUnionWithTombstones};

// Select tombstone set implementation by feature flag.
// Temporarily use HashSet for all variants until Hydro exposes FST tombstones.
#[cfg(feature = "tombstone_fst")]
type TombSet = std::collections::HashSet<String>;

#[cfg(all(not(feature = "tombstone_fst"), feature = "tombstone_hashset"))]
type TombSet = std::collections::HashSet<String>;

#[cfg(not(any(feature = "tombstone_fst", feature = "tombstone_hashset")))]
type TombSet = std::collections::HashSet<String>;

/// Primary store state lattice: map of live values paired with a tombstone set.
#[derive(Clone, Debug, Default)]
pub struct StoreState<V> {
    inner: MapUnionWithTombstones<HashMap<String, V>, TombSet>,
}

impl<V> StoreState<V> {
    /// Construct a new store state from explicit map/tombstone collections.
    pub fn new_from(map: impl Into<HashMap<String, V>>, tombstones: impl Into<TombSet>) -> Self {
        Self {
            inner: MapUnionWithTombstones::new_from(map, tombstones),
        }
    }

    /// Reveal the inner map/tombstone references.
    pub fn as_reveal_ref(&self) -> (&HashMap<String, V>, &TombSet) {
        self.inner.as_reveal_ref()
    }

    /// Reveal mutable references to the inner map/tombstone collections.
    pub fn as_reveal_mut(&mut self) -> (&mut HashMap<String, V>, &mut TombSet) {
        self.inner.as_reveal_mut()
    }
}

impl<V> Merge<StoreState<V>> for StoreState<V>
where
    V: Clone + Merge<V> + LatticeFrom<V> + IsBot,
{
    fn merge(&mut self, other: StoreState<V>) -> bool {
        let keys_to_resurrect: Vec<String> = {
            let (map, _) = other.inner.as_reveal_ref();
            map.keys().cloned().collect()
        };

        let mut changed = false;
        if !keys_to_resurrect.is_empty() {
            let (_, tombs) = self.inner.as_reveal_mut();
            for key in &keys_to_resurrect {
                if tombs.remove(key) {
                    changed = true;
                }
            }
        }

        changed | self.inner.merge(other.inner)
    }
}

/// Construct a PUT delta: singleton map entry, empty tombstone set.
pub fn delta_put<V>(key: String, value: V) -> StoreState<V>
where
    V: Clone,
{
    StoreState::new_from([(key, value)], TombSet::default())
}

/// Construct a DELETE delta: empty map, singleton tombstone set.
pub fn delta_del<V>(key: String) -> StoreState<V>
where
    V: Clone,
{
    // Tombstone set implements FromIterator<String>
    StoreState::new_from([], std::iter::once(key).collect::<TombSet>())
}

/// Lookup helper: returns Some(&V) only if key is live (present and not tombstoned).
pub fn get_live<'a, V>(state: &'a StoreState<V>, key: &str) -> Option<&'a V> {
    let (map, tombs) = state.as_reveal_ref();
    if tombs.contains(key) {
        None
    } else {
        map.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::LwwWrapper;
    use lattices::Merge;

    #[test]
    fn put_delete_resurrect_flow() {
        let mut state: StoreState<LwwWrapper<String>> = StoreState::default();

        // PUT k=1
        state.merge(delta_put("k".to_string(), LwwWrapper::new("1".to_string())));
        assert_eq!(get_live(&state, "k").unwrap().get(), "1");

        // DELETE k
        state.merge(delta_del::<LwwWrapper<String>>("k".to_string()));
        assert!(get_live(&state, "k").is_none());

        // PUT k=2 (resurrection)
        state.merge(delta_put("k".to_string(), LwwWrapper::new("2".to_string())));
        assert_eq!(get_live(&state, "k").unwrap().get(), "2");
    }

    #[test]
    fn delete_idempotent() {
        let mut state: StoreState<LwwWrapper<String>> = StoreState::default();
        state.merge(delta_put("x".to_string(), LwwWrapper::new("A".to_string())));
        state.merge(delta_del::<LwwWrapper<String>>("x".to_string()));
        state.merge(delta_del::<LwwWrapper<String>>("x".to_string())); // second delete
        assert!(get_live(&state, "x").is_none());
    }
}
