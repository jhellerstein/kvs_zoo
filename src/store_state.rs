//! Store state wrapper using `MapUnionWithTombstones` so core remains oblivious to
//! tombstone mechanics. Keys are always `String`, so we unconditionally use
//! the compressed `FstTombstoneSet<String>` implementation for tombstones.
//! If numeric keys are introduced later we can add a Roaring bitmap variant.
//!
//! Helper delta constructors produce lattice fragments for PUT and DELETE without
//! requiring `KVSCore` to understand tombstones.

use std::collections::HashMap;

use lattices::map_union_with_tombstones::MapUnionWithTombstones;
use lattices::tombstone::FstTombstoneSet;

/// Primary store state lattice: map of live values paired with a compressed FST tombstone set.
pub type StoreState<V> = MapUnionWithTombstones<HashMap<String, V>, FstTombstoneSet<String>>;

/// Construct a PUT delta: singleton map entry, empty tombstone set.
pub fn delta_put<V>(key: String, value: V) -> StoreState<V>
where
    V: Clone,
{
    StoreState::new_from([(key, value)], FstTombstoneSet::default())
}

/// Construct a DELETE delta: empty map, singleton tombstone set.
pub fn delta_del<V>(key: String) -> StoreState<V>
where
    V: Clone,
{
    // FstTombstoneSet implements FromIterator<String>
    StoreState::new_from([], std::iter::once(key))
}

/// Lookup helper: returns Some(&V) only if key is live (present and not tombstoned).
pub fn get_live<'a, V>(state: &'a StoreState<V>, key: &str) -> Option<&'a V> {
    let (map, tombs) = state.as_reveal_ref();
    if tombs.contains(key) { None } else { map.get(key) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattices::Merge;
    use crate::values::LwwWrapper;

    #[test]
    fn put_delete_resurrect_flow() {
        let mut state: StoreState<LwwWrapper<String>> = StoreState::default();

        // PUT k=1
        state.merge(delta_put("k".to_string(), LwwWrapper::new("1".to_string())));
        assert_eq!(get_live(&state, "k").unwrap().inner(), "1");

        // DELETE k
        state.merge(delta_del::<LwwWrapper<String>>("k".to_string()));
        assert!(get_live(&state, "k").is_none());

        // PUT k=2 (resurrection)
        state.merge(delta_put("k".to_string(), LwwWrapper::new("2".to_string())));
        assert_eq!(get_live(&state, "k").unwrap().inner(), "2");
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
