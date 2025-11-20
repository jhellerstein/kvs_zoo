use std::collections::BTreeMap;

use crate::values::VCWrapper;
use lattices::Merge;

/// Merge an incoming vector clock into the map entry for `key`,
/// returning the updated clock snapshot (cloned from the entry).
pub fn merge_into(
    map: &mut BTreeMap<String, VCWrapper>,
    key: &str,
    incoming: VCWrapper,
) -> VCWrapper {
    let entry = map.entry(key.to_string()).or_default();
    entry.merge(incoming);
    entry.clone()
}

/// Bump the local member for `key` in the map and return the updated clock snapshot.
pub fn bump_local(map: &mut BTreeMap<String, VCWrapper>, key: &str, member_raw: u32) -> VCWrapper {
    let entry = map.entry(key.to_string()).or_default();
    entry.bump(member_raw.to_string());
    entry.clone()
}
