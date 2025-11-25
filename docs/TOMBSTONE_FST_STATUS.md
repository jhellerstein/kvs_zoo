# FST Tombstone Status

## Current Status

The tombstone-based KVS storage feature has been implemented with generic key type support. However, the FST and Roaring tombstone sets are not currently active due to Hydro dependency constraints.

## Implementation Details

- **Generic Key Support**: ✅ Complete - KVSCore now supports generic key types (String, u64, custom types)
- **LocalMap Module**: ✅ Complete - Generic wrapper around MapUnionWithTombstones
- **Type Aliases**: ✅ Defined for FST, Roaring, and HashSet tombstones
- **Tests**: ✅ Passing with HashSet tombstones

## Hydro Dependency Issue

### Current Revision
The project uses Hydro revision `0c6666bfdcda799f8c835bb7198f7d24e1c1c41d` which does NOT include:
- `lattices::tombstone::FstTombstoneSet`
- `lattices::tombstone::RoaringTombstoneSet`

### Main Branch
The latest Hydro main branch DOES include the tombstone module, but has breaking API changes:
- `MemberId::from_raw()` has been removed
- `MemberId.raw_id` field access has changed
- These changes affect multiple files in the codebase

## Temporary Workaround

The type aliases currently use `HashSet` as a fallback:
```rust
pub type LocalHashMapFst<V> = LocalMap<String, V, HashMap<String, V>, HashSet<String>>;
pub type LocalHashMapRoaring<V> = LocalMap<u64, V, HashMap<u64, V>, HashSet<u64>>;
```

This provides the same tombstone semantics but without the space-efficient compression.

## Path Forward

To enable FST and Roaring tombstones:

1. **Update Hydro Dependency**: Update to a Hydro revision that includes the tombstone module
2. **Fix MemberId API Changes**: Update all code using `MemberId::from_raw()` and `.raw_id`
3. **Update Type Aliases**: Change to use `FstTombstoneSet<String>` and `RoaringTombstoneSet`
4. **Test**: Verify all tests pass with the new tombstone implementations

## Files to Update for FST Support

When ready to enable FST tombstones:

1. `kvs_zoo/Cargo.toml` - Update hydro git revision
2. `kvs_zoo/src/kvs_core/local_map.rs` - Update type aliases
3. All files using `MemberId` - Update to new API
4. Run full test suite to verify

## Test Status

Current test results with HashSet tombstones:
- ✅ 27 protocol tests passing
- ✅ 3 local_map tests passing  
- ✅ 4 other unit tests passing
- ⚠️ 3 integration tests failing (Store type parameter issue in generated code)

The integration test failures are unrelated to tombstones - they're due to the generic Store type parameter needing to be specified in Hydro's code generation templates.
