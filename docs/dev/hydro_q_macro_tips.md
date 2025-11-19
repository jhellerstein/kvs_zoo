# Tips for writing `q!` closures + scan state with Hydro

This note captures patterns that keep Hydro's generated code compiling cleanly in trybuild/test contexts.

## Symptoms observed
- Generated examples referenced invalid paths like `std::collections::btree::map::BTreeMap` (E0433).
- Type inference failures on `state_ref_unchecked` for scan state (E0282).
- Generic leakage where explicit `DataEvent<V>` inside a `q!` closure required an extra type parameter `V` in generated functions (E0412).

These surfaced when `q!` closures directly used std collection types and/or captured generics implicitly.

## Recommended pattern (Hybrid)
Use a combination of ctor path rewrites and monomorphic wrapper structs:

1. Path normalization via ctor:
   In `lib.rs`, install a rewrite so deep internal paths do not leak:
   ```rust
   #[ctor::ctor]
   fn init_rewrites() {
       stageleft::add_private_reexport(
           vec!["std","collections","btree","map","BTreeMap"],
           vec!["std","collections","BTreeMap"],
       );
   }
   ```
   This cures E0433 deep-path expansion issues.

2. Wrapper for generic scan state:
   When a `scan` state is a generic collection (e.g. `BTreeMap<K,V>`), Stageleft codegen may drop `<K,V>` and re-emit only the head, triggering E0107 and E0282. Provide a monomorphic wrapper struct (e.g. `ClockState`) so the generator references a concrete symbol and preserves type inference.

3. Direct collection only when safe:
   Use raw `BTreeMap` inside `q!` closures only if the generated code will not re-materialize the type (e.g. simple maps not captured as persistent scan state). If failures appear, revert to a wrapper.

Rationale: ctor-installed rewrites fix path stability; wrappers fix generic token loss. Together they minimize boilerplate while keeping codegen robust.

## Example (VectorClockBackground hybrid)
File: `kvs_zoo/src/background/vector_clock.rs`

```rust
#[derive(Clone, Debug, Default)]
pub struct ClockState { pub inner: ::std::collections::BTreeMap<String, VCWrapper> }
pub fn new_clock_state() -> ClockState { ClockState::default() }

let vector_clock_updates = data.clone().scan(
    q!(|| kvs_zoo::background::vector_clock::new_clock_state()),
    q!(move |state: &mut kvs_zoo::background::vector_clock::ClockState, event| {
        match event {
            DataEvent::Put { key, .. } | DataEvent::Delete { key } => {
                let member_raw = CLUSTER_SELF_ID.raw_id;
                let entry = state.inner.entry(key.clone()).or_default();
                entry.bump(member_raw.to_string());
                Some((key, member_raw, entry.clone()))
            }
            DataEvent::Get { .. } => None,
        }
    }),
);

let aggregated = combined_meta
    .filter_map(q!(|event| match event {
        MetaEvent::VectorClock { key, member: _, clock } => Some((key, clock)),
        _ => None,
    }))
    .scan(
        q!(|| kvs_zoo::background::vector_clock::new_clock_state()),
        q!(|state: &mut kvs_zoo::background::vector_clock::ClockState, (key, clock)| {
            let entry = state.inner.entry(key.clone()).or_default();
            entry.merge(clock);
            Some(kvs_zoo::kvs_core::events::MetaEvent::VectorClockSnapshot { key, clock: entry.clone() })
        }),
    );
```

## Additional tips
- Centralize path rewrites (single ctor function) for all affected std types (`BTreeMap`, `HashMap`, `HashSet`).
- Use wrapper structs when: (a) state spans multiple closures (`scan`), or (b) generated examples must hold the state and generics would be dropped.
- Prefer direct collections only for ephemeral closures or maps not persisted across `scan`.
- If you observe E0107/E0282 after removing a wrapper, restore a monomorphic wrapper.
- Use `cargo nextest run <test>` for targeted feedback.
- Provide a `new_<name>_state()` helper (e.g. `new_clock_state()`, `new_store_state()`, `new_prune_state()`, `new_frontier_state()`) for uniform scan initialization.

## When to apply this
- Background stages that use `scan` or stateful operators inside `q!` closures.
- Any time you see generated code errors about deep std collection paths or `state_ref_unchecked` inference issues.

## Wrapper Inventory
- `ClockState`: Vector clock map per key (BTreeMap<String, VCWrapper>) used in background updates and aggregation.
- `PruneState`: Tracks latest, pending tomb VCs, and frontier VCs for strict prune decisions.
- `FrontierState`: Maintains merged per-key frontier clocks for downstream reclamation logic.
- `StoreState<V>`: Lattice-based key/value map plus tombstone set via `MapUnionWithTombstones`; generic over value lattice `V`.

All wrappers follow the pattern: lightweight struct + `new_<name>_state()` + public `inner` (or structured fields) with std collections rewritten via ctor. If removal of any wrapper yields codegen generic loss or path errors, revert immediately.
