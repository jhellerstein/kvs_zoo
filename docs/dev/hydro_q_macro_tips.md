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

<!-- Removed example: VectorClockBackground no longer exists in the codebase. -->

## Additional tips
- Centralize path rewrites (single ctor function) for all affected std types (`BTreeMap`, `HashMap`, `HashSet`).
- Use wrapper structs when: (a) state spans multiple closures (`scan`), or (b) generated examples must hold the state and generics would be dropped.
- Prefer direct collections only for ephemeral closures or maps not persisted across `scan`.
- If you observe E0107/E0282 after removing a wrapper, restore a monomorphic wrapper.
- Use `cargo nextest run <test>` for targeted feedback.
- Provide a `new_<name>_state()` helper (e.g. `new_store_state()`) for uniform scan initialization.

## When to apply this
- Background stages that use `scan` or stateful operators inside `q!` closures.
- Any time you see generated code errors about deep std collection paths or `state_ref_unchecked` inference issues.

## Wrapper Inventory
- `StoreState<V>`: Lattice-based key/value map plus tombstone set via `MapUnionWithTombstones`; generic over value lattice `V`.

All wrappers follow the pattern: lightweight struct + `new_<name>_state()` + public `inner` (or structured fields) with std collections rewritten via ctor. If removal of any wrapper yields codegen generic loss or path errors, revert immediately.
