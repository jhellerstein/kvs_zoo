# Tips for writing `q!` closures + scan state with Hydro

This note captures patterns that keep Hydro's generated code compiling cleanly in trybuild/test contexts.

## Symptoms observed
- Generated examples referenced invalid paths like `std::collections::btree::map::BTreeMap` (E0433).
- Type inference failures on `state_ref_unchecked` for scan state (E0282).
- Generic leakage where explicit `DataEvent<V>` inside a `q!` closure required an extra type parameter `V` in generated functions (E0412).

These surfaced when `q!` closures directly used std collection types and/or captured generics implicitly.

## Recommended pattern
- Use a public wrapper type for scan state instead of raw collections.
  - Define a small struct (e.g., `ClockState`) that owns the map.
  - Expose a public zero-arg constructor (e.g., `new_clock_state()`), and use it in the `scan` initializer.
- Type the closure state parameter explicitly using the wrapper type.
- Avoid explicit generic event types in the closure parameter list when possible; let the pipeline type drive it.
- Prefer small, public helper functions called from within `q!` blocks (constructors, small transforms).

Rationale: the wrapper type and helpers give the code generator stable, named symbols to reference, avoiding fragile expansions of std paths and easing type inference around `state_ref_unchecked`.

## Example (VectorClockBackground)
File: `kvs_zoo/src/background/vector_clock.rs`

```rust
/// Wrapper around the vector-clock state map used inside `q!` closures.
#[derive(Clone, Debug, Default)]
pub struct ClockState {
    pub inner: ::std::collections::BTreeMap<String, VCWrapper>,
}

/// Construct a fresh `ClockState` for scans/aggregations.
pub fn new_clock_state() -> ClockState {
    ClockState::default()
}

// ...

let vector_clock_updates = data.clone().scan(
    q!(|| kvs_zoo::background::vector_clock::new_clock_state()),
    q!(move |state: &mut kvs_zoo::background::vector_clock::ClockState, event| {
        match event {
            DataEvent::Put { key, .. } | DataEvent::Delete { key } => {
                let member_raw = CLUSTER_SELF_ID.raw_id;
                let entry = state
                    .inner
                    .entry(key.clone())
                    .or_insert_with(kvs_zoo::values::VCWrapper::new);
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
            let entry = state
                .inner
                .entry(key.clone())
                .or_insert_with(kvs_zoo::values::VCWrapper::new);
            entry.merge(clock);
            Some(kvs_zoo::background::vector_clock::VectorClockSnapshot { key, clock: entry.clone() })
        }),
    );
```

## Additional tips
- Keep helper functions and wrapper types `pub` so the generated trybuild crate can reference them.
- When you must use std types directly, prefer the re-exported paths (e.g., `std::collections::BTreeMap`) over deep module paths.
- Use `cargo nextest run <test>` to run a specific test quickly; it avoids slow filtering through unrelated tests.

## When to apply this
- Background stages that use `scan` or stateful operators inside `q!` closures.
- Any time you see generated code errors about `std::collections::btree::map::BTreeMap` or `state_ref_unchecked` inference issues.
