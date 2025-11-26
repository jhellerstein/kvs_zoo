# Hydro / Stageleft Generic Substitution Bug

## Summary
Generated runtime code (under `target/hydro_trybuild/.../examples/`) produced by Hydro/Stageleft for `KVSCore::process` contains *unsubstituted* generic identifiers `K`, `V`, and `Store` inside staged closure parameter lists:

```rust
|state: &mut Store, operation: KVSOperation<K, V>| { ... }
```

These appear inside monomorphic functions such as:

```rust
fn __hydro_runtime<'a>(...) -> Dfir<'a> { ... }
```

Since `__hydro_runtime` has no generic parameters beyond the lifetime `'a`, the identifiers are out of scope, causing compile errors (E0412 / E0433) across multiple tests.

## Affected Versions
- hydro_lang / dfir_rs / hydro_deploy / hydro_std / lattices:
  - At commit `c648f0d` (earlier known working baseline for other code)
  - At latest `main` branch as of Nov 21, 2025
  - **Issue is present in both versions** (confirmed via testing)
- Stageleft crate version in workspace: `0.10.2`
- **This is a pre-existing bug, not a recent regression.**

## Reproduction Steps
1. In a crate depending on Hydro, define a generic function that stages a closure via `q!` inside another generic (`KVSCore::process<'a, K, V, L, Store>`).
2. Call it with concrete turbofish arguments, e.g.:
   ```rust
   KVSCore::process::<String, LwwWrapper<String>, _, std::collections::HashMap<String, LwwWrapper<String>>>(ops)
   ```
3. Build tests that trigger Hydro codegen (e.g., using `hydro_lang::test_util` or deploying a cluster).
4. Inspect generated file: `target/hydro_trybuild/kvs_zoo/examples/kvs_zoo__kvs_core__KVSNode_cluster_2_<HASH>.rs`.
5. Observe unsubstituted identifiers in the scan closure and its initializer closure (`|| Store::default()`), both failing to compile.

## Expected Behavior
The staged closure should be monomorphized: parameter list and initializer use concrete types, matching the type hints already emitted by Stageleft. Example expected emitted fragment:

```rust
|state: &mut std::collections::HashMap<String, LwwWrapper<String>>, 
 operation: KVSOperation<String, LwwWrapper<String>>| { ... }
```

Initializer:
```rust
|| std::collections::HashMap::<String, LwwWrapper<String>>::default()
```

No bare `Store`, `K`, or `V` tokens should remain in monomorphic code.

## Actual Behavior
Bare generic tokens remain:
```rust
|state: &mut Store, operation: KVSOperation<K, V>| { ... }
|| Store::default()
```
Resulting compile errors:
- `error[E0412]: cannot find type 'Store' in this scope`
- `error[E0412]: cannot find type 'K' in this scope`
- `error[E0412]: cannot find type 'V' in this scope`
- `error[E0433]: failed to resolve: use of undeclared type 'Store'`

## Impact
Blocks any test or example that relies on staged execution of `KVSCore::process`. Secondary effects:
- Snapshot examples fail.
- Meta stream tests fail.
- Paxos slot buffer tests fail.
- Guardrail trybuild tests diverge from expected diagnostics.

## Diagnostics Collected
Representative failing fragment (identical on both `c648f0d` and `main`):
```
examples/kvs_zoo__kvs_core__KVSNode_cluster_2_BE11C5DA.rs:3572:54
|state: &mut Store, operation: KVSOperation<K, V>|
```
Errors list repeated for multiple generated clusters on both tested commits.

## Suspected Root Cause
Stageleft generic substitution pass appears not to run (or runs before generic specialization) for closures emitted within a generic function context. Token-level capture of identifiers occurs; type hints around the closure *do* contain concrete types, indicating the specialization data is present but not applied to the closure body.

## Suggested Fix Directions
1. Ensure closure bodies produced by `q!` are re-written after monomorphization, substituting generic params with concrete types provided by turbofish invocation.
2. Alternatively, require explicit type hints on closure parameter patterns; integrate these hints during codegen rather than adjacent wrappers.
3. Provide a lint or build-time diagnostic when unsubstituted generics appear in monomorphic staged code.

## Workaround (Local)
Introduce monomorphic wrapper functions (e.g., `process_string_lww_hashmap`) that move staging outside generic contexts; call these wrappers instead of the fully generic function. (Not yet applied here—seeking upstream fix first.)

## Environment
- macOS (Apple Silicon) / Rust edition 2024
- rustc version (implicit via workspace; can supply on request)
- hydro crates tested at both:
  - `c648f0d` (earlier pinned version)
  - latest `main` branch (Nov 21, 2025)
- **Bug confirmed present in both versions**

## Request
Please confirm whether this is a known issue with Stageleft/Hydro generic staging. Guidance on recommended pattern (monomorphic wrapper vs. upstream substitution fix) would help. If a patch is acceptable, I can supply a minimal reproduction crate.

Thanks!
