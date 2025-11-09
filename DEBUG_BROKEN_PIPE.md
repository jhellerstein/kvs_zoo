# Debug Log: Broken Pipe Issue

## Problem
Examples on `feat/cluster-spec-api` branch hit `BrokenPipe` error when calling `run_ops()`.
Examples on `main` branch work perfectly.

## Key Observations

### Main Branch (WORKS)
- Examples construct deployment manually in example code
- No builder API in library
- No "deploy" feature in main dependencies
- Dev-dependencies have `features = ["deploy", "viz", "sim"]`
- Example code directly calls hydro_lang APIs like `.with_process()`
- Sequence: `deploy()` → `connect_bincode()` → `start()` → sleep → `run_ops()` ✅

### Feature Branch (BROKEN)
- Builder API in library (`src/server.rs`)
- Library code calls `.with_process()`, `.with_cluster()`, etc.
- Added "deploy" feature to main dependencies (required for library to compile)
- Examples use builder: `Server::builder().build()` → `deployment.start()` → `run_ops()` ❌
- Same sequence as main but gets BrokenPipe

## Experiments Tried

### Experiment 1: Remove "deploy" feature from main dependencies
**Result:** Library doesn't compile - `.with_process()` requires deploy feature
**Conclusion:** Need deploy feature for builder code in library

### Experiment 2: Add `[features] deploy = []` and use `#[cfg(feature = "deploy")]` guards
**Result:** Examples don't compile - builder methods not available
**Tried:** `cargo build --examples --features deploy`
**Result:** Compiles but still BrokenPipe at runtime
**Conclusion:** Feature gating works for compilation but doesn't fix runtime issue

### Experiment 3: Remove `.with_default_optimize()` call
**Hypothesis:** Maybe optimization breaks something
**Result:** Can't compile without deploy feature (need `.with_process()`)
**Conclusion:** Can't test this without deploy feature

### Experiment 4: Keep deploy feature, add `.with_default_optimize()` back
**Result:** Still BrokenPipe ❌
**Conclusion:** `.with_default_optimize()` is not the sole cause

### Experiment 5: Deploy feature WITHOUT `.with_default_optimize()`
**Result:** Still BrokenPipe ❌  
**Conclusion:** `.with_default_optimize()` is NOT the cause. Something else about having deploy feature in main deps breaks it.

## Critical Question
Why does adding "deploy" feature to main dependencies break the examples?
Main's examples work WITHOUT deploy in main deps because the hydro APIs are called from example code (compiled with dev-deps).

### Experiment 6: Run tests instead of examples
**Result:** Tests PASS ✅ (3 passed in composable_integration)
**Key Finding:** Tests don't use KVSBuilder - they construct deployment manually like main's examples
**CONCLUSION:** The bug is specifically in the KVSBuilder API, not in having deploy feature per se!

## Root Cause Hypothesis
The KVSBuilder code in the library is somehow broken when compiled with the "deploy" feature.
Tests and main's examples work because they don't use the builder - they construct deployments manually.

### Detailed Comparison
**Working Test/Example sequence:**
1. `let flow = Flow::new();`
2. `let (deployment, dispatch, maintenance) = Server::deploy(&flow);`  // calls dispatch.create_deployment()
3. `let port = Server::run(&proxy, &deployment, &client_external, dispatch, maintenance);`
4. `let nodes = flow.with_process()...with_cluster()...deploy(&mut deployment_obj);`
5. `deployment_obj.deploy().await;`
6. `let (out, in) = nodes.connect_bincode(port).await;`
7. `deployment_obj.start().await;`  ✅ Works

**Broken Builder sequence:**
1. `let flow = Flow::new();`
2. `let clusters = dispatch.create_deployment(&flow);`  // same!
3. `let port = Server::run(&proxy, &clusters, &client_external, dispatch, maintenance);`  // same!
4. `let nodes = flow.with_process()...with_cluster(clusters.kvs_cluster())...deploy(&mut deployment_obj);`  // same!
5. `deployment_obj.deploy().await;`
6. `let (out, input) = nodes.connect_bincode(port).await;`
7. Returns to caller
8. Caller: `deployment.start().await;`  ❌ BrokenPipe

**Conclusion:** The sequences are IDENTICAL! But one works and one doesn't.

## New Hypothesis: Stageleft Codegen Issue
When the builder code (which calls `Server::run()`) is compiled as part of the LIBRARY with the "deploy" feature enabled, stageleft might generate different code than when the same calls happen in test/example code (compiled with dev-deps).

The `Server::run()` method contains `q!()` macros (stageleft quotes) that generate runtime code. Maybe these are being compiled differently?

## Next Steps
[ ] Check if removing all builder code makes examples work (proves it's the builder)
[ ] Try moving builder into a separate module/file
[ ] Check generated code in target/hydro_trybuild to see if there's a difference
[ ] Contact Hydro team about potential stageleft issue with library vs example compilation

## Current State
- Cargo.toml: Has `features = ["deploy"]` in hydro_lang main dependency
- server.rs: Has `.with_default_optimize()` in build_with()
- No feature guards (all removed)
- About to test if this works...

## Questions to Answer
1. Does `.with_default_optimize()` cause the issue?
2. Does having "deploy" in main deps change runtime behavior somehow?
3. Is there a difference in how stageleft compiles with/without deploy feature?
4. Is the builder pattern itself causing an issue with connection setup?

## Next Steps
[ ] Test current state (deploy feature + with_default_optimize)
[ ] If broken: try deploy feature WITHOUT with_default_optimize  
[ ] If broken: compare exact deployment/start/connection sequence between main and branch
[ ] Check if tests work (they might use different code path)
