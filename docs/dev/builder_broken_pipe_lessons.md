# Builder BrokenPipe Investigation – Lessons Learned

## TL;DR

The `BrokenPipe` errors were caused by dropping the Hydro deployment node handle (`nodes`) inside `KVSBuilder::build_with` before processes were started. This prematurely closed underlying IPC endpoints. Fix: retain the handle (temporary `std::mem::forget(nodes)`) so pipes live through `deployment.start()`.

## What Happened

- Manual examples and tests built flows in user code: the `nodes` handle stayed in scope.
- Builder hid flow creation internally and returned only `(deployment, out, input)`.
- Returning caused `nodes` to drop → pipe/file descriptors closed.
- Later `deployment.start()` tried to use closed channels → writers saw `BrokenPipe`.

## Why It Was Subtle

- Sequences of calls (manual vs builder) looked identical.
- Feature gating and macro (`q!`) speculation were distractions; runtime resource lifetime ownership was the real issue.
- Stageleft codegen behaved consistently; the lifetime boundary (library function return) differed.

## Key Lessons

1. Retain ownership of runtime graph artifacts (processes, clusters, externals) until after start.
2. Returning partial handles (without deployment graph token) risks silent resource teardown.
3. Builder ergonomics must model lifetimes explicitly (opaque wrapper or RAII struct).
4. Avoid leaking unless for short-lived demos; design a non-leaking stable API soon.

## Interim Fix

- Added `std::mem::forget(nodes)` after `connect_bincode` to keep descriptors alive.
- Verified across: local, replicated, sharded, sharded+replicated spec examples.

## Recommended Next API Improvement

Introduce a `BuiltKVS` struct:

```rust
pub struct BuiltKVS {
    deployment: hydro_deploy::Deployment,
    nodes: hydro_lang::runtime::NodesHandle, // (whatever the concrete type is)
    out: Pin<Box<dyn Stream<Item=String>>>,
    input: Pin<Box<dyn Sink<KVSOperation<V>, Error=io::Error>>>,
}
```

- `Drop` impl stops deployment cleanly.
- Provides `start()`, `shutdown()`, and accessors.

## Future Work

- Integrate `KVSCluster` spec directly: `Server::builder().from_spec(KVSCluster::sharded_replicated(3,3))`.
- Keep explicit router composition in examples and docs (educational goal); document tradeoffs and when to choose each.
- Document lifecycle invariants: connect then start; keep node handle alive.

## Takeaway

Hidden lifetimes in helper/builders can mask resource teardown bugs. Make ownership explicit, design for RAII, and treat premature drops as first-class failure modes during API design.
