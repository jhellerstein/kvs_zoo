# Detail examples

These mirror the "magic" examples but show explicit Hydro wiring so you can learn how the graph is constructed.

Explicit demos:

- local_detail.rs — explicit wiring of simple local KVS
- replicated_detail.rs — explicit wiring of RoundRobin + Gossip
- sharded_detail.rs — explicit key-hash routing and per-shard wiring

Run them:

```bash
cargo run --example replicated_detail
cargo run --example sharded_detail
```

Tip: Compare each file side-by-side with its magic counterpart (e.g., `replicated.rs`). Pair this with the `src/layer_flow.rs` file to see how the helper functions encapsulate the same wiring.
