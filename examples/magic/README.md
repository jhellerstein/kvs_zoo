# Magic examples

Short demos that use server helpers and wiring pipelines to keep code minimal.

Run them:

```bash
cargo run --example local
cargo run --example replicated
cargo run --example sharded
cargo run --example sharded_replicated
cargo run --example linearizable
cargo run --example three_level_recursive
```

Learn more wiring details in the paired Detail examples:

- replicated_detail.rs — explicit Hydro wiring for the replicated topology
- sharded_detail.rs — explicit Hydro wiring and shard-by-hash prints
