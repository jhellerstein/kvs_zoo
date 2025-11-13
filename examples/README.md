# KVS Zoo Examples

This directory contains runnable demos of different KVS topologies, all described with a small, recursive layering API in `kvs_layer` and wired with reusable pipelines.

## The layering API (kvs_layer)

Two building blocks you can nest to any depth:

- `KVSCluster<Name, D, M, Child>` — a cluster layer pairing:
    - before_storage `D` (routing/ordering)
    - after_storage `M` (replication/responders)
    - `Child` — another `KVSCluster<…>` or `()` at the leaf
- `KVSNode<Name, D, M>` — per-member (leaf) layer with before/after strategies

See the types and traits in:

- `src/kvs_layer/types.rs` — `KVSCluster`, `KVSNode`, `KVSClusters`
- `src/kvs_layer/wire_down.rs` — `KVSWire` (before_storage routing/ordering)
- `src/kvs_layer/wire_up.rs` — `AfterWire` (after_storage replication/responders)
- `src/kvs_layer/spec.rs` — `KVSSpec` (cluster creation/registration)

## Reusable wiring (pipelines)

If you don’t want to wire Hydro by hand, use the pipelines:

- `src/pipelines/single_layer.rs`
    - `pipeline_single_layer_from_process` — route → replicate → process
- `src/pipelines/two_layer.rs`
    - `pipeline_two_layer_from_process` — parent route/replicate → leaf route → process
    - `pipeline_two_layer_from_enveloped` — variant for pre-enveloped operations

The examples below use both the simple server helpers and the explicit “detail” variants to show the minimal vs explicit Hydro wiring.

## Examples

- `local.rs` — Single node (no replication)
- `replicated.rs` — 3 replicas with gossip
    - `replicated_detail.rs` — same architecture with explicit Hydro wiring
- `sharded.rs` — 3 shards, single node per shard
    - `sharded_detail.rs` — same architecture with explicit key-hash routing info
- `sharded_replicated.rs` — 3 shards × 3 replicas
- `linearizable.rs` — Paxos + log-based delivery
- `three_level_recursive.rs` — nested layering demonstration

## Run them

```bash
cargo run --example local
cargo run --example replicated
cargo run --example sharded
cargo run --example sharded_replicated
cargo run --example linearizable
```

Output style is intentionally consistent:

- Header: `🚀 …`
- Per-op: `→ …`
- Footer: `✅ … complete`
