# KVS Zoo Examples

This directory contains runnable demos of different KVS topologies, all described with a small, recursive layering API in `kvs_layer` and wired with reusable pipelines.

## Magic vs Detail

We present each idea in two flavors:

- Magic: short demos using helpers/pipelines that hide most Hydro wiring
- Detail: equivalent demos with explicit Hydro graph wiring for learning

Start here depending on your goal:

- If you want to see the effect quickly, run the Magic examples.
- If you want to understand how the wiring works, read and run the Detail ones.

See also: `examples/magic/README.md` and `examples/detail/README.md` for curated lists.

## The layering API (kvs_layer)

Two building blocks you can nest to any depth:

- `KVSCluster<Name, D, M, Child>` — a cluster layer pairing:
    - before_storage `D` (routing/ordering)
    - after_storage `M` (replication/responders)
    - `Child` — another `KVSCluster<…>` or `()` at the leaf
- `KVSNode<Name, D, M>` — per-member (leaf) layer with before/after strategies

See the types and traits in:

- `src/kvs_layer/types.rs` — `KVSCluster`, `KVSNode`, `KVSClusters`
- `src/kvs_layer/wire_before.rs` — `KVSWire` (before_storage routing/ordering)
- `src/kvs_layer/wire_after.rs` — `AfterWire` (after_storage replication/responders)
- `src/kvs_layer/spec.rs` — `KVSSpec` (cluster creation/registration)

## Reusable wiring (flows)

If you don’t want to wire Hydro by hand, use the unified flow:

- `src/layer_flow.rs`
    - `layer_flow` — unified: parent before_storage → optional leaf before_storage (use `NoLeaf` for no-op) → processing → after_storage. Accepts either bare operations or envelopes via `Into<KVSOperation<_>>`.

The examples below use both the simple server helpers and the explicit “detail” variants to show the minimal vs explicit Hydro wiring.

## Examples

- Magic
    - `local.rs` — Single node (no replication)
    - `replicated.rs` — 3 replicas with gossip
    - `sharded.rs` — 3 shards, single node per shard
    - `sharded_replicated.rs` — 3 shards × 3 replicas
    - `linearizable.rs` — Paxos + Sequenced<Broadcast> (slot-ordered replication)
    - `three_level_recursive.rs` — nested layering demonstration
- Detail (explicit Hydro wiring)
    - `replicated_detail.rs` — same architecture as `replicated.rs`, explicit wiring
    - `sharded_detail.rs` — same architecture as `sharded.rs`, explicit routing info

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
