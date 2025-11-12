# KVS Zoo Examples

This directory contains examples that demonstrate different KVS architectures, all described with a single, unified, recursive cluster specification API.

## The Cluster Spec (one API for all topologies)

The API is a tiny tree of two building blocks:

- `KVSCluster<D, M, Child>` — a cluster level with dispatch `D`, maintenance `M`, and a child (another cluster or a node)
- `KVSNode<D, M>` — a leaf node with per-node dispatch `D` and maintenance `M`

You compose these to any depth. A minimal example:

```rust
use kvs_zoo::cluster_spec::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::{SingleNodeRouter, ShardedRouter, RoundRobinRouter};
use kvs_zoo::maintenance::{BroadcastReplication, ZeroMaintenance};

// Single node
let local = KVSCluster::new(SingleNodeRouter, ZeroMaintenance, 1, KVSNode { dispatch: (), maintenance: (), count: 1 });

// Sharded (3 shards)
let sharded = KVSCluster::new(ShardedRouter::new(3), ZeroMaintenance, 3, KVSNode { dispatch: (), maintenance: (), count: 1 });

// Sharded + Replicated (3 shards × 3 replicas)
let sharded_repl = KVSCluster::new(
    ShardedRouter::new(3),
    BroadcastReplication::default(),
    3,
    KVSNode { dispatch: RoundRobinRouter::new(), maintenance: (), count: 3 },
);

// Build and run directly from the spec
let mut server = sharded_repl.build_server::<kvs_zoo::values::CausalString>().await?;
server.start().await?;
```

Auxiliary clusters (for protocols like Paxos) are added with `.with_cluster(count, Option<name>)` and are optional.

## Examples

- `local.rs` — Single node
- `replicated.rs` — 3 replicas with gossip
- `sharded.rs` — 3 shards, single node per shard
- `sharded_replicated.rs` — 3 shards × 3 replicas (canonical)
- `linearizable.rs` — Paxos + log-based replication (proposers/acceptors as auxiliary clusters)

## Running

```bash
cargo run --example local
cargo run --example replicated                      # causal by default
cargo run --example replicated -- --lattice lww     # last-write-wins
cargo run --example sharded
cargo run --example sharded_replicated
cargo run --example linearizable
```

Outputs are intentionally terse and uniform:

- Header: `🚀 ...`
- Operations: `→ ...`
- Footer: `✅ ... complete`
