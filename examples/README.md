# KVS Zoo Examples

This directory contains examples demonstrating different KVS architectures and the composable API design.

## Examples by Architecture

### Basic Architectures

Simple, single-concern architecture that forms the building block:

- **`local.rs`** - Single-process KVS with no distribution

  - Architecture: One process, no networking
  - Consistency: Strong (deterministic)
  - Use case: Hello, world! style example for development and testing

### Distributed Architectures

Complex architectures that combine multiple distributed systems concepts:

- **`replicated.rs`** - Multi-replica with gossip synchronization

  - Architecture: Multiple replicas with epidemic gossip
  - Consistency: Causal (with vector clocks)
  - Use case: Low latency, high availability, scalability with request volume

- **`sharded.rs`** - Hash-based horizontal partitioning

  - Architecture: Hash routing to independent shards
  - Consistency: Per-shard strong, global eventual
  - Use case: Scalability for large datasets

- **`sharded_replicated.rs`** - Combined sharding + replication

  - Architecture: Hash routing to replicated shard clusters
  - Consistency: Eventual (but this could be configured differently)
  - Use case: Large-scale systems needing both performance and fault tolerance

- **`linearizable.rs`** - Paxos-based strongly consistent KVS
  - Architecture: Proposers + Acceptors + Replicas
  - Consistency: Linearizable (strongest guarantee)
  - Use case: Critical systems requiring strong consistency

### Unified Driver API

Most examples implement the `KVSDemo` trait and use the `run_kvs_demo_impl!` macro. See `local.rs` for a complete example. The macro handles deployment, client-server communication, and operation execution.

**Note:** `linearizable.rs` uses direct flow construction due to multi-cluster lifetime constraints (proposers, acceptors, replicas must share the same lifetime).

## Running Examples

```bash
# Basic architectures
cargo run --example local
cargo run --example linearizable

# Distributed architectures
cargo run --example replicated
cargo run --example sharded
cargo run --example sharded_replicated


```

## Architecture Decision Matrix

| Example            | Coordination          | Consistency Model              | Convergence | Scalability | Complexity |
| ------------------ | --------------------- | ------------------------------ | ----------- | ----------- | ---------- |
| Local              | None                  | Strong                         | N/A         | Low         | Low        |
| Replicated         | Coordination-free     | Eventually consistent (causal) | Guaranteed  | Medium      | Medium     |
| Sharded            | Coordination-free     | Per-shard strong               | N/A         | High        | Medium     |
| Sharded+Replicated | Coordination-free     | Eventually consistent (causal) | Guaranteed  | High        | High       |
| Linearizable       | Coordination-required | Strong (linearizable)          | N/A         | Low         | Medium     |

## Key Concepts

- **CALM Theorem**: Coordination-free systems achieve eventual consistency; coordination-required systems provide stronger guarantees at the cost of availability
- **Composability**: The `KVSServer<V, Storage, Router>` API composes value types, storage strategies, and routing patterns
- **Lattice semantics**: Coordination-free systems use merge operations (LWW, causal) that guarantee convergence without consensus
