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

Most examples use the unified `KVSDemo` trait and `run_kvs_demo()` driver:

```rust
impl KVSDemo for MyDemo {
    type Value = String;
    type Storage = MyKVS;
    type Router = MyRouter;

    fn create_router<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Router { ... }
    fn cluster_size(&self) -> usize { ... }
    fn description(&self) -> &'static str { ... }
    // ... other methods
}
```

**Note:** The linearizable (Paxos) example uses a specialized driver due to its multi-cluster requirements, but follows the same conceptual pattern.

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

## Key Architectural Concepts Demonstrated

1. **CALM Theorem**: Coordination-free systems (replicated, sharded) can achieve eventual consistency without coordination, while coordination-required systems (linearizable) provide stronger guarantees at the cost of availability and performance

2. **Coordination vs. Coordination-free**: Systems that avoid coordination can scale linearly and remain available during partitions, while coordination-based systems (Paxos) provide stronger consistency at the cost of latency, scalability and availability

3. **Eventual Consistency**: Coordination-free systems use lattice-based merge operations (LWW, causal) that guarantee convergence without requiring agreement protocols

4. **Composability**: The `KVSServer<V, Storage, Router>` API shows how complex architectures can be built by composing simple, well-defined components

5. **Routing Strategies**: How different routing strategies (broadcast, round-robin, hash-based) affect coordination requirements and scalability

6. **Storage Strategies**: How different local storage strategies (LWW, causal) provide different consistency guarantees while remaining coordination-free

7. **Unified API Design**: How the `KVSServer<V, Storage, Router>` API enables clean composition of any architecture through type parameters rather than separate functions
