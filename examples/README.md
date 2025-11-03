# KVS Zoo Examples

This directory contains examples demonstrating different KVS architectures and the composable API design.

## Examples by Architecture

### Basic Architectures

Simple, single-concern architecture that forms the building block:

- **`local.rs`** - Single-process KVS with no distribution

  - Architecture: One process, no networking
  - Consistency: Strong (deterministic)
  - Use case: Development, testing, simple applications

### Distributed Architectures

Complex architectures that combine multiple distributed systems concepts:

- **`replicated.rs`** - Multi-replica with gossip synchronization

  - Architecture: Multiple replicas with epidemic gossip
  - Consistency: Causal (with vector clocks)
  - Use case: High availability, partition tolerance

- **`sharded.rs`** - Hash-based horizontal partitioning

  - Architecture: Hash routing to independent shards
  - Consistency: Per-shard strong, global eventual
  - Use case: Scalability for large datasets

- **`sharded_replicated.rs`** - Combined sharding + replication
  - Architecture: Hash routing to replicated shard clusters
  - Consistency: Per-shard causal, global eventual
  - Use case: Large-scale systems needing both performance and fault tolerance


- **`linearizable.rs`** - Paxos-based strongly consistent KVS
  - Architecture: Proposers + Acceptors + Replicas
  - Consistency: Linearizable (strongest guarantee)
  - Use case: Critical systems requiring strong consistency

### Unified Driver API

Most examples use the elegant unified `KVSDemo` trait and `run_kvs_demo()` driver:

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

**Benefits:**

- Clean, consistent interface across architectures
- Easy to compare different approaches
- Minimal boilerplate code
- Automatic deployment and operation handling

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

| Example            | Consistency      | Availability | Partition Tolerance | Scalability | Complexity |
| ------------------ | ---------------- | ------------ | ------------------- | ----------- | ---------- |
| Local              | Strong           | Low          | None                | Low         | Low        |
| Replicated         | Causal           | High         | High                | Medium      | Medium     |
| Sharded            | Per-shard Strong | Low          | Low                 | High        | Medium     |
| Sharded+Replicated | Per-shard Causal | High         | High                | High        | High       |
| Linearizable       | Strong           | Medium       | Low                 | Low         | Medium     |

## Key Architectural Concepts Demonstrated

1. **Trade-offs**: Each architecture makes different trade-offs between consistency, availability, and partition tolerance (CAP theorem)

2. **Composability**: The `KVSServer<V, Storage, Router>` API shows how complex architectures can be built by composing simple components

3. **Routing Strategies**: How different routing strategies (Local, Replicated, Sharded) affect system behavior

4. **Storage Strategies**: How different storage strategies (LWW, Replicated) provide different consistency guarantees

5. **Scalability Patterns**: How sharding enables horizontal scaling while replication enables fault tolerance

6. **Unified API Design**: How the `KVSServer<V, Storage, Router>` API enables clean composition of any architecture through type parameters rather than separate functions
