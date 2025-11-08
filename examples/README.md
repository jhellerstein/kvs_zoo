# KVS Zoo Examples

This directory contains examples demonstrating different KVS server architectures using the composable server framework.

## Server Architecture Examples

All examples use the **composable server architecture** from `kvs_zoo::server`, showcasing how different server types can be composed to create sophisticated distributed systems.

### Core Server Types

- **`local.rs`** - LocalKVSServer (Single Node)

  - **Architecture**: Single process, no networking
  - **Nodes**: 1
  - **Consistency**: Strong (deterministic)
  - **Use case**: Development, testing, simple applications

- **`replicated.rs`** - ReplicatedKVSServer (Replicated Cluster)

  - **Architecture**: Multiple replicas with epidemic gossip
  - **Nodes**: 3 replicas
  - **Consistency**: Causal (default) or LWW (with `--lattice` flag)
  - **Use case**: High availability, fault tolerance
  - **Flags**:
    - `--lattice causal` - Uses CausalString with vector clock (default)
    - `--lattice lww` - Uses LwwWrapper with last-write-wins semantics

- **`sharded.rs`** - ShardedKVSServer<LocalKVSServer> (Sharded)

  - **Architecture**: Hash-based key partitioning
  - **Nodes**: 3 shards (3 total nodes)
  - **Consistency**: Per-shard strong, global eventual
  - **Use case**: Horizontal scalability for large datasets

- **`sharded_replicated.rs`** - ShardedKVSServer<ReplicatedKVSServer> (**The Holy Grail!**)
  - **Architecture**: Sharded + replicated (nested composition)
  - **Nodes**: 3 shards × 3 replicas = 9 total nodes
  - **Consistency**: Per-shard causal, cross-shard eventual
  - **Use case**: Web-scale applications requiring both scalability and fault tolerance

### Composable Server Framework

All examples demonstrate the use of the `KVSServer<V>` trait:

```rust
pub trait KVSServer<V> {
    type Deployment<'a>;
    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a>;
    fn run<'a>(...) -> ServerPorts<V>;
    fn size() -> usize;
}
```

This enables architectural composability, where any server can be used as a building block for more complex architectures.

## Running Examples

```bash
# Basic architectures
cargo run --example local

# Distributed architectures
cargo run --example replicated                      # Uses causal consistency by default
cargo run --example replicated -- --lattice causal  # Explicit causal with vector clocks
cargo run --example replicated -- --lattice lww     # Last-write-wins semantics

cargo run --example sharded
cargo run --example sharded_replicated
```

## Architecture Comparison

| Example                 | Server Type                                           | Nodes | Consistency Model | Scalability | Fault Tolerance |
| ----------------------- | ----------------------------------------------------- | ----- | ----------------- | ----------- | --------------- |
| `local.rs`              | `LocalKVSServer<String>`                              | 1     | Strong            | Low         | None            |
| `replicated.rs`         | `ReplicatedKVSServer<CausalString>` or `<LwwWrapper>` | 3     | Causal or LWW     | Medium      | High            |
| `sharded.rs`            | `ShardedKVSServer<LocalKVSServer<String>>`            | 3     | Per-shard strong  | High        | Low             |
| `sharded_replicated.rs` | `ShardedKVSServer<ReplicatedKVSServer<CausalString>>` | 9     | Per-shard causal  | Very High   | Very High       |

## Key Benefits

- **🔧 Composability**: Servers can be nested and combined arbitrarily
- **📈 Scalability**: From 1 node to 9+ nodes with the same patterns
- **🛡️ Fault Tolerance**: Replication provides resilience to node failures
- **⚡ Performance**: Sharding distributes load across multiple nodes
- **🎯 Flexibility**: Mix and match consistency models and architectures

## Integration Tests

All examples are validated by comprehensive integration tests in `tests/composable_integration.rs` that verify correctness of operations and consistency guarantees.
