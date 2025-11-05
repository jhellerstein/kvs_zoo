# KVS Server Architecture

## Overview

The KVS Zoo now uses a **composable server architecture** as the standard way to build distributed key-value stores. This architecture replaces the previous server.rs implementation with a more flexible, composable approach.

## Core Concept

The architecture is built around the `KVSServer<V>` trait, which provides a uniform interface for different server types:

```rust
pub trait KVSServer<V> {
    type Deployment<'a>;
    
    fn create_deployment<'a>(flow: &FlowBuilder<'a>) -> Self::Deployment<'a>;
    fn run<'a>(proxy: &Process<'a, ()>, deployment: &Self::Deployment<'a>, client_external: &External<'a, ()>) -> ServerPorts<V>;
    fn size() -> usize;
}
```

## Server Types

### 1. LocalKVSServer<V>
- **Architecture**: Single node with LWW semantics
- **Nodes**: 1
- **Use case**: Development, testing, simple applications
- **Consistency**: Strong (single node)

### 2. ReplicatedKVSServer<V>
- **Architecture**: Multiple replicas with epidemic gossip
- **Nodes**: 3 (configurable)
- **Use case**: High availability, fault tolerance
- **Consistency**: Causal (with vector clocks)

### 3. ShardedKVSServer<T>
- **Architecture**: Sharded system using any underlying server type T
- **Nodes**: 3 × T.size()
- **Use case**: Horizontal scalability
- **Consistency**: Per-shard strong, global eventual

## Composability

The key innovation is **true composability**. Servers can be nested:

```rust
// Simple servers
LocalKVSServer<String>           // 1 node
ReplicatedKVSServer<String>      // 3 nodes

// Composed servers  
ShardedKVSServer<LocalKVSServer<String>>      // 3 nodes (3 shards × 1 node each)
ShardedKVSServer<ReplicatedKVSServer<String>> // 9 nodes (3 shards × 3 replicas each)
```

## Examples

All examples now use the composable server architecture:

- `examples/local.rs` - LocalKVSServer
- `examples/replicated.rs` - ReplicatedKVSServer  
- `examples/sharded.rs` - ShardedKVSServer<LocalKVSServer>
- `examples/sharded_replicated.rs` - ShardedKVSServer<ReplicatedKVSServer> (**The Holy Grail!**)

## Integration Tests

Comprehensive integration tests validate correctness:

- `test_local_kvs_service()` - Tests LocalKVSServer
- `test_replicated_kvs_service()` - Tests ReplicatedKVSServer with causal consistency
- `test_sharded_kvs_service()` - Tests ShardedKVSServer<LocalKVSServer>
- `test_sharded_replicated_kvs_service()` - Tests ShardedKVSServer<ReplicatedKVSServer>

## Benefits

1. **True Composability**: Any server can be used as a building block for sharding
2. **Feature Inheritance**: Features added to base servers automatically work at all composition levels
3. **Clean Abstractions**: Each server type encapsulates its complexity
4. **Uniform Interface**: All servers implement the same `KVSServer` trait
5. **Scalable Architecture**: From 1 node to 9+ nodes with the same code patterns

## Migration

The old server.rs has been completely replaced. The new architecture:

- Uses `kvs_zoo::server` module instead of `kvs_zoo::composable`
- Provides `LocalKVSServer`, `ReplicatedKVSServer`, and `ShardedKVSServer` types
- Maintains backward compatibility through type aliases in `kvs_types.rs`
- Offers cleaner, more maintainable code with better separation of concerns

This composable server architecture represents the evolution of the KVS Zoo into a truly modular, scalable distributed systems framework.