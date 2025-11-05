# KVS Zoo Examples - Clean Architecture

## Overview

The examples directory has been cleaned up to contain only the **latest, standardized examples** using the composable server architecture. All outdated and duplicate examples have been removed.

## Current Examples (4 Total)

### 1. `local.rs` - LocalKVSServer
- **Server Type**: `LocalKVSServer<String>`
- **Architecture**: Single node with LWW semantics
- **Nodes**: 1
- **Use Case**: Development, testing, simple applications

### 2. `replicated.rs` - ReplicatedKVSServer  
- **Server Type**: `ReplicatedKVSServer<CausalString>`
- **Architecture**: 3 replicas with epidemic gossip
- **Nodes**: 3
- **Use Case**: High availability, fault tolerance

### 3. `sharded.rs` - ShardedKVSServer<LocalKVSServer>
- **Server Type**: `ShardedKVSServer<LocalKVSServer<String>>`
- **Architecture**: 3 shards, each being a single node
- **Nodes**: 3
- **Use Case**: Horizontal scalability

### 4. `sharded_replicated.rs` - ShardedKVSServer<ReplicatedKVSServer> (**The Holy Grail!**)
- **Server Type**: `ShardedKVSServer<ReplicatedKVSServer<CausalString>>`
- **Architecture**: 3 shards × 3 replicas = 9 total nodes
- **Nodes**: 9
- **Use Case**: Web-scale applications requiring both scalability and fault tolerance

## Removed Examples

The following outdated examples were removed during cleanup:

- ❌ `local_composable.rs` - Duplicate of `local.rs`
- ❌ `replicated_composable.rs` - Duplicate of `replicated.rs`  
- ❌ `composable_working.rs` - Experimental prototype, superseded by main examples
- ❌ `examples/driver/` - Empty directory

## Standard Naming Convention

All examples now follow a consistent naming pattern:
- **Architecture type**: `local`, `replicated`, `sharded`, `sharded_replicated`
- **File extension**: `.rs`
- **No prefixes or suffixes**: Clean, descriptive names

## Running Examples

```bash
# Single node
cargo run --example local

# Replicated cluster  
cargo run --example replicated

# Sharded (3 nodes)
cargo run --example sharded

# Sharded + Replicated (9 nodes) - The Holy Grail!
cargo run --example sharded_replicated
```

## Validation

All examples are validated by:
- ✅ **Compilation**: `cargo check --examples`
- ✅ **Integration Tests**: `cargo test --test composable_integration`
- ✅ **Runtime Testing**: Each example runs successfully
- ✅ **Documentation**: Updated README.md with current architecture

## Benefits of Cleanup

1. **🧹 Clarity**: No duplicate or outdated examples
2. **📏 Consistency**: Standard naming convention
3. **🎯 Focus**: Only the essential, working examples
4. **📚 Documentation**: Clear, up-to-date README
5. **🔧 Maintainability**: Easier to maintain and understand

The examples directory now provides a clean, focused demonstration of the composable server architecture capabilities.