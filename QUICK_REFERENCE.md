# KVS Zoo Quick Reference 🦁

## 🚀 Running Examples

```bash
# Basic architectures (unified API)
cargo run --example local              # Single node
cargo run --example replicated         # Multi-replica + gossip
cargo run --example sharded            # Hash partitioning
cargo run --example sharded_replicated # Sharding + replication

# Advanced architecture (Paxos consensus)
cargo run --example linearizable       # Linearizable consistency

# Development
cargo check --examples                 # Check all compile
cargo test                             # Run tests
```

## 🏗️ Creating Your Own Architecture

### 1. Define Your Demo
```rust
struct MyDemo;

impl KVSDemo for MyDemo {
    type Value = String;           // What values to store
    type Storage = MyKVS;          // How to store (LwwKVS, ReplicatedKVS, etc.)
    type Router = MyRouter;        // How to route (LocalRouter, ShardedRouter, etc.)
    
    fn create_router<'a>(&self, flow: &FlowBuilder<'a>) -> Self::Router {
        MyRouter::new(/* config */)
    }
    
    fn cluster_size(&self) -> usize { 3 }
    
    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        vec![
            KVSOperation::Put("key1".to_string(), "value1".to_string()),
            KVSOperation::Get("key1".to_string()),
        ]
    }
}
```

### 2. Run Your Demo
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo(MyDemo).await
}
```

## 📦 Available Components

### Storage Types
```rust
// Simple last-writer-wins
type Storage = LwwKVS;

// Replicated with epidemic gossip
type Storage = EpidemicReplicatedKVS<String>;

// Replicated with broadcast
type Storage = BroadcastReplicatedKVS<String>;

// Basic sharded storage
type Storage = ShardedKVS;
```

### Router Types
```rust
// Broadcast to all nodes (single node or testing)
type Router = LocalRouter;

// Round-robin distribution
type Router = RoundRobinRouter;

// Hash-based sharding
type Router = ShardedRouter;
```

## 🔧 Common Patterns

### Unified API Examples
The following examples all use the same unified driver API:

### Local Development
```rust
struct LocalDemo;
impl KVSDemo for LocalDemo {
    type Storage = LwwKVS;
    type Router = LocalRouter;
    fn cluster_size(&self) -> usize { 1 }
}
```

### High Availability
```rust
struct ReplicatedDemo;
impl KVSDemo for ReplicatedDemo {
    type Storage = KVSReplicatedEpidemic<String>;
    type Router = RoundRobinRouter;
    fn cluster_size(&self) -> usize { 3 }
}
```

### Scalability
```rust
struct ShardedDemo;
impl KVSDemo for ShardedDemo {
    type Storage = KVSLww;
    type Router = ShardedRouter;
    fn create_router(&self, _flow) -> Self::Router {
        ShardedRouter::new(3)  // 3 shards
    }
    fn cluster_size(&self) -> usize { 3 }
}
```

### Web Scale
```rust
struct WebScaleDemo;
impl KVSDemo for WebScaleDemo {
    type Storage = KVSReplicatedEpidemic<String>;  // Replicated per shard
    type Router = ShardedRouter;                   // Hash routing
    fn cluster_size(&self) -> usize { 3 }
}
```

## 🔒 Advanced: Linearizable KVS

The linearizable example uses a **different architecture** (not the unified API) because consensus algorithms require fundamentally different abstractions:

### Paxos-based Linearizable KVS
```rust
// Located in src/linearizable/ module
// Uses Multi-Paxos consensus for total ordering
// Architecture: Proxy → Proposers → Acceptors → Replicas
// Provides linearizability (strongest consistency)

// Run with:
cargo run --example linearizable

// Key differences from unified API:
// - Multi-cluster architecture (4 separate clusters)
// - External client integration
// - Consensus protocol vs. simple replication
// - Higher latency but strongest consistency guarantees
```

## 🎯 Key Files

```
src/
├── core.rs              # KVSCore - fundamental operations
├── protocol.rs          # KVSOperation and KVSResponse types
├── server.rs            # KVSServer<V, Storage, Router>
├── kvs_types.rs         # 🏷️ Convenient type aliases for common configurations
├── lww.rs               # LwwKVS - simple storage
├── replicated.rs        # ReplicatedKVS - with gossip
├── sharded.rs           # 🔀 ShardableKVS trait + all implementations + ShardedKVS
├── values/              # 🎁 Value wrappers & consistency semantics (unified)
│   ├── mod.rs           #   └── Unified API + comprehensive docs
│   ├── lww.rs           #   └── LwwWrapper - last-writer-wins
│   ├── causal.rs        #   └── CausalWrapper - causal consistency
│   ├── vector_clock.rs  #   └── VCWrapper - causality tracking
│   └── utils.rs         #   └── Utility functions for examples
├── linearizable/        # 🔒 Advanced: Paxos-based linearizable KVS
│   ├── mod.rs           #   └── Module documentation
│   ├── paxos.rs         #   └── Core Multi-Paxos implementation
│   ├── paxos_with_client.rs #   └── Client integration
│   └── linearizable.rs  #   └── Linearizable KVS implementation
└── routers/             # 🧭 All routing strategies (unified)
    ├── mod.rs           #   └── KVSRouter trait + re-exports
    ├── local.rs         #   └── LocalRouter - broadcast to all
    ├── round_robin.rs   #   └── RoundRobinRouter - round-robin distribution
    ├── sharded.rs       #   └── ShardedRouter - hash-based
    ├── gossip.rs        #   └── EpidemicGossip - epidemic spreading
    └── broadcast.rs     #   └── BroadcastReplication - reliable broadcast

examples/
├── driver/mod.rs        # run_kvs_demo() - unified driver
├── local.rs             # Single-node example
├── replicated.rs        # Multi-replica example
├── sharded.rs           # Sharded example
├── sharded_replicated.rs # Combined example
└── linearizable.rs      # 🔒 Advanced: Paxos consensus example
```

## 🏷️ Pre-configured Types

The `kvs_types.rs` module provides convenient aliases for common configurations:

### Common Configurations (Clean KVS-Prefix Naming)
```rust
use kvs_zoo::kvs_types::*;

// Local development
type MyKVS = StringKVSLocalLww;

// High availability
type MyKVS = StringKVSReplicatedEpidemicGossip;

// Scalability
type MyKVS = StringKVSShardedLww;

// Web scale (scalability + availability)
type MyKVS = StringKVSShardedReplicatedEpidemicGossip;
```

### Architecture Categories (Clean KVS-Prefix Naming)
- **Local**: `KVSLocalLww<V>` - Single-node configurations
- **Replicated**: `KVSReplicatedEpidemicGossip<V>` - Multi-replica with gossip
- **Sharded**: `KVSShardedLww<V>` - Hash-partitioned for scalability
- **Sharded+Replicated**: `KVSShardedReplicatedEpidemicGossip<V>` - Combined approach

## 🎁 Value Wrappers & Consistency

The `values/` module provides different consistency semantics:

### Available Value Types
```rust
// Last-writer-wins (simple overwrite)
use kvs_zoo::values::LwwWrapper;
let lww = LwwWrapper::new("value".to_string());

// Causal consistency with vector clocks
use kvs_zoo::values::{CausalWrapper, VCWrapper};
let mut vc = VCWrapper::new();
vc.bump("node1".to_string());
let causal = CausalWrapper::new(vc, "value".to_string());

// Vector clocks for causality tracking
use kvs_zoo::values::VCWrapper;
let mut vc = VCWrapper::new();
vc.bump("node_id".to_string());
```

### Usage with Storage
```rust
// Use any value wrapper with any storage
type LwwKVS = KVSServer<LwwWrapper<String>, LwwStorage, LocalRouter>;
type CausalKVS = KVSServer<CausalString, ReplicatedStorage, RoundRobinRouter>;
```

## 🚀 Extending the System

### Add New Storage
```rust
pub struct KVSMy;  // Follow KVS-prefix naming

impl KVSMy {
    pub fn put<'a, V>(...) -> KeyedSingleton<...> {
        // Your storage logic here
        crate::core::KVSCore::put(processed_tuples)
    }
    
    pub fn get<'a, V>(...) -> Stream<...> {
        crate::core::KVSCore::get(keys, state)
    }
}

impl<V> KVSShardable<V> for KVSMy {  // Use new trait name
    type StorageType = V;
    fn put(...) { Self::put(...) }
    fn get(...) { Self::get(...) }
}
```

### Add New Router
```rust
pub struct MyRouter;

impl<V> KVSRouter<V> for MyRouter {
    fn route_operations<'a>(...) -> Stream<...> {
        // Your routing logic here
    }
}
```

## 📚 Learning Resources

- **[Architecture Guide](ARCHITECTURE.md)** - Detailed technical documentation
- **[Examples Guide](examples/README.md)** - How to run examples
- **Source code** - Extensively commented for learning

## 🎓 Concepts Demonstrated

- **CAP Theorem**: Consistency vs Availability vs Partition tolerance
- **Consistency Models**: Strong, causal, eventual consistency
- **Replication**: Gossip protocols, epidemic spreading
- **Partitioning**: Hash-based sharding, consistent hashing
- **Fault Tolerance**: Network partitions, node failures
- **Dataflow Programming**: Hydro's stream-based approach

---

**Quick start**: `cargo run --example local` and explore! 🦁✨