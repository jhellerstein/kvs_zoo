# KVS Zoo 🦁

A collection of progressively more sophisticated Key-Value Store implementations built with [Hydro](https://github.com/hydro-project/hydro), designed as educational examples for an upcoming book about distributed programming.

The **KVS Zoo** demonstrates how to build distributed systems using Hydro's global dataflow programming model and a **composable architecture** that separates operation interception from data replication, progressing from simple local stores to sophisticated sharded and replicated architectures with causal consistency.

## 📚 Background

This project builds on prior distributed systems research:

- **[Anna KVS](https://github.com/hydro-project/anna)**: A lattice-based key-value store emphasizing coordination-free semantics (original papers [here](https://dsf.berkeley.edu/jmh/papers/anna_ieee18.pdf) and [here](https://www.vldb.org/pvldb/vol12/p624-wu.pdf)
- **[Hydro Project](https://hydro.run/)**: A Rust framework for correct and performance distributed systems

The implementations showcase Hydro's approach to building distributed systems: expressing coordination patterns as location-aware, low-latency dataflow graphs.

## 🏗️ Architecture

The zoo includes several KVS variants, each building on the previous:

### 1. **Local KVS** (`local.rs`)

A single-node key-value store with last-writer-wins semantics.

- **Example**: `examples/local.rs`
- **Concepts**: Basic Hydro dataflow, external interfaces, process/cluster abstraction

### 2. **Replicated KVS** (`replicated.rs`)

Multi-node replication with causal consistency, using Demers-style rumor-mongering gossip protocol and lattice wrappers.

- **Example**: `examples/replicated.rs`
- **Concepts**: Gossip protocols, eventual consistency, lattice-based merge semantics
- **Features**:
  - Periodic gossip with configurable intervals
  - Probabilistic tombstoning for rumor cleanup
  - Separation of rumor metadata and lattice-merged values
  - Uses `LatticeKVSCore` for coordination-free convergence

### 3. **Sharded KVS** (`sharded.rs`)

Horizontal partitioning via consistent hashing for scalability.

- **Example**: `examples/sharded.rs`
- **Concepts**: Data partitioning, hash-based routing, independent shards
- **Features**:
  - Consistent key-to-shard mapping
  - Proxy-based routing with unicast to specific shards
  - No cross-shard communication (independent operation)

### 4. **Sharded + Replicated KVS** (`sharded_replicated.rs`)

Combines sharding and replication for both scalability and fault tolerance.

- **Example**: `examples/sharded_replicated.rs`
- **Concepts**: Hybrid architecture, multi-cluster coordination
- **Features**:
  - Multiple shard clusters, each with internal replication
  - Gossip within each shard for consistency
  - Partitioning across shards for capacity



## 🧪 Core Components

### Lattice-Based Semantics

- **`lattice_core.rs`**: Coordination-free KVS using lattice merge operations
- **`vector_clock.rs`**: Vector clocks for causal ordering
- **Causal Values**: `DomPair<VCWrapper, SetUnion<T>>` for causally-consistent multi-values

### Supporting Infrastructure

- **`protocol.rs`**: Common KVS operations (Get/Put)
- **`core.rs`**: Traditional last-write-wins KVS core
- **`examples_support.rs`**: Shared demo helpers for causal operations

## 🚀 Getting Started

### Prerequisites

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone the repository
git clone https://github.com/jhellerstein/kvs_zoo.git
cd kvs_zoo
```

### Build

```bash
# Build the library and examples
cargo build --examples

# Run tests (includes causal consistency, sharding, gossip protocol tests)
cargo test
```

### Run Examples

Each example demonstrates a different KVS architecture:

```bash
# Local single-node KVS
cargo run --example local

# Replicated KVS with 3-node gossip cluster
cargo run --example replicated

# Sharded KVS with 3 independent shards
cargo run --example sharded

# Sharded + Replicated (3 shard clusters, each with 3 replicas)
cargo run --example sharded_replicated


```

Most examples use **causal consistency** with vector-clock-timestamped values that merge via set union.

## 📖 Example Walkthrough

Here's what happens when you run the replicated example:

```rust
use kvs_zoo::replicated::ReplicatedKVSServer;
use kvs_zoo::examples_support::{CausalString, generate_causal_operations, log_operation};

// Set up a 3-node replicated cluster
let (client_input_port, _, _) =
    ReplicatedKVSServer::<CausalString>::run_replicated_kvs(
        &proxy, &kvs_cluster, &client_external
    );

// Generate operations with vector-clock timestamps
// PUT "alpha" => {"a1"} @ VC[node1: 1]
// PUT "alpha" => {"a2"} @ VC[node2: 1]  (concurrent!)
// PUT "beta"  => {"b1"} @ VC[node3: 1]
let operations = generate_causal_operations();

// Send operations through the client interface
for op in operations {
    log_operation(&op);
    client_sink.send(op).await?;
}
```

**Output** shows:

- Operations distributed across replicas
- Gossip propagation between nodes
- Concurrent writes merging via set union (both "a1" and "a2" retained)
- Causal ordering preserved by vector clocks

## 🧬 Key Design Patterns

### 1. **Lattice Merge Semantics**

Values implement the `Merge` trait, allowing coordination-free convergence:

```rust
impl<V: Merge<V>> LatticeKVSCore {
    pub fn put(operations: Stream<KVSOperation<V>, ...>)
        -> (KeyedSingleton<String, V, ...>, Stream<String, ...>)
}
```

### 2. **Gossip with Rumor Store**

Demers-style gossip separates metadata from values:

- **Rumor Store**: Tracks which keys have been updated (using `MapUnionWithTombstones`)
- **Lattice Store**: Holds actual merged values
- **Optimization**: Only gossip keys, fetch values from local lattice store

### 3. **Proxy-Based Routing**

Sharding uses a specialized proxy for deterministic routing:

```rust
operations
    .map(|op| {
        let shard_id = hash(key) % shard_count;
        (MemberId::from_raw(shard_id), op)
    })
    .into_keyed()
    .demux_bincode(cluster)
```



## 📊 Consistency Spectrum

The KVS Zoo demonstrates the spectrum of consistency models:

| Variant            | Consistency      | Coordination        | Latency    | Fault Tolerance  |
| ------------------ | ---------------- | ------------------- | ---------- | ---------------- |
| Local              | N/A              | None                | Lowest     | None             |
| Replicated         | Eventual         | Gossip              | Low        | High             |
| Sharded            | Per-Key          | Hash routing        | Low        | Medium           |
| Sharded+Replicated | Causal           | Gossip+Hash         | Medium     | High             |

## 🧪 Testing

The test suite includes:

- **Protocol tests**: Basic Get/Put operations
- **Vector clock tests**: Causality tracking, concurrent updates, merge semantics
- **Causal consistency tests**: Happens-before relationships, convergence
- **Sharding tests**: Key distribution, shard independence

```bash
# Run all tests
cargo test

# Run with nextest for better output
cargo install cargo-nextest
cargo nextest run
```

## 🤝 Contributing

This project is designed for educational purposes as part of a distributed systems book. Contributions that improve clarity, add documentation, or demonstrate additional distributed systems concepts are welcome!

## 📚 Related Resources

- [Hydro Documentation](https://hydro.run/docs/)
- [Anna: A KVS for Any Scale](https://arxiv.org/abs/1809.00089)
- [Conflict-free Replicated Data Types (CRDTs)](https://crdt.tech/)
- [Vector Clocks](https://en.wikipedia.org/wiki/Vector_clock)
- [Demers Gossip Protocol](https://www.cs.cornell.edu/people/egs/cornellonly/syslunch/spring05/andre.pdf)

## 📄 License

This project is designed as educational material for the Hydro distributed programming book.
