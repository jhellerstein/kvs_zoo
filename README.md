# KVS Zoo 🦁 🐵 🦒

A collection of progressively more sophisticated Key-Value Store implementations built with [Hydro](https://github.com/hydro-project/hydro), designed as educational examples for an upcoming book about distributed programming.

The **KVS Zoo** demonstrates how to build distributed systems using Hydro's global dataflow programming model and a composable server architecture that allows mixing and matching
request-routing layers, replication layers, and value semantics to create sophisticated distributed key-value stores from reusable components.

## 📚 Background

This project builds on prior distributed systems research:

- **[Anna KVS](https://github.com/hydro-project/anna)**: A lattice-based key-value store emphasizing coordination-free semantics (original papers [here](https://dsf.berkeley.edu/jmh/papers/anna_ieee18.pdf) and [here](https://www.vldb.org/pvldb/vol12/p624-wu.pdf)
- **[Hydro Project](https://hydro.run/)**: A Rust framework for correct and performance distributed systems

The implementations showcase Hydro's approach to building distributed systems: expressing coordination patterns as location-aware, low-latency dataflow graphs that provide zero-overhead _distributed safety_ at compile time.

## 🏗️ Architecture

The zoo showcases a composable architecture where routing layers, replication layers, and value semantics can be mixed and matched.

### Data and Metadata Streams

The core emits two orthogonal event streams:

- `DataEvent<V>` captures observable effects such as put/delete responses (and future streaming reads).
- `MetaEvent` carries background metadata such as tomb notifications or summary statistics.

Pipeline stages subscribe to the streams they care about:

- **Before** stages run sequentially just before storage, routing and ordering `KVSOperation<V>` requests.
- **After** stages run sequentially just after storage, consuming `DataEvent<V>` and optionally `MetaEvent` when replication needs the extra context.
- **Background** stages (opt-in) attach to either `DataEvent<V>` or `MetaEvent` streams for longer-lived background work.

Stages branch by cloning Hydro streams, which could potentially be optimized in future.

### Core Abstractions

- **Composable layers**:
  - Before storage (routing/ordering): `SingleNodeRouter`, `RoundRobinRouter`, `ShardedRouter`, `PaxosDispatcher`, and `Pipeline<...>` to compose them
  - After storage (replication/responders): `NoReplication`, `SimpleGossip`, `BroadcastReplication`, and `SequencedReplication<R>` wrapper for slot-ordered delivery
- **Single entrypoint**: `layer_flow` (for focused wiring) and `plumb_kvs_dataflow` (for end-to-end server wiring with external I/O)
- **Unified replication API**: all strategies implement `replicate_updates` over `ReplicationUpdate<V>` so the same code handles slotted (sequenced) and unslotted updates
- **Value Types**: `LwwWrapper<T>` (last-writer-wins), `CausalWrapper<T>` (causal with vector clocks)

### Example Architectures

### 1. **Local KVS** (`examples/local.rs`)

Single-node key-value store with sequential semantics.

- **Routing**: `SingleNodeRouter`
- **Replication**: `NoReplication`
- **Nodes**: 1
- **Concepts**: Basic Hydro dataflow, external interfaces, process/cluster abstraction

### 2. **Replicated KVS** (`examples/replicated.rs`)

Multi-node replication with selectable eventual consistency model.

- **Routing**: `RoundRobinRouter`
- **Replication**: `SimpleGossip` (epidemic rumor-mongering)
- **Value Types**:
  - `CausalString` (default) - _causal_ consistency with vector clocks
  - `LwwWrapper<String>` (via `--lattice lww`) - last-writer-wins _non-deterministic_ consistency
- **Nodes**: 3 replicas
- **Concepts**: Gossip protocols, eventual consistency, lattice-based merge semantics
- **Features**:
  - Periodic gossip with configurable intervals
  - Probabilistic tombstoning for rumor cleanup
  - Runtime selection of consistency model via CLI flag

### 3. **Sharded KVS** (`examples/sharded.rs`)

Horizontal partitioning via consistent hashing for scalability.

- **Routing**: `Pipeline<ShardedRouter, SingleNodeRouter>`
- **Replication**: `NoReplication` (per-shard)
- **Nodes**: 3 shards
- **Concepts**: Data partitioning, hash-based routing, independent shards
- **Features**:
  - Consistent key-to-shard mapping
  - Proxy-based routing with unicast to specific shards
  - No cross-shard communication (independent operation)

### 4. **Sharded + Replicated KVS** (`examples/sharded_replicated.rs`)

Combines sharding and replication for both scalability and fault tolerance.

- **Routing**: `Pipeline<ShardedRouter, RoundRobinRouter>`
- **Replication**: `BroadcastReplication` (within each shard)
- **Nodes**: 3 shards × 3 replicas = 9 total nodes
- **Concepts**: Hybrid architecture, multi-level composition
- **Features**:
  - Multiple shard clusters, each with internal replication
  - Broadcast replication within each shard for consistency
  - Partitioning across shards for capacity

### 5. **Linearizable KVS** (`examples/linearizable.rs`)

Imposes a total order with Paxos while keeping background replication pluggable.

- **Routing/Ordering**: `PaxosDispatcher` (consensus before execution)
- **Replication**: `SequencedReplication<BroadcastReplication>` (gap-filling, slot-ordered delivery)
- **Nodes**: 3 Paxos proposers + 3 Paxos acceptors + 3 KVS replicas = 9 total
- **Concepts**: Consensus, linearizability, slot-preserving replication
- **Features**:
  - Paxos imposes a global slot order on operations
  - Replication preserves slots and enforces in-order application per replica
  - Strong consistency guarantees

## 🧪 Core Components

### Composable wiring (`src/wiring.rs` and `src/layer_flow.rs`)

KVS architectures are built by composing before_storage and after_storage layers, then wiring them with a single entrypoint:

```rust
// Create external I/O and plumb the full stack
let (layers, port) = plumb_kvs_dataflow::<LwwWrapper<String>, _>(
  &proxy,
  &client_external,
  &flow,
  kvs_spec, // e.g., a nested KVSCluster spec built from routing/replication components
);
```

For focused scenarios (e.g., tests), `layer_flow` plumbs a single cluster with selected routing and replication over a stream of `KVSOperation<V>`.

### Value Semantics (`src/values/`)

- **`LwwWrapper<T>`**: Last-writer-wins (simple overwrite)
- **`CausalWrapper<T>`**: Causal consistency using `DomPair<VCWrapper, SetUnionHashSet<T>>`
- **`VCWrapper`**: Vector clock primitive for causality tracking

### Replication Strategies (`src/after_storage/`)

- **`NoReplication`**: No background synchronization
- **`SimpleGossip<V>`**: Demers-style rumor-mongering with probabilistic tombstoning
- **`BroadcastReplication<V>`**: Eager broadcast of all updates
- **`SequencedReplication<R>`**: Slot-ordered delivery wrapper over another replication strategy (gap-filling based on sequence/slot)
- Unified over `ReplicationUpdate<V>` via `replicate_updates`

Notes:
- Replication strategies implement a unified `replicate_updates` API over `ReplicationUpdate<V>` so they can handle both slotted and unslotted updates without duplication.

### Routing/Ordering Strategies (`src/before_storage/`)

- **`SingleNodeRouter`**: Direct to single node
- **`RoundRobinRouter`**: Load balance across replicas
- **`ShardedRouter`**: Hash-based key partitioning
- **`PaxosDispatcher`**: Global total order via Paxos consensus
- **`Pipeline<R1, R2>`**: Compose two routing strategies

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
cargo run --example replicated                      # Causal consistency (default)
cargo run --example replicated -- --lattice causal  # Explicit causal
cargo run --example replicated -- --lattice lww     # Last-write-wins

# Sharded KVS with 3 independent shards
cargo run --example sharded

# Sharded + Replicated (3 shards × 3 replicas = 9 nodes)
cargo run --example sharded_replicated

# Linearizable KVS with Paxos consensus
cargo run --example linearizable
```

See `examples/README.md` for detailed documentation on each architecture.

## 📖 Example Walkthrough

Here's what happens when you run the replicated example with causal consistency:

```bash
cargo run --example replicated -- --lattice causal
```

The example:

1. **Deploys** a 3-node replicated cluster with `SimpleGossip`
2. **Sends** operations with causal values (vector clock + value):
   ```rust
   PUT "doc" => CausalString { vc: {node1: 1}, val: "v1" }
   PUT "doc" => CausalString { vc: {node2: 1}, val: "v2" }  // Concurrent!
   GET "doc"
   ```
3. **Gossips** updates between replicas periodically
4. **Merges** concurrent writes via set union (both "v1" and "v2" retained)
5. **Prints** responses showing the merged causal value

**Output** demonstrates:

- Operations routed round-robin across replicas
- Causal ordering preserved by vector clocks
- Concurrent writes converging via lattice merge
- Eventual consistency through gossip propagation

## 🧬 Key Design Patterns

### 1. **Composable Architecture**

Servers, routing, replication, and values are independent dimensions:

```rust
// Mix and match components (spec-style)
type MyKVS = KVSCluster<
  ShardCluster,
  Pipeline<ShardedRouter, RoundRobinRouter>,        // before_storage
  BroadcastReplication<CausalString>,               // after_storage
  ()                                                // leaf
>;

let routing = Pipeline::new(ShardedRouter::new(3), RoundRobinRouter::new());
let replication = BroadcastReplication::<CausalString>::default();
```

### 2. **Lattice Merge Semantics**

Values implement the `Merge` trait for coordination-free convergence:

```rust
impl<V: Merge<V>> KVSCore<V> {
    // Concurrent writes automatically merge via lattice join
    fn put(&mut self, key: String, value: V) {
        self.store.merge_with(key, value);
    }
}
```

### 3. **Gossip with Rumor Store**

Epidemic gossip separates metadata from values:

- **Rumor Store**: Tracks which keys have been updated (metadata only)
- **Lattice Store**: Holds actual merged values
- **Optimization**: Only gossip keys, fetch values from local store

### 4. **Routing Pipelines**

Compose routing layers for multi-level architectures:

```rust
// First route by shard, then by replica within shard
let pipeline = Pipeline::new(
    ShardedRouter::new(num_shards),
    RoundRobinRouter::new()
);
```

## 📊 Consistency Spectrum

The KVS Zoo demonstrates the spectrum of consistency models:

| Variant             | Consistency      | Coordination    | Latency | Fault Tolerance | Nodes |
| ------------------- | ---------------- | --------------- | ------- | --------------- | ----- |
| Local               | Strong           | None            | Lowest  | None            | 1     |
| Replicated (LWW)    | Eventual         | Gossip          | Low     | High            | 3     |
| Replicated (Causal) | Causal           | Gossip          | Low     | High            | 3     |
| Sharded             | Per-shard strong | Hash routing    | Low     | Medium          | 3     |
| Sharded+Replicated  | Per-shard causal | Gossip + Hash   | Medium  | Very High       | 9     |
| Linearizable        | Linearizable     | Paxos consensus | Higher  | High            | 9     |

## 🧪 Testing

The test suite includes comprehensive validation:

- **Protocol tests**: Basic Get/Put operations (`tests/protocol_tests.rs`)
- **Vector clock tests**: Causality tracking, concurrent updates (`tests/vector_clock_tests.rs`)
- **Causal consistency tests**: Happens-before relationships, convergence (`tests/causal_consistency_tests.rs`)
- **Sharding tests**: Key distribution, shard independence (`tests/sharding_tests.rs`)
- **Replication tests**: Gossip protocol, convergence (`tests/replication_strategy_tests.rs`)
- **Linearizability tests**: Paxos consensus, total order (`tests/linearizability_tests.rs`)
- **Composability tests**: Server trait implementations (`tests/composable_integration.rs`)

```bash
# Run all tests
cargo test

# Run with nextest for better output
cargo install cargo-nextest
cargo nextest run

# Run specific test suite
cargo test causal_consistency
```

### Snapshot tests (insta)

We use the [`insta`](https://crates.io/crates/insta) crate to snapshot the user-facing stdout of examples in `tests/examples_snapshots.rs`. The test harness filters out unstable, internal process-launch noise; only semantically meaningful lines (banner, shard mapping lines, operation outputs, completion banners) are retained to keep snapshots stable over time.

Common workflow:

```bash
# Run snapshot tests normally
cargo test --test examples_snapshots -- --nocapture

# Review and accept updated snapshots interactively after intentional output changes
cargo insta review

# Force regenerate all snapshots (CI should not do this)
INSTA_UPDATE=always cargo test --test examples_snapshots
```

Commit updated `tests/snapshots/*.snap` files when outputs change intentionally. CI runs these to guard the educational surface of example output.

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
