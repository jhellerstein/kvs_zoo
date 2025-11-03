# Replicated Key-Value Store with Gossip Protocol

This module implements a replicated key-value store using an all-to-all gossip protocol built with the Hydro dataflow framework.

## Architecture

The replicated KVS consists of:

1. **Coordinator Process**: Handles external client communication
2. **KVS Cluster**: A cluster of nodes that maintain replicated state
3. **Gossip Protocol**: Ensures eventual consistency across all nodes

## Key Components

### `ReplicatedKVSServer<V>`

The main server implementation that provides:

- `run_gossip_cluster_with_coordinator()`: Sets up the complete replicated system
- `gossip_kvs()`: Core gossip protocol implementation

### Gossip Protocol Design

The gossip protocol works as follows:

1. **Operation Distribution**: Client operations are distributed to cluster nodes via the coordinator using round-robin
2. **All-to-All Broadcasting**: Each PUT operation is immediately broadcast to ALL other nodes using `broadcast_bincode`
3. **Local Processing**: Each node processes operations locally using the shared `KVSCore` logic
4. **Gossip Reception**: Nodes receive gossip messages containing PUT operations from other nodes
5. **Stream Merging**: Local and gossip PUTs are merged using Hydro's `chain` method for complete state
6. **Eventual Consistency**: All nodes converge to identical state containing all PUT operations
7. **Response Aggregation**: Results are collected back to the coordinator and sent to clients

### Key Implementation Details

- **Gossip Subset**: Currently configured to broadcast to ALL nodes (100% coverage)
- **Propagation Delay**: 1 tick + network latency for full cluster propagation
- **Broadcast Method**: Uses Hydro's `broadcast_bincode` for reliable all-to-all communication
- **Stream Merging**: Uses Hydro's `chain` method to merge local and gossip streams
- **Consistency Model**: Eventual consistency with last-write-wins semantics
- **Complete Replication**: Every node receives and processes every PUT operation from every other node

## Usage

### Basic Setup

```rust
use kvs_zoo::replicated::StringReplicatedKVSServer;

let flow = hydro_lang::compile::builder::FlowBuilder::new();
let coordinator = flow.process();
let kvs_cluster = flow.cluster();
let client_external = flow.external();

let (input_port, results_port, fails_port) =
    StringReplicatedKVSServer::run_gossip_cluster_with_coordinator(
        &coordinator,
        &kvs_cluster,
        &client_external
    );
```

### Running the Example

```bash
cargo run --example replicated
```

This demonstrates:

- Setting up a 3-node replicated cluster
- Sending various KVS operations
- Observing eventual consistency in action

## Consistency Guarantees

The gossip protocol provides:

- **Eventual Consistency**: All nodes will eventually converge to the same state
- **Partition Tolerance**: The system continues to operate even if some nodes are temporarily unreachable
- **High Availability**: Operations can be processed as long as at least one node is available

## Implementation Notes

### Shared Core Logic

The implementation reuses the `KVSCore` from the local KVS module, providing:

- `ht_build_cluster()`: Builds key-value stores from operation streams
- `ht_query_cluster()`: Handles GET operations with batching
- `batch_gets_cluster()`: Batches GET operations for efficiency

### Hydro-Specific Features

The implementation leverages several Hydro constructs:

- **Clusters**: For distributed node management
- **Streams**: For operation processing
- **KeyedSingletons**: For state management
- **Ticks**: For batching and consistency

### Ordering and Determinism

The gossip protocol handles non-deterministic ordering through:

- `assume_ordering(nondet!())`: Explicitly marking non-deterministic streams
- Commutative operations where possible
- Eventual consistency rather than strong consistency

## Testing

The module includes comprehensive tests:

- `test_gossip_kvs_cluster()`: Tests the complete gossip protocol
- Integration with the existing KVS test suite

## Future Enhancements

Potential improvements include:

- Vector clocks for causal ordering
- Conflict resolution strategies
- Configurable gossip intervals
- Membership management
- Anti-entropy mechanisms

## Educational Value

This implementation demonstrates:

- How to build distributed systems with Hydro
- Gossip protocol design patterns
- Eventual consistency in practice
- Code reuse between local and distributed implementations
