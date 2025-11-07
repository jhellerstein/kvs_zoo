# Log-Based Replication

## Problem

When using unordered replication strategies (like `BroadcastReplication` or `EpidemicGossip`) with consensus protocols (like Paxos), operations can arrive out of order at different replicas, causing divergence:

```
1. Op1 (slot 0) sent to Node1, delayed in network
2. Op2 (slot 1) sent to Node2  
3. Node2 replicates Op2 to Node1
4. Node1 receives Op2 (slot 1) before Op1 (slot 0)
```

Result: Node1 and Node2 apply operations in different orders.

## Solution

The `LogBased<R>` wrapper adds slot-based ordering guarantees to any replication strategy:

- Operations are replicated with their slot numbers
- Each replica buffers operations that arrive early (gaps in sequence)
- Operations are applied only when all prior slots are filled
- Maintains total order across all replicas

## Design

### Wrapper Pattern

`LogBased<R>` wraps any existing replication strategy:

```rust
// Unordered broadcast (has the bug)
BroadcastReplication

// Log-based broadcast (fixed)
LogBased<BroadcastReplication>

// Log-based gossip (also works)
LogBased<EpidemicGossip>
```

### Trait Extension

The `ReplicationStrategy` trait now supports both:

1. **Unordered replication**: `replicate_data()` - existing behavior
2. **Slotted replication**: `replicate_slotted_data()` - new method for ordered replication

Default implementation of `replicate_slotted_data()` strips slots and uses unordered replication (loses ordering). `LogBased` provides proper gap-filling implementation.

## Usage

### In Linearizable KVS

```rust
use kvs_zoo::replication::{LogBased, BroadcastReplication};

// Create log-based broadcast replication
let replication = LogBased::new(BroadcastReplication::new());

// Use with linearizable KVS
type LinearizableKVS = LinearizableKVSServer<
    LwwWrapper<String>,
    LogBased<BroadcastReplication<LwwWrapper<String>>>
>;
```

### Composition Examples

```rust
// Log-based broadcast (most common for linearizable systems)
LogBased<BroadcastReplication<V>>

// Log-based gossip (for eventual consistency with ordering)
LogBased<EpidemicGossip<V>>

// Unordered broadcast (original behavior, has ordering bug with consensus)
BroadcastReplication<V>

// Unordered gossip (original behavior)
EpidemicGossip<V>
```

## Implementation Details

### Gap-Filling Algorithm

Based on `hydro/hydro_test/src/cluster/kv_replica/sequence_payloads.rs`:

1. **Buffer**: Collect operations in a cycle
2. **Sort**: Order by slot number within each tick
3. **Track**: Maintain next expected slot number
4. **Process**: Apply contiguous operations, buffer gaps
5. **Repeat**: Continue until all gaps filled

### Key Components

- `replicate_slotted_data()`: Main entry point for slotted replication
- `disseminate_slotted()`: Delegates to inner strategy for dissemination
- `sequence_slotted_operations()`: Implements gap-filling logic

## Files Modified

- `kvs_zoo/src/replication/mod.rs`: Added `replicate_slotted_data()` method
- `kvs_zoo/src/replication/logbased.rs`: New `LogBased<R>` wrapper
- `kvs_zoo/src/linearizable.rs`: Updated to use slotted replication
- `kvs_zoo/examples/linearizable.rs`: Updated to use `LogBased<BroadcastReplication>`

## Testing

All existing tests pass. New tests added for:
- `LogBased` wrapper creation and composition
- Trait implementations
- Type safety
