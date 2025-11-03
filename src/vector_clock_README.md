# Vector Clock Implementation

This module provides a `VectorClock` lattice type for tracking causality in distributed systems.

## Overview

A vector clock is a data structure used to determine the partial ordering of events in a distributed system. Each node maintains a vector (map) of logical timestamps, one for each node in the system. This allows the system to determine if events are causally related or concurrent.

## Implementation Details

The `VectorClock` is implemented as a wrapper around `MapUnionHashMap<String, Max<usize>>`, which provides:

- **Lattice merge semantics**: When merging two vector clocks, takes the maximum counter for each node ID
- **Causal ordering**: Supports `happened_before()` to determine if one event causally precedes another
- **Concurrency detection**: Supports `is_concurrent()` to detect concurrent events

## Usage Example

```rust
use ide_test::vector_clock::VectorClock;
use lattices::Merge;

// Node 1 performs an operation
let mut vc1 = VectorClock::new();
vc1.bump("node1".to_string());  // vc1: {node1: 1}

// Node 2 performs an operation
let mut vc2 = VectorClock::new();
vc2.bump("node2".to_string());  // vc2: {node2: 1}

// These events are concurrent
assert!(vc1.is_concurrent(&vc2));

// Node 1 receives node 2's vector clock and merges
vc1.merge(vc2.clone());  // vc1: {node1: 1, node2: 1}

// Node 1 performs another operation
vc1.bump("node1".to_string());  // vc1: {node1: 2, node2: 1}

// Now vc2 happened before vc1
assert!(vc2.happened_before(&vc1));
```

## Use with LatticeKVSCore

The `VectorClock` type can be used as the value type in `LatticeKVSCore` since it implements the `Merge` trait:

```rust
use ide_test::lattice_core::LatticeKVSCore;
use ide_test::protocol::KVSOperation;
use ide_test::vector_clock::VectorClock;

// Create operations with vector clock values
let ops = vec![
    KVSOperation::Put("key1".to_string(), vc1),
    KVSOperation::Put("key1".to_string(), vc2),
];

// The KVS will automatically merge vector clocks using lattice semantics
let kvs = LatticeKVSCore::put(ops.into_stream());
```

## Lattice Properties

The `VectorClock` satisfies the lattice properties:

- **Idempotence**: Merging a clock with itself doesn't change it
- **Commutativity**: `vc1 ∨ vc2 = vc2 ∨ vc1`
- **Associativity**: `(vc1 ∨ vc2) ∨ vc3 = vc1 ∨ (vc2 ∨ vc3)`

These properties are verified in the test suite.
