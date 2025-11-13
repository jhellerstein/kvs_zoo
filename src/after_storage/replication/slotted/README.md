# Slot-based replication (after_storage/replication/slotted)

This directory contains replication strategies and wrappers that explicitly preserve and operate on slot metadata. Slots are produced by ordering layers (e.g., Paxos) and are required for strong ordering/linearizability. Keeping these in a submodule lets you read the common-case replication code (broadcast, gossip) without slot-specific details.

## What’s here

- LogBasedDelivery<R>
  - Wrapper that adds slot-based total ordering to any `ReplicationStrategy<V>`.
  - Disseminates slotted updates via the inner strategy, then performs gap-filling sequencing so operations are applied in slot order.
- SlottedBroadcastReplication<V>
  - Broadcast variant that preserves slots end-to-end for slotted streams.
  - Delegates unordered `replicate_data` to the vanilla `BroadcastReplication<V>` to avoid duplication.

Both are re-exported at `kvs_zoo::after_storage::replication::*`, so you can import:

```rust
use kvs_zoo::after_storage::replication::{LogBasedDelivery, SlottedBroadcastReplication};
```

Or explicitly via the submodule:

```rust
use kvs_zoo::after_storage::replication::slotted::{LogBasedDelivery, SlottedBroadcastReplication};
```

## When to use these

Use slot-based replication when you:
- Run a consensus/ordering layer (e.g., Paxos) that tags ops with slot numbers.
- Require linearizable behavior where GETs must observe prior PUTs across replicas.
- Want to keep the server wiring agnostic and let leaf components enforce per-slot ordering.

If you don’t need strong ordering, prefer the common-case strategies:
- `BroadcastReplication<V>`
- `SimpleGossip<V>`

## Quick wiring sketch

Here’s a minimal pattern for a linearizable two-layer setup using Paxos for ordering and slotted replication at the cluster layer:

```rust
use kvs_zoo::before_storage::{ordering::paxos::{PaxosDispatcher, PaxosConfig}, SlotOrderEnforcer};
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::after_storage::replication::{LogBasedDelivery, SlottedBroadcastReplication};
use kvs_zoo::kvs_layer::{KVSCluster, KVSNode};
use kvs_zoo::values::LwwWrapper;

// Cluster layer uses LogBased over a slotted broadcast strategy
type ClusterRepl<V> = LogBasedDelivery<SlottedBroadcastReplication<V>>;

// Two-layer KVS: cluster (routing + replication) -> leaf (slot enforcement)
type LinearizableKVS = KVSCluster<
    MyClusterTag,
    RoundRobinRouter,
    ClusterRepl<LwwWrapper<String>>,
    KVSNode<MyLeafTag, SlotOrderEnforcer, ()>,
>;
```

On the server wiring side, preserve slot metadata with `Envelope<slot, op>` and use `wire_two_layer_from_inputs` so the leaf can enforce ordering:

```rust
let enveloped = slotted_ops.map(q!(|(slot, op)| kvs_zoo::protocol::Envelope::new(slot, op)));
let responses = kvs_zoo::server::wire_two_layer_from_inputs(&proxy, &layers, &kvs, enveloped);
```

## Important behavior notes

- The default `ReplicationStrategy::replicate_slotted_data` (in `after_storage::mod`) strips slots and reattaches a dummy slot. That path is for “unordered replication only” and does not guarantee slot order. For correct slot ordering, use `LogBasedDelivery` (and if you need broadcast dissemination, pair it with `SlottedBroadcastReplication`).
- `SlottedBroadcastReplication` only overrides the slotted method; its unordered `replicate_data` behaves exactly like `BroadcastReplication`.

## See it in action

- Example: `examples/linearizable.rs`
- Test: `tests/linearizable_paxos_slot_buffer_tests.rs`

These demonstrate: (1) generating slotted ops via Paxos, (2) wiring to preserve slot metadata, and (3) enforcing leaf-level per-slot ordering so GETs don’t overtake prior PUTs.
