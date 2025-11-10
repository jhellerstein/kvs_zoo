# Dispatch Combinations Guide

This guide makes explicit how to compose routing and replication to achieve common topologies and properties. It’s meant for learning and design conversations—not to hide details.

## Notation

- Routing:
  - `SingleNodeRouter`
  - `RoundRobinRouter` (replica selection)
  - `ShardedRouter` (key space partitioning)
  - `Pipeline<A, B>` (compose A then B)
- Replication (examples):
  - `()` (no background replication)
  - `NoReplication` (explicit no-op strategy)
  - `EpidemicGossip<V>`
  - `BroadcastReplication<V>`
  - `LogBased<R>` (used with Paxos/certified append)
- Value types: `LwwWrapper<String>`, `CausalString`, etc.

## Topology → Composition → Properties

1. Single node

- Composition: `KVSServer<LwwWrapper<String>, SingleNodeRouter, ()>`
- Properties: Strong local ordering (deterministic). No replication.
- When: Local dev, deterministic tests, simple demos.

2. N-way replicated (load balanced)

- Composition: `KVSServer<CausalString, RoundRobinRouter, EpidemicGossip<CausalString>>`
- Properties: Eventual consistency with causal semantics (merge on conflict). Fault tolerance via replication.
- Alternatives:
  - Swap `EpidemicGossip` for `BroadcastReplication` for faster convergence under low churn.
  - Use `LwwWrapper<String>` for last-write-wins.

3. Sharded (partitioned only)

- Composition: `KVSServer<LwwWrapper<String>, Pipeline<ShardedRouter, SingleNodeRouter>, ()>`
- Properties: Horizontal partitioning (S shards, 1 replica each). Per-shard single node.
- When: Scale reads/writes by key-space partitioning without replication.

4. Sharded + Replicated (S shards × R replicas)

- Composition: `KVSServer<CausalString, Pipeline<ShardedRouter, RoundRobinRouter>, BroadcastReplication<CausalString>>`
- Properties: Throughput via sharding; availability via replication; eventual convergence.
- Notes: ShardedRouter decides shard; RoundRobinRouter chooses replica in that shard.

5. Linearizable via Paxos (single group)

- Composition: `KVSServer<LwwWrapper<String>, PaxosDispatcher<LwwWrapper<String>>, LogBased<BroadcastReplication<LwwWrapper<String>>>>`
- Properties: Linearizable writes/reads via consensus; replication via log-based propagation.
- When: Strong consistency requirements.

## Practical Tips

- Choosing V: Pick value semantics first (e.g., `CausalString` for CRDT-like sets; `LwwWrapper` for overwrite semantics).
- Replication vs routing: Routing decides where operations go immediately; replication decides how state propagates across nodes (in the background, or immediately).
- Pipelines: Use `Pipeline<ShardedRouter, RoundRobinRouter>` when you need both partitioning and per-partition replication.
- Testing: Start with `()` replication or `NoReplication` for isolation, then layer in replication mechanics.

## Example Type Aliases (mirroring code)

- Local: `type Local = KVSServer<LwwWrapper<String>, SingleNodeRouter, ()>;`
- Replicated (gossip): `type ReplicatedGossip = KVSServer<CausalString, RoundRobinRouter, EpidemicGossip<CausalString>>;`
- Sharded Local: `type ShardedLocal = KVSServer<LwwWrapper<String>, Pipeline<ShardedRouter, SingleNodeRouter>, ()>;`
- Sharded Replicated: `type ShardedReplicated = KVSServer<CausalString, Pipeline<ShardedRouter, RoundRobinRouter>, BroadcastReplication<CausalString>>;`
