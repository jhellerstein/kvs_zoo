> **Historical note**: The API described below referenced the now-removed `after_storage::control` / `after_storage::pipeline` modules. `events::MetaEvent` and the background pipeline supersede this design; keep this document for archaeology only.

Love it. Here’s a tight, two-channel API that keeps Data exactly as-is, makes Control purely opt‑in, supports multiple control handlers (replication and local maintenance), and never routes Control through the KVSOperation stream.

## Core types (short names)

- Data stays your existing ReplicationStrategy<V>.
- Control is a separate, opt‑in stream with a union enum.

```rust
// Control payloads live in after_storage::control
pub enum CtrlMsg {
    Tomb(TombMeta),
    Reclaim(ReclaimMeta),
    // future variants...
}

pub struct TombMeta { pub key: String, pub seq: Option<u64> }
pub struct ReclaimMeta { pub frontier_seq: Option<u64>, pub epoch: u64 }
```

## Stage traits (short, composable)

- DataStage: wraps any existing ReplicationStrategy<V>.
- CtrlRep: replicates control messages (e.g., broadcast/gossip) – separate channel.
- CtrlLocal: consumes control locally (scheduling, maintenance) – separate channel.
- Pipe: sequential composition. AfterPipe makes a pipe look like your After type.

```rust
// Wrap an existing data replicator; unchanged examples keep using ReplicationStrategy<V>
pub struct DataStage<R>(pub R);

// Replicate control messages over the network (NOT via KVSOperation)
pub struct CtrlRep<CR>(pub CR);

// Handle control locally (maintenance/timers); can be used alongside CtrlRep
pub struct CtrlLocal<L>(pub L);

// Composition
pub struct Pipe<A, B>(pub A, pub B);
pub struct AfterPipe<P>(pub P);
```

Contracts (conceptual, not verbose):
- DataStage<R>: data_in -> R.replicate_data(data_in)
- CtrlRep<CR>: ctrl_in -> CR.replicate_ctrl(ctrl_in)
- CtrlLocal<L>: ctrl_in -> L.handle_ctrl(ctrl_in)
- Pipe<A,B>: feeds A then B for both channels
- AfterPipe<P>: implements ReplicationStrategy<V> for Data; Control is routed only if present

## How the Control channel is consumed

- Replication: implement replicate_ctrl(ctrl: Stream<CtrlMsg>) -> Stream<CtrlMsg> on your chosen protocol (e.g., BroadcastReplication<CtrlMsg>).
- Local: implement handle_ctrl(ctrl: Stream<CtrlMsg>) -> Stream<CtrlMsg> (you can pass-through, enrich, or siphon side-effects on timers).
- The Control path never touches the KVSOperation stream; it’s a parallel stream inside after_storage.

## Zero-changes by default

Existing examples remain unchanged: After = ReplicationStrategy<V>.

```rust
// unchanged example
type ReplicatedKVS = KVSCluster<
  Replica,
  RoundRobinRouter,
  SimpleGossip<LwwWrapper<String>>,
  ()
>;
```

## Turn Control on only when you want it

You swap the After type to an AfterPipe that includes control stages. Data syntax stays identical; Control is opt‑in. You can include both CtrlRep and CtrlLocal (in any order via Pipe).

### Replicated example with tombstone maintenance

```rust
use kvs_zoo::after_storage::control::{CtrlMsg, TombMeta, ReclaimMeta};
use kvs_zoo::after_storage::replication::{BroadcastReplication, BroadcastReplicationConfig};
use kvs_zoo::after_storage::pipeline::{DataStage, CtrlRep, CtrlLocal, Pipe, AfterPipe};

// Data replicator unchanged
type D = DataStage<SimpleGossip<LwwWrapper<String>>>;

// Control replicator (e.g., broadcast control plane)
type CRep = CtrlRep<BroadcastReplication<CtrlMsg>>;

// Local maintenance (e.g., reclamation manager)
struct Reclaim; // your local handler
type CLoc = CtrlLocal<Reclaim>;

type ReplicatedWithMaint = KVSCluster<
  Replica,
  RoundRobinRouter,
  AfterPipe<Pipe<D, Pipe<CRep, CLoc>>>, // Data first, then Control Rep, then Control Local
  ()
>;

// In main:
let data = DataStage(SimpleGossip::new(100usize));
let ctrl_rep = CtrlRep(BroadcastReplication::with_config(BroadcastReplicationConfig::low_latency()));
let ctrl_loc = CtrlLocal(Reclaim { /* cfg */ });

let mut kvs_spec: ReplicatedWithMaint = Default::default();
kvs_spec.after = AfterPipe(Pipe(data, Pipe(ctrl_rep, ctrl_loc)));
```

Notes:
- Data still uses SimpleGossip<LwwWrapper<String>> exactly as today.
- CtrlMsg is your control union; you can add variants over time.
- CtrlRep and CtrlLocal run on the Control channel only and can both be present.

### Sharded + replicated: Control only on inner layer

```rust
type ShardedReplicatedWithMaint = KVSCluster<
  Shard,
  ShardedRouter,
  (),
  KVSCluster<
    Replica,
    RoundRobinRouter,
    AfterPipe<Pipe<
      DataStage<BroadcastReplication<CausalString>>,
      CtrlRep<BroadcastReplication<CtrlMsg>>
    >>,
    ()
  >
>;

let data = DataStage(BroadcastReplication::with_config(BroadcastReplicationConfig::low_latency()));
let ctrl_rep = CtrlRep(BroadcastReplication::with_config(BroadcastReplicationConfig::low_latency()));

let mut kvs_spec: ShardedReplicatedWithMaint = Default::default();
kvs_spec.child.after = AfterPipe(Pipe(data, ctrl_rep));
```

## Producing control messages (where do they come from?)

- You split KVSOperation upstream into:
  - Data stream (Put/updates) → DataStage
  - Control stream (e.g., Tomb(key), ReclaimFrontier) → Ctrl stages
- This split happens inside after_storage wiring when you opt in; there’s no change to KVSCore or to examples unless you choose AfterPipe.

For local maintenance without replication:
- Use AfterPipe(Pipe(DataStage(...), CtrlLocal(Reclaim {...}))) only; omit CtrlRep entirely.

For replication-only control (no local handling yet):
- Use AfterPipe(Pipe(DataStage(...), CtrlRep(BroadcastReplication::<CtrlMsg>::...))).

## Why this fits your asks

- Short names; “Data” favored over “Value”.
- Two channels only: Data (existing) and Control (opt‑in).
- Multiple handling paths for Control: replication and local maintenance can both be present.
- Control never goes through the KVSOperation stream.
- Examples remain unchanged unless you opt in by swapping After to AfterPipe and adding Ctrl stages.

If you want, I can scaffold the tiny adapters (DataStage, CtrlRep, CtrlLocal, Pipe, AfterPipe) and a minimal CtrlMsg enum as placeholders in after_storage so you can try the type aliases right away.