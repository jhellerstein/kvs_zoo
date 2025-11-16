# Metadata & Background Pipeline Quickstart

The current pipeline exposes two primary streams from `KVSCore`—**DataEvent** and **MetaEvent**—plus an optional **Background** stage for long-lived maintenance consumers.

---

## Streams at a Glance

| Stream      | Producer            | Typical Consumers                        | Notes |
|-------------|---------------------|-------------------------------------------|-------|
| `DataEvent` | `KVSCore::process`  | After pipeline (client responses, replication) | Read-side effects clients care about (Put/Delete/Get). |
| `MetaEvent` | `KVSCore::process`  | Background stages, After stages that opt-in | Tombs, summaries, reclaim frontiers, compaction digests, and future metadata. |

`MetaEvent` variants are intentionally small. Phase 1 (current) includes:

```rust
pub enum MetaEvent {
    Tomb { key: String },
    TombSummary { total_tombs: usize, last_tomb_key: Option<String> },
    ReclaimFrontier { frontier_seq: Option<u64>, epoch: u64 },
    CompactionDigest { format: MetaDigestFormat, bytes: Vec<u8> },
}
```

The matching digest descriptor is:

```rust
pub enum MetaDigestFormat {
    TombIndexJsonV1,
}
```

---

## Wiring a Background Stage

1. Implement `MetaBackground<V>` for your stage. You receive the cluster handle plus the data & meta streams. Return the streams you want propagated upstream (typically the data stream unmodified, metadata tapped or transformed).
2. Assign the background stage on your `KVSCluster` instance before calling `plumb_kvs_dataflow`.

Example: the shipped `TombIndexBackground` keeps simple tomb stats and can emit summaries for tests.

```rust
use kvs_zoo::background::TombIndexBackground;

let mut spec = ReplicatedKVS::default();
spec.background = TombIndexBackground::new()
    .with_logging(true)
    .with_summaries(true);
```

Under the hood `KVSCluster::plumb_background` walks the layer tree, giving each stage a chance to attach to the shared metadata stream.

---

## Background vs After Stages

- **After** stages still handle client-facing behavior (replication strategies, response transforms). They may ignore metadata entirely.
- **Background** stages run out-of-band. They receive the same event references but are free to batch, delay, or emit their own metadata (for example, `TombIndexBackground` now emits JSON digests via `MetaEvent::CompactionDigest`).
- Nothing in the metadata stream requires mutexes—the pipeline stays lock-free by design. Stages opt-in to cloning if they need ownership.

---

## Legacy Control Channel Status

The old `after_storage::control` module, `CtrlMsg` enum, and associated property tests have been removed. Any docs or examples that referenced “control channels” should now refer to “metadata” or “background” stages instead. Historical notes live in git history; new work should rely on the `MetaEvent` stream and `background` module.

---

## Next Steps

- Build a concrete `ReclaimFrontier` consumer/background stage that reacts to the new meta events and coordinates tomb retention.
- Extend property/integration tests to cover the digest payload contract (JSON schema) and any downstream consumers.
- Document guidance for downstream systems on decoding digests (e.g. CLI example, metrics hook).

See `docs/background_pipeline_plan.md` for the full roadmap.
