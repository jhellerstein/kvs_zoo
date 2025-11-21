# Background Pipeline Status & Follow-Ups (November 2025)

This document replaces the sprawling restart plan with a concise snapshot that can ride in the PR description. For wiring details and examples, see [`docs/metadata_background_quickstart.md`](metadata_background_quickstart.md).

---

## Executive Summary

- `KVSCore::process` now emits distinct `DataEvent<V>` and `MetaEvent` streams and every helper (`plumb_kvs_dataflow`, detail examples) forwards both.
- `MetaEvent` covers `Tomb`, `TombSummary`, and JSON digests via `CompactionDigest`/`MetaDigestFormat::TombIndexJsonV1`.
- `TombIndexBackground` attaches through `MetaBackground`, logs stats optionally, emits summaries, and produces the digest payload for downstream maintenance.
- Integration coverage: `meta_stream_tests` exercise tomb emission, background plumb wiring, and digest surfacing; `cargo nextest run` passes (104 tests, 8 skipped).
- User-facing docs refreshed (README, example READMEs, quickstart) to point at the metadata/background story.

---

## Delivered Components

| Area | Highlights | Notes |
|------|------------|-------|
| Core events | `events.rs` defines `DataEvent`, `MetaEvent`, and `MetaDigestFormat` | designed for additive growth (scans, richer digests) |
| Background plumbing | `MetaBackground` + `BackgroundPlumb` traits walk the layer tree | background stages receive borrowed refs; no allocations when unused |
| Tomb index stage | `TombIndexBackground` tracks tomb counts, optional logging, emits summaries + JSON digest | digest encoded once via `serde_json::to_vec` |
| Examples/tests | All demos use new plumbing; `meta_stream_tests` assert tomb, summary, digest emission | serialised with `serial_test` to dodge Hydro trybuild races |
| Documentation | README + quickstart link to metadata/background primer | redundant plan trimmed to this note |

---

## Outstanding Follow-Ups

1. **Digest consumers** – provide a reference reader (CLI/tooling) that decodes `MetaDigestFormat::TombIndexJsonV1` and surfaces metrics.
2. **After-stage opt-in** – audit after-storage strategies for optional metadata handling (e.g., replication strategies emitting tomb awareness).
3. **Property tests** – rebuild the old control-channel property coverage around the new metadata stream semantics once digest consumers land.

These items stay on the backlog but are not required for the current PR.

---

## PR Checklist Snapshot

- [x] `cargo fmt`
- [x] `cargo nextest run`
- [x] Docs updated (`README`, examples, quickstart, this note)
- [x] No legacy control-channel references remain
- [x] Tests cover background plumbing (tombs, summaries, digests)

Keep this list in sync with the PR body when opening/refreshing the review.

---

## Historical Context

The previous 300-line plan (restored via Git history if needed) documented the branch restart from the legacy control channel. With the refactor now concrete, this lean summary should suffice; consult commit history for deep archeology.

# Background Pipeline & Stream Refactor Plan

This document captures the restart plan from `after-control-pipeline` toward a cleaner architecture with **Data** and **Metadata** streams plus an optional **Background** pipeline. It is intentionally self‑contained so we can delete legacy control-channel code without needing git archaeology.

---
## 1. Goals

1. Remove task‑specific control pipeline abstractions (AfterPipe, Pipe, CtrlRep, CtrlLocal).
2. Introduce a simple, pluggable dual-stream output from `KVSCore`: `DataEvent<V>` and `MetaEvent`.
3. Add an optional third pipeline slot **Background** for periodic / maintenance tasks.
4. Keep existing Delete/tombstone semantics (logical removal + tombstone set) while making tomb propagation a metadata concern.
5. Preserve ergonomic improvements (Default impls, example style).
6. Leave room for future operations like `Scan` and anti‑entropy without overfitting early (no forced digest formats).

---
## 2. High-Level Architecture

```
Client Ops (KVSOperation) ──> Before Pipeline ──> KVSCore.scan ─┬─> DataEvent<V> ─┬─> After Pipeline (data consumers)
                                                   │            │
                                                   │            └─> Background Pipeline (data consumers)
                                                   └─> MetaEvent ─┬─> After Pipeline (meta consumers)
                                                                  └─> Background Pipeline (meta consumers)
Timer/Triggers ───────────────────────────────────────────────────────────────────────────────────────────┘
```

- **Before**: synchronous per operation (routing/order). Single stream of `KVSOperation<V>`.
- **After**: fast path; may subscribe to DataEvents and/or MetaEvents. Receives borrowed references; cloning only if a stage explicitly needs ownership beyond the call.
- **Background**: maintenance path; may also subscribe to both event classes. Same borrowing semantics; cloning deferred until retention is requested.

---
## 3. Streams & Enums (Initial Minimal Set)

### DataEvent<V>
Represents the observable effects or responses of operations (NOT necessarily raw deltas).
```rust
pub enum DataEvent<V> {
    Put { key: String, value: V },
    Delete { key: String },
    Get { key: String, value: Option<V> },
    // Future:
    // ScanStart { token: u64, prefix: Option<String>, limit: Option<usize> },
    // ScanChunk { token: u64, entries: Vec<(String, V)> },
    // ScanEnd { token: u64, total: usize },
}
```

### MetaEvent
Opaque maintenance / system metadata. Extensible; no semantics hard-coded.
```rust
pub enum MetaEvent {
    Tomb { key: String },            // immediate tomb marker
    // Reclaim { key: String },      // emitted by Background later
    // TombSummary { format_id: u16, bytes: Vec<u8> }, // optional digest
    // Stat { name: &'static str, value: u64 },
    // Custom { topic: &'static str, payload: Vec<u8> },
}
```

---
## 4. Pipelines

We retain a generic `Pipeline<A,B>` (reuse existing Before composition). Top-level cluster type adds a background slot. Both After and Background pipelines subscribe independently to DataEvent and MetaEvent streams; absence of subscribers means zero cloning.
```rust
pub struct KVSCluster<Name, Bf, Af, Bg, Child> {
    pub before: Bf,
    pub after: Af,
    pub background: Bg, // Bg = () for none initially; later Pipeline<...>
    pub child: Child,
}
```
Examples:
```rust
// Replicated only
type Replicated = KVSCluster<Replica, RoundRobinRouter, BroadcastReplication<CausalString>, (), ()>;

// Replicated + background tomb maintenance
type ReplicatedWithBg = KVSCluster<Replica, RoundRobinRouter, BroadcastReplication<CausalString>, Pipeline<TombAntiEntropy, Reclaimer>, ()>;

// Sharded + replicated + background inner only
type Inner = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<CausalString>, Pipeline<TombAntiEntropy, Reclaimer>, ()>;
type ShardedReplicatedWithBg = KVSCluster<Shard, ShardedRouter, (), (), Inner>;
### Unified Subscription Traits
```rust
pub trait DataConsumer<D> { fn on_data(&mut self, ev: &D); }
pub trait MetaConsumer<M> { fn on_meta(&mut self, meta: &M); }
pub trait DataMetaConsumer<D,M>: DataConsumer<D> + MetaConsumer<M> {}
impl<T,D,M> DataMetaConsumer<D,M> for T where T: DataConsumer<D> + MetaConsumer<M> {}
```
Dispatcher keeps `Vec<&mut dyn DataConsumer<_>>` and `Vec<&mut dyn MetaConsumer<_>>`. Emission path:
1. Borrow event once.
2. Invoke all consumers with `&event`.
3. If one or more consumers declare ownership need (future extension: `fn needs_owned(&self)->bool`), perform a single clone and hand out references to the cloned instance for those consumers.
No cloning occurs when there are zero subscribers for a category.
```

---
## 5. Tombstone Flow

1. On `KVSOperation::Delete`, `KVSCore` emits:
   - `DataEvent::Delete { key }`
   - `MetaEvent::Tomb { key }`
2. **After** may replicate Tomb immediately (low latency safety).
3. **Background** builds tomb index from Tomb events; periodically emits future `Reclaim` events (added later).
4. Anti-entropy for tombs (optional) becomes a Background stage (e.g., `TombAntiEntropy`) consuming tombs and producing digests or retransmits.

No digest format is mandated; if implemented, it is purely additional `MetaEvent` variants.

---
## 6. Scan Support (Future Extension)

When adding `KVSOperation::Scan`:
- Emit `ScanStart`, then `ScanChunk` items, finishing with `ScanEnd` inside DataEvent.
- After can forward or transform response ordering; Background typically ignores unless doing analytics.

---
## 7. Implementation Phases

| Phase | Action | Notes |
|-------|--------|-------|
| 1 | Branch off main (`metadata-background-pipeline`) | Fresh start; no cherry-pick history necessary. |
| 2 | Copy in Delete + tombstone logic from feature branch | Manual extraction; keep tests/behavior. |
| 3 | Re-add Default impls (copy code) | Preserve ergonomics only. |
| 4 | Introduce `events.rs` with enums | No wiring yet. |
| 5 | Add background field to `KVSCluster` (unused) | Placeholder `()` initially. |
| 6 | Strip control pipeline examples and types | Remove AfterPipe / CtrlRep / CtrlLocal code entirely. |
| 7 | Adjust examples (replicated_with_tombstone minimal) | Show Delete → NOT FOUND only. |
| 8 | Build & run smoke tests | `cargo check`, run examples. |
| 9 | Document decisions & open PR | Link to this plan; invite feedback. |
| 10 | Wire Metadata emission in `KVSCore` | ✅ emits `DataEvent::Delete` + `MetaEvent::Tomb` to shared background handle (Arc/Mutex). |
| 11 | Add optional After tomb replication stage | TODO — reuse existing broadcast strategy with `DataConsumer` impl. |
| 12 | Add Background tomb index stage | Phase 0 stub (`BgTombIndex`) logging meta; upgrade to real index later. |
| 13 | Future: Reclaimer stage + Reclaim events | Controlled by retention policy. |

---
## 8. Risk & Mitigation

| Risk | Mitigation |
|------|------------|
| Loss of experimental control code | This doc + archived branch tag. |
| Over-simplification of future needs | Extensible enums; Background optional. |
| Performance unknowns (extra meta stream) | Keep initial meta volume low (only Tomb). Benchmark later. |
| Scan complexity later | Reserved DataEvent variants; additive change. |
| Tomb ordering vs Put races | Emit Tomb immediately on Delete in same operator path. |

---
## 9. Open Questions (to revisit)
- Epoch / version tagging for Tomb events? (Add field once we define global frontier semantics.)
- Do we need separate streams for latency-sensitive vs bulk metadata? (Potential optimization later.)
- Should Background get a changefeed (DataEvent) or rely solely on MetaEvent? (Decide after initial tomb index implementation.)

---
## 10. Immediate Next Actions

1. Feed shared background handle into After pipeline once a consumer opts in.
2. Expose background handle for tests/examples (optional introspection API).
3. Update README/examples as wiring solidifies (done for tombstone phase 0).
4. Phase 0 shipped with `TombIndexBackground` (logs + summary Tomb metadata) to exercise the background hook.
5. Prep PR with notes on Arc/Mutex background hand-off.

---
## 10a. Execution Tracker (Live)

| Status | Item | Notes |
|--------|------|-------|
| ✅ | Background plumbing trait (`MetaBackground`) established in `src/background/mod.rs` | Cluster exposes `plumb_background`; `TombIndexBackground` compiling. |
| ✅ | Cargo workspace builds (`cargo check`) | Verified after module move. |
| ✅ | Relocate tomb background stages from `after_storage/cleanup` into `background/` | `TombIndexBackground` now lives under `src/background/`. |
| ✅ | Rename `after_storage::control` API surface to metadata-centric naming | Simplified to `events::MetaEvent`; legacy `after_storage::meta` shim removed. |
| ✅ | Update docs/tests to drop "control channel" phrasing | Added `docs/metadata_background_quickstart.md`; naming aligned with Metadata/background terminology. |
| ✅ | Point meta stream plumbing away from After-specific control helpers | Removed legacy MaintenanceReplicator trait + impls; cross_layer_flow now surfaces data/meta for the background tee wiring. |
| ✅ | Expand integration tests for metadata consumers | Added `background_plumb_routes_meta_events` to ensure stages observe tomb meta via BackgroundPlumb. |

_Update this table as each step is completed to keep the branch audit-ready._

---
## 11. Success Criteria (Initial PR)
- Builds successfully (cargo check).
- Examples run (local, replicated, sharded) with unchanged behavior for Put/Get/Delete.
- Tombstone Delete still results in NOT FOUND on subsequent Get.
- No references to AfterPipe / CtrlRep / CtrlLocal remain.
- `events.rs` present but unused (foundation only).

---
## 12. Deferred Work (Tracked separately)
- After pipeline Tomb propagation (if After opts into meta stream).
- Background tomb index persistence/statistics beyond logging.
- Reclaim logic + retention configuration.
- Scan operation support.
- Anti-entropy (digest or incremental sync) as optional Background stage.
- Rebuild metadata pipeline property tests (legacy control suite removed).

---
## 13. Rationale for Dropping Control Channel Implementation
The prior control pipeline added abstraction layers (AfterPipe, Pipe, CtrlRep, CtrlLocal) that conflated immediate replication and periodic maintenance, increasing API surface and cognitive load. Recasting into Data + Metadata streams with an optional Background pipeline separates concerns cleanly and conforms more directly to Hydro’s stream-based model.

---
## 14. Reference Snippets (For Upcoming Implementation)

DataEvent emission (conceptual):
```rust
match op {
    KVSOperation::Put(key, val) => {
        store.insert(key.clone(), val.clone());
        emit_data(DataEvent::Put { key, value: val });
    }
    KVSOperation::Delete(key) => {
        store.remove(&key);
        tombs.insert(key.clone());
        emit_data(DataEvent::Delete { key: key.clone() });
        emit_meta(MetaEvent::Tomb { key });
    }
    KVSOperation::Get(key) => {
        let v = store.get(&key).cloned();
        emit_data(DataEvent::Get { key, value: v });
    }
    // Scan later...
}
```

Background tomb index (conceptual):
```rust
fn tomb_index_stage(meta: Stream<MetaEvent>) -> TombSet {
    // fold tomb events into a set; periodically output stats
}
```

---
## 15. Contact / Edit Notes
This plan is authoritative for the restart. Amend here before implementing major deviations.

---
## 16. Minimal After → Background Tee (Phase 0)

We intentionally defer all advanced optimization (batching, filtering, cloning strategies) and start with **the simplest possible Tee** so we can wire the Background pipeline early and iterate.

### Purpose
Duplicate each `DataEvent<V>` after commit/visibility into the Background pipeline *if* a background receiver is registered, without altering the primary After path or introducing buffering/latency.

### Minimal Contract (Local Single-Thread Runtime)
- Input: `DataEvent<V>` from upstream.
- Foreground call: `next.on_data(&ev)`.
- Background tap: if a background data consumer is present, `bg.on_data(&ev)`.
- MetaEvents dispatched via the unified dispatcher using the same borrow pattern.
- No cloning unless a consumer needs ownership (not part of Phase 0).

### Skeleton Type
```rust
pub struct TeeAfter<D, Next, BgData> {
    pub next: Next,
    pub bg_data: Option<BgData>,
    _pd: std::marker::PhantomData<D>,
}

impl<D, Next, BgData> TeeAfter<D, Next, BgData>
where
    Next: DataConsumer<D>,
    BgData: DataConsumer<D>,
{
    pub fn on_data(&mut self, ev: &D) {
        self.next.on_data(ev);          // foreground path
        if let Some(bg) = &mut self.bg_data { bg.on_data(ev); } // background tap
    }
}
```

Notes:
1. No cloning required if stages treat `E` immutably; we pass by reference.
2. If a background stage needs ownership later, it can internally clone selectively.
3. Registration = set `tee.background = Some(stage)`; removal = set back to `None`.
4. Placement: Typically the final After stage before returning results; can be earlier if subsequent After stages must also observe tapped events.

### Integration Steps (Phase 0)
1. Define `AfterStage` & `BackgroundStage` traits (if not already present) using by-reference invocation.
2. Add `TeeAfter` struct in `after/tee.rs` (or similar location).
3. Cluster builder inserts `TeeAfter` as last After stage with `background: None` by default.
4. Provide `enable_background(bg_stage)` that sets the Option.
5. Background pipeline begins with a stage implementing `BackgroundStage<DataEvent<V>>` (e.g., tomb index builder).
6. MetaEvent wiring remains unchanged; Tee only duplicates DataEvent.

### Follow-On (Deferred)
Add: filtering, metrics, backpressure policies, Arc-sharing, batching, summarization. These remain explicitly out of scope for this first implementation.

This minimal Tee unblocks experimentation with Background stages (tomb indexing, reclamation) without committing to a heavier abstraction prematurely.

---
## 17. Unified Event Dispatcher (Future Enhancement)

Centralizing emission removes ad hoc tees once multiple consumers appear:
```rust
pub struct EventDispatcher<D,M> {
    data_consumers: Vec<Box<dyn DataConsumer<D>>>,
    meta_consumers: Vec<Box<dyn MetaConsumer<M>>>,
}

impl<D: Clone, M: Clone> EventDispatcher<D,M> {
    pub fn emit_data(&mut self, ev: D) {
        if self.data_consumers.is_empty() { return; }
        for c in &mut self.data_consumers { c.on_data(&ev); }
        // Ownership optimization later (single clone if needed_owned())
    }
    pub fn emit_meta(&mut self, meta: M) {
        if self.meta_consumers.is_empty() { return; }
        for c in &mut self.meta_consumers { c.on_meta(&meta); }
    }
}
```
Phase 0 keeps manual Tee; dispatcher becomes Phase N when complexity warrants.

