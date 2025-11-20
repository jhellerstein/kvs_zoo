# Tombstone Pruning via Vector Clocks — Plan & Tracking

## Objective
Use vector clocks (VCs) to decide when tombstones are safe to forget, with a tidy, composable Hydro pipeline: VC background → replication → local frontier maintenance → prune.

## Scope
- Integrate VC background signals with a local frontier (merged VCs per key).
- Update tombstone cleanup to use VC frontier dominance (`happened_before`) for safety.
- Keep components modular and testable; avoid brittle codegen patterns inside `q!`.

## Architecture
- **Background VC (existing):** `VectorClockBackground` observes `DataEvent::{Put,Delete}` and emits `MetaEvent::VectorClock{key, member, clock}` and optional per-key merged digests (`CompactionDigest::VectorClockJsonV1`). Uses `ClockState` wrapper for stable codegen.
- **Replication (existing):** `BroadcastReplication<CausalString>` spreads client writes; remote nodes emit their own VC meta via background; no separate VC replication is required.
- **Frontier Collector (new):** `TombstoneFrontierCollector` consumes VC digests, decodes `VectorClockSnapshot`, and `scan`s into a `FrontierState { inner: BTreeMap<String, VCWrapper> }` by merging.
- **Tombstone Cleanup (updated):** Enumerates `(key, tombstone_vc)` and joins with `frontier[key]`. Prunes when `tombstone_vc.happened_before(frontier_vc)` (optionally also age-based retention).
- **Membership (later optional):** For dynamic membership, only prune when frontier covers all active members.

### Data Flow (conceptual)
source ops → routing → { client core } + { replication core } → interleave data/meta → VectorClockBackground → VC digests → FrontierCollector → FrontierState → join with tombstone enumeration → prune

## Deliverables
- `FrontierState` wrapper + constructor (public, zero-arg).
- Frontier collector operator wired into the meta stream.
- VC-aware pruning integrated into `tombstone_cleanup`.
- Tests: local, replicated, safety cases.
- Docs updates (this file, plus references to dev tips for `q!`).

## Implementation Notes
- Use public wrapper structs (e.g., `FrontierState`) and public constructors inside `q!` closures to avoid fragile std path expansions. See `docs/dev/hydro_q_macro_tips.md`.
- Prefer `std::collections::BTreeMap` re-export path; avoid deep module paths.
- Keep closure parameter types explicit for state in `scan`.

## Work Breakdown
1) Define `FrontierState` + ctor
- File: `src/maintain/vector_frontier.rs` (new) or colocate with background.
- Type: `pub struct FrontierState { pub inner: BTreeMap<String, VCWrapper> }`
- `pub fn new_frontier_state() -> FrontierState`

2) Frontier collector stage
- Input: `MetaEvent::CompactionDigest{ VectorClockJsonV1 }`
- Steps:
  - `filter_map` → decode bytes to `VectorClockSnapshot { key, clock }`
  - `scan(new_frontier_state, |state, (key, clock)| { state.inner.entry(key).or_insert(...).merge(clock) })`
- Output: a handle/stream exposing frontier by key (e.g., singleton state + on-demand or periodic snapshots).

3) VC-aware tombstone cleanup
- Enumerate tombstones: `(key, tombstone_vc)` (from existing index).
- Cross with frontier: `join on key` → `filter(tombstone_vc.happened_before(frontier_vc))` → emit cleanup.
- Optional: add retention age guard.

4) Wire into `KVSCluster` plumbing
- Ensure `VectorClockBackground.with_digests(true)` where pruning is enabled.
- Interleave background meta with frontier collector; pass frontier to cleanup.
- Maintain `assume_ordering::<TotalOrder>` as needed.

5) Tests
- Local prune: delete occurs, frontier reflects, tombstone prunes; before frontier updated, no prune.
- Replicated prune: two nodes; delete on node 0; ensure prune only after node 1’s VC observed in frontier.
- Safety: ensure non-dominated or concurrent tombstone VCs are not pruned.

## Open Questions / Risks
- Membership coverage: define minimal guarantee — current plan is conservative (prune only after frontier includes all active members or enforce retention window).
- Digest frequency: if digests are sparse, pruning may lag. Consider periodic re-emission or sampling.
- Large state: frontier map may grow; consider eviction for keys with no tombstones pending.

## CI & Execution
- CI uses nextest by default; keep new tests under reasonable runtime. For long-running replicated tests, consider marking or sharding.

## Links
- VC background: `src/background/vector_clock.rs`
- Dev tips: `docs/dev/hydro_q_macro_tips.md`
- Tombstone cleanup (current): `src/maintain/tombstone_cleanup.rs`

## Progress Log
- 2025-11-16: VC background stabilized with `ClockState`; end-to-end VC replication test green.
- 2025-11-16: Plan authored; TODOs created. Next: implement FrontierState + collector.
