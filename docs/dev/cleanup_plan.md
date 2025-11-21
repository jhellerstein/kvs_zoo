# KVS Zoo Codebase Cleanup Plan

## 1. Purpose
Streamline the repository to emphasize architectural elegance and learnability by:
- Removing dead or low-value code.
- Reducing duplication (DRY) in plumbing layers.
- Simplifying naming and public API ergonomics.
- Centralizing documentation of ordering and layering concepts.
- Preparing incremental, low-risk refactors with clear acceptance criteria.

## 2. Architectural Snapshot
Core layers today:
- **kvs_layer/**: Declarative layering DSL (cluster creation `KVSSpec`, routing `KVSPlumb`, replication/responders `AfterPlumb`, replication fanout `ReplicationPlumb`, layer types `KVSCluster`, `KVSNode`, layer registry `KVSClusters`).
- **plumbing.rs**: Orchestration entrypoint (`plumb_kvs_dataflow`) + helper (`extract_put_deltas`). Binds external I/O, executes downward and upward passes, invokes core and background.
- **kvs_core/**: Sequential operation processor (`KVSCore`), event types (`DataEvent`, `MetaEvent`). Two convenience wrappers for client vs replicated ops differ only in a response flag.
- **background/**: Background stream attach traits (`BackgroundPlumb`, `MetaBackground`), tomb indexing stage (`TombIndexBackground`).
- **store_state.rs**: Lattice wrapper with tombstone support (PUT/DELETE deltas, resurrection semantics).

## 3. Key Findings
### 3.1 Dead / Unused / Low-Value Code
- `EventDispatcher` (unused, marked with `#[allow(dead_code)]`).
- Duplicate constructor `new_with_background` in `KVSCluster` (redundant with `with_background`).
- Simulation logic in some core tests duplicates processing semantics outside Hydro streams (could shrink for focus).

### 3.2 Duplication / DRY Opportunities
- Repeated `.assume_ordering(nondet!(/** comment */))` patterns across modules (noise for learners).
- Very similar cluster vs node implementations in `ReplicationPlumb` and `KVSPlumb` base cases.
- Separate `process_client_ops` / `process_replicated_ops` differ only in boolean envelope metadata.
- Response stamping logic inline in `plumb_kvs_dataflow`.

### 3.3 Abstraction Improvements
- Centralize ordering annotation via a helper: `ordered(stream, label)` to reduce cognitive overhead.
- Factor replication fanout logic into a shared helper (`fanout_replication<A,V>(...)`).
- Merge `MetaBackground` + `BackgroundPlumb` into unified `BackgroundStage<V>` for simpler mental model.
- Move `extract_put_deltas` closer to replication concerns (new `replication_helpers.rs`).

### 3.4 Naming & Learnability
- Multiple “plumb” trait names + standalone `plumb_kvs_dataflow` create redundancy; consider rename of file `plumbing.rs` → `orchestrate.rs`.
- Marker type `KVSCore::KVSNode` vs architectural `KVSNode<Name,B,A>`—rename core marker to `MemberNode` (or `CoreMember`).
- Introduce `docs/architecture_glossary.md` referencing: Before, After, Background, Core, Orchestration, Cluster vs Node.

### 3.5 API Ergonomics
- Return structure from `plumb_kvs_dataflow` could evolve into `KVSDeployment { layers, client_port, data_stream, meta_stream }` for future extensibility.
- Boolean environment-based response stamping can be extracted for clarity.

### 3.6 Tests & Verification
- Keep existing nextest + trybuild guardrails; add property-style replication equivalence test after refactor.
- Focus tests on stream path rather than sequential simulation duplicates.

## 4. Cleanup Phases
| Phase | Focus | Risk | Outcome |
|-------|-------|------|---------|
| 0 | Low-risk deletions & helper intro | Low | Lean code, helper seeds |
| 1 | Replication & delta helper relocation | Low-Med | DRY replication plumbing |
| 2 | Background trait unification | Med | Simpler background API |
| 3 | Naming & orchestration rename | Med | Clearer entrypoints |
| 4 | Documentation & glossary | Low | Improved learnability |
| 5 | Optional API struct returns & panic refinement | Med | Future-proof ergonomics |
| 6 | Validation, property test, polish | Low | Confidence & release readiness |

### Phase 0 (Immediate)
Actions:
- Remove `EventDispatcher`, `new_with_background`.
- Add `ordered(stream, label)` helper; refactor obvious calls.
- Add `stamp_responses(responses, enabled)` helper.
- Internal refactor: consolidate `process_client_ops` / `process_replicated_ops` into `process_ops(tag_responses, operations)`; wrappers remain for external clarity.
Acceptance: All tests + clippy green.

### Phase 1
- Create `replication_helpers.rs`; move `extract_put_deltas`.
- Factor shared replication cluster/node logic.
- Regression test comparing old vs new replication output on sample architectures.
Acceptance: No change in test counts or outputs (except internal helper location).

### Phase 2
- Introduce `BackgroundStage<V>` replacing `BackgroundPlumb` + `MetaBackground`.
- Adapt `TombIndexBackground` to implement single trait.
- Adjust orchestration call path.
Acceptance: Background meta summary/digest tests unchanged.

### Phase 3
- Rename `plumbing.rs` → `orchestrate.rs`; provide re-export + deprecation lint.
- Rename core marker `KVSNode` to `MemberNode` (update cluster type references accordingly).
- Update examples & README imports.
Acceptance: All examples build; nextest green; deprecation warnings acceptable (or feature gated).

### Phase 4
- Add `architecture_glossary.md`.
- Remove repetitive inline ordering comments (now explained once).
- Simplify README wiring example using `ordered()` helper.
Acceptance: Docs build; length reduction in README without loss of clarity.

### Phase 5 (Optional Enhancements)
- Replace root terminal panic with compile-time guard or explicit `Result`.
- Change orchestration return to `KVSDeployment` struct.
Acceptance: Migration notes; examples upgraded; stable tests.

### Phase 6
- Add property test: replication does not create extra responses (set membership compare).
- Final clippy, doc, and (optional) benchmark run.
- Tag release & write `CHANGELOG.md` summarizing major refactors.

## 5. Acceptance Criteria Summary
- No functional regressions (tests & guardrails pass each phase).
- Clippy: zero warnings at each phase.
- Docs reflect new names and helpers after Phase 4.
- Migration notes and compatibility re-exports survive at least one version.

## 6. Migration Notes
Old → New:
- `use kvs_zoo::plumbing::plumb_kvs_dataflow` ⇒ `use kvs_zoo::orchestrate::plumb_kvs_dataflow` (old path re-exported temporarily).
- Core marker rename: `Cluster<KVSNode>` ⇒ `Cluster<MemberNode>`.
- Delta helper: `kvs_zoo::plumbing::extract_put_deltas` ⇒ `kvs_zoo::replication::extract_put_deltas` (or re-export at root).
- Response ordering: inline `.assume_ordering(...)` ⇒ `ordered(stream, "label")`.

## 7. Open Decisions
- Marker rename choice: `MemberNode` vs `CoreMember` (clarity vs brevity).
- Keep deprecation period (1 minor release) vs immediate removal.
- Introduce feature flags for experimental background stages?

## 8. Risk Mitigation
- Perform refactors in small PRs; run full nextest after each.
- Snapshot replication behavior before Phase 1 (log streams for deterministic sample seeds).
- Maintain exhaustive guardrail compile tests during renames to catch trait bound regressions.

## 9. Execution Checklist
- [ ] Phase 0 implementation
- [ ] Phase 1 replication refactor & regression test
- [ ] Phase 2 background merge
- [ ] Phase 3 renames & README updates
- [ ] Phase 4 docs & glossary
- [ ] Phase 5 optional ergonomic improvements
- [ ] Phase 6 validation & release tagging

## 10. Guiding Principles
- Prefer small, explicit traits over broad mega-traits.
- Minimize public API churn through re-exports and deprecation warnings.
- Keep code examples pedagogical: reveal architectural layering, hide incidental complexity.
- Replace comment noise with central conceptual docs.
- Back each simplification with untouched test results to reinforce correctness.

---
Last updated: 2025-11-20
