use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use lattices::Merge;
use serde::{Deserialize, Serialize};

use crate as kvs_zoo;

/// Wrapper around the vector-clock state map used inside `q!` closures.
///
/// Rationale: the Hydro code generator in test/trybuild contexts sometimes
/// expands type paths like `std::collections::BTreeMap` into invalid
/// `std::collections::btree::map::BTreeMap`. By using a named, public wrapper
/// type here and referencing it explicitly in `q!` closures, we avoid those
/// brittle expansions and keep generated code compiling.
#[derive(Clone, Debug, Default)]
pub struct ClockState {
    pub inner: ::std::collections::BTreeMap<String, VCWrapper>,
}

/// Construct a fresh `ClockState` for scans/aggregations.
pub fn new_clock_state() -> ClockState {
    ClockState::default()
}
use crate::background::{BackgroundDataStream, BackgroundMetaStream, MetaBackground};
use crate::kvs_core::events::{DataEvent, MetaDigestFormat, MetaEvent};
use crate::values::VCWrapper;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TombPruned {
    pub key: String,
}

#[derive(Clone, Debug)]
pub enum PruneEvent {
    ClockSnap(String, VCWrapper),
    Tomb(String),
    Frontier(String, VCWrapper),
}

#[derive(Clone, Debug, Default)]
pub struct PruneState {
    pub latest: ::std::collections::BTreeMap<String, VCWrapper>,
    pub pending: ::std::collections::BTreeMap<String, VCWrapper>,
    pub frontier: ::std::collections::BTreeMap<String, VCWrapper>,
}

pub fn new_prune_state() -> PruneState {
    PruneState::default()
}

/// Snapshot of the merged vector clock state for a particular key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClockSnapshot {
    pub key: String,
    pub clock: VCWrapper,
}

/// Background stage that emits vector clock updates for each key and
/// optionally aggregates them into digests for downstream maintenance hooks.
#[derive(Clone, Debug, Default)]
pub struct VectorClockBackground {
    log_updates: bool,
    emit_digests: bool,
}

impl VectorClockBackground {
    pub fn new() -> Self { Self::default() }

    /// Enable stdout logging whenever a local vector clock update is produced.
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.log_updates = enabled;
        self
    }

    /// Emit `MetaEvent::CompactionDigest` snapshots with merged vector clocks.
    pub fn with_digests(mut self, enabled: bool) -> Self {
        self.emit_digests = enabled;
        self
    }
}

impl<V> MetaBackground<V> for VectorClockBackground
where
    V: Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + 'static,
{
    fn attach<'a>(
        &mut self,
        _cluster: &Cluster<'a, crate::kvs_core::KVSNode>,
        data: BackgroundDataStream<'a, V>,
        meta: BackgroundMetaStream<'a>,
    ) -> (BackgroundDataStream<'a, V>, BackgroundMetaStream<'a>) {
        let vector_clock_updates = data.clone().scan(
            q!(|| kvs_zoo::background::vector_clock::new_clock_state()),
            q!(move |state: &mut kvs_zoo::background::vector_clock::ClockState, event| {
                match event {
                    DataEvent::Put { key, .. } => {
                        let member_raw = CLUSTER_SELF_ID.raw_id;
                        let entry = state
                            .inner
                            .entry(key.clone())
                            .or_insert_with(kvs_zoo::values::VCWrapper::new);
                        entry.bump(member_raw.to_string());
                        Some((key, member_raw, entry.clone()))
                    }
                    DataEvent::Delete { key } => {
                        let member_raw = CLUSTER_SELF_ID.raw_id;
                        let entry = state
                            .inner
                            .entry(key.clone())
                            .or_insert_with(kvs_zoo::values::VCWrapper::new);
                        entry.bump(member_raw.to_string());
                        Some((key, member_raw, entry.clone()))
                    }
                    DataEvent::Get { .. } => None,
                }
            }),
        );

        let vector_meta =
            vector_clock_updates
                .clone()
                .map(q!(|(key, member, clock)| MetaEvent::VectorClock {
                    key,
                    member,
                    clock,
                }));

        if self.log_updates {
            vector_clock_updates
                .clone()
                .for_each(q!(|(key, member, clock)| {
                    println!(
                        "[bg] vector_clock member={} key={} clock={:?}",
                        member, key, clock
                    );
                }));
        }

        let mut combined_meta = meta.clone()
            .interleave(vector_meta.clone())
            .assume_ordering(nondet!(/** meta with vector clocks */));

        if self.emit_digests {
            let aggregated = combined_meta
                .clone()
                .filter_map(q!(|event| match event {
                    MetaEvent::VectorClock {
                        key,
                        member: _,
                        clock,
                    } => Some((key, clock)),
                    _ => None,
                }))
                .scan(
                    q!(|| kvs_zoo::background::vector_clock::new_clock_state()),
                    q!(|state: &mut kvs_zoo::background::vector_clock::ClockState, (key, clock)| {
                        let entry = state
                            .inner
                            .entry(key.clone())
                            .or_insert_with(kvs_zoo::values::VCWrapper::new);
                        entry.merge(clock);
                        Some(kvs_zoo::background::vector_clock::VectorClockSnapshot {
                            key,
                            clock: entry.clone(),
                        })
                    }),
                );

            let digest_meta = aggregated.clone().map(q!(
                |snapshot: kvs_zoo::background::vector_clock::VectorClockSnapshot| {
                    let payload = serde_json::to_vec(&snapshot)
                        .expect("serialize vector clock snapshot");
                    MetaEvent::CompactionDigest {
                        format: MetaDigestFormat::VectorClockJsonV1,
                        bytes: payload,
                    }
                }
            ));

            if self.log_updates {
                aggregated.clone().for_each(q!(
                    |_snapshot: kvs_zoo::background::vector_clock::VectorClockSnapshot| { () }
                ));
            }

            combined_meta = combined_meta
                .interleave(digest_meta.clone())
                .assume_ordering(nondet!(/** meta with vector clock digests */));

            // (no-op)

            // VC-based tomb prune: capture tomb VC at deletion, compare with frontier, emit pruned digest
            // Use the aggregated snapshots directly as the (local) frontier stream.
            let frontier_snaps = aggregated.clone();

            let events = aggregated
                .clone()
                .map(q!(|snap: kvs_zoo::background::vector_clock::VectorClockSnapshot| kvs_zoo::background::vector_clock::PruneEvent::ClockSnap(snap.key, snap.clock)))
                .interleave(
                    combined_meta
                        .clone()
                        .filter_map(q!(|event| match event { kvs_zoo::kvs_core::events::MetaEvent::Tomb { key } => Some(kvs_zoo::background::vector_clock::PruneEvent::Tomb(key)), _ => None }))
                )
                .interleave(
                    frontier_snaps
                        .clone()
                        .map(q!(|snap: kvs_zoo::background::vector_clock::VectorClockSnapshot| kvs_zoo::background::vector_clock::PruneEvent::Frontier(snap.key, snap.clock)))
                )
                .assume_ordering(nondet!(/** prune events interleaved */));

            // Strict: only emit when tomb VC <= frontier VC
            let pruned_meta = events.scan(
                q!(|| kvs_zoo::background::vector_clock::new_prune_state()),
                q!(|state: &mut kvs_zoo::background::vector_clock::PruneState, event: kvs_zoo::background::vector_clock::PruneEvent| {
                    match event {
                        kvs_zoo::background::vector_clock::PruneEvent::ClockSnap(key, clock) => { state.latest.insert(key, clock); None }
                        kvs_zoo::background::vector_clock::PruneEvent::Tomb(key) => {
                            let tomb_vc = state.latest.get(&key).cloned().unwrap_or_else(kvs_zoo::values::VCWrapper::new);
                            state.pending.insert(key.clone(), tomb_vc.clone());
                            if let Some(frontier_vc) = state.frontier.get(&key).cloned() {
                                if tomb_vc.happened_before(&frontier_vc) || tomb_vc == frontier_vc {
                                    state.pending.remove(&key);
                                    let payload = serde_json::to_vec(&kvs_zoo::background::vector_clock::TombPruned { key: key.clone() }).expect("serialize tomb pruned");
                                    return Some(kvs_zoo::kvs_core::events::MetaEvent::CompactionDigest { format: kvs_zoo::kvs_core::events::MetaDigestFormat::TombPrunedJsonV1, bytes: payload });
                                }
                            }
                            None
                        }
                        kvs_zoo::background::vector_clock::PruneEvent::Frontier(key, clock) => {
                            state.frontier.insert(key.clone(), clock.clone());
                            if let (Some(tomb_vc), Some(frontier_vc)) = (state.pending.get(&key).cloned(), state.frontier.get(&key).cloned()) {
                                if tomb_vc.happened_before(&frontier_vc) || tomb_vc == frontier_vc {
                                    state.pending.remove(&key);
                                    let payload = serde_json::to_vec(&kvs_zoo::background::vector_clock::TombPruned { key: key.clone() }).expect("serialize tomb pruned");
                                    return Some(kvs_zoo::kvs_core::events::MetaEvent::CompactionDigest { format: kvs_zoo::kvs_core::events::MetaDigestFormat::TombPrunedJsonV1, bytes: payload });
                                }
                            }
                            None
                        }
                    }
                }),
            );

            combined_meta = pruned_meta
                .clone()
                .interleave(combined_meta)
                .assume_ordering(nondet!(/** pruned digests prioritized + meta */));

            // Also emit a direct prune digest upon tomb if not strict (for visibility/tests).
            // No direct prune emission in strict mode.
        }

            // Build/maintain a local frontier of merged per-key clocks from digests.
            // This wires in the stateful frontier collector; snapshots are currently unused here
            // but the accumulated state is available for local maintenance (e.g., tombstone cleanup).
            let (combined_meta, _frontier_snaps) = kvs_zoo::after_storage::meta::build_frontier(combined_meta);

            (data, combined_meta)
    }
}
