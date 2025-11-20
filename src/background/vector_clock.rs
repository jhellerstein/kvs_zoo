use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate as kvs_zoo;

/// Wrapper around the vector-clock state map used inside `q!` closures.
/// Provides a monomorphic symbol so codegen preserves the type without
/// dropping generic arguments (seen when using raw BTreeMap directly).
#[derive(Clone, Debug, Default)]
pub struct ClockState {
    pub inner: ::std::collections::BTreeMap<String, VCWrapper>,
}

pub fn new_clock_state() -> ClockState {
    ClockState::default()
}
use crate::background::{BackgroundDataStream, BackgroundMetaStream, MetaBackground};
use crate::kvs_core::events::{DataEvent, MetaEvent};
use crate::values::VCWrapper;

/// State for pruning tombstones based on vector clock frontiers.
#[derive(Clone, Debug, Default)]
pub struct PruneState {
    // Tombs awaiting a frontier snapshot sufficiently advanced to prune.
    pub pending: ::std::collections::BTreeMap<String, VCWrapper>,
    // Current merged frontier (vector clock snapshot per key).
    pub frontier: ::std::collections::BTreeMap<String, VCWrapper>,
}

pub fn new_prune_state() -> PruneState {
    PruneState::default()
}

#[inline]
pub fn can_prune(tomb_vc: &VCWrapper, frontier_vc: &VCWrapper) -> bool {
    tomb_vc.happened_before(frontier_vc) || tomb_vc == frontier_vc
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable stdout logging whenever a local vector clock update is produced.
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.log_updates = enabled;
        self
    }

    /// Emit merged vector clock snapshots (now as direct MetaEvent::VectorClockSnapshot).
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
            q!(
                move |state: &mut kvs_zoo::background::vector_clock::ClockState, event| {
                    match event {
                        DataEvent::Put { key, .. } | DataEvent::Delete { key } => {
                            let member_raw = CLUSTER_SELF_ID.raw_id;
                            let clock = kvs_zoo::values::vc_helpers::bump_local(
                                &mut state.inner,
                                &key,
                                member_raw,
                            );
                            Some((key, member_raw, clock))
                        }
                        DataEvent::Get { .. } => None,
                    }
                }
            ),
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

        let mut combined_meta = meta
            .clone()
            .interleave(vector_meta.clone())
            .assume_ordering(nondet!(/** meta with vector clocks */));

        if self.emit_digests {
            // Aggregate merged per-key clocks and emit direct snapshot events.
            // Aggregate merged per-key clocks into snapshot events.
            let snapshots = combined_meta
                .clone()
                .filter_map(q!(|event| match event {
                    MetaEvent::VectorClock { key, member: _, clock } => Some((key, clock)),
                    _ => None,
                }))
                .scan(
                    q!(|| kvs_zoo::background::vector_clock::new_clock_state()),
                    q!(|state: &mut kvs_zoo::background::vector_clock::ClockState,(key, clock)| {
                        let snapshot = kvs_zoo::values::vc_helpers::merge_into(&mut state.inner,&key,clock);
                        Some(MetaEvent::VectorClockSnapshot { key, clock: snapshot })
                    }),
                );

            // Tomb events (original meta stream).
            let tombs = combined_meta.clone().filter_map(q!(|event| match event {
                MetaEvent::Tomb { key } => Some(key),
                _ => None,
            }));

            // Interleave snapshots and tombs, performing pruning inline.
            let pruned_meta = snapshots
                .clone()
                .map(q!(|snap| (Some(snap), None::<String>)))
                .interleave(tombs.map(q!(|key| (None, Some(key)))))
                .assume_ordering(nondet!(/** snapshots + tombs interleaved */))
                .scan(
                    q!(|| kvs_zoo::background::vector_clock::new_prune_state()),
                    q!(|state: &mut kvs_zoo::background::vector_clock::PruneState,(maybe_snap, maybe_tomb)| {
                        let mut emit: Option<MetaEvent> = None;
                        if let Some(MetaEvent::VectorClockSnapshot { key, clock }) = maybe_snap {
                            state.frontier.insert(key.clone(), clock.clone());
                            if let Some(tomb_vc) = state.pending.get(&key).cloned() {
                                let frontier_vc = state.frontier.get(&key).unwrap();
                                if kvs_zoo::background::vector_clock::can_prune(&tomb_vc, frontier_vc) {
                                    state.pending.remove(&key);
                                    emit = Some(MetaEvent::TombPruned { key });
                                }
                            }
                        }
                        if let Some(tomb_key) = maybe_tomb {
                            let tomb_vc = state.frontier.get(&tomb_key).cloned().unwrap_or_default();
                            // If frontier already advanced, prune immediately; else store.
                            if let Some(frontier_vc) = state.frontier.get(&tomb_key).cloned() {
                                if kvs_zoo::background::vector_clock::can_prune(&tomb_vc, &frontier_vc) {
                                    emit = Some(MetaEvent::TombPruned { key: tomb_key });
                                } else {
                                    state.pending.insert(tomb_key, tomb_vc);
                                }
                            } else {
                                state.pending.insert(tomb_key, tomb_vc);
                            }
                        }
                        emit
                    }),
                );

            // Prioritize emitted TombPruned / snapshots before original meta.
            combined_meta = pruned_meta
                .clone()
                .interleave(snapshots.clone())
                .interleave(combined_meta)
                .assume_ordering(nondet!(/** pruned + snapshots + meta */));
        }

        // Build local frontier (merged per-key clocks) from snapshot events.
        let (combined_meta, _frontier_snaps) =
            kvs_zoo::after_storage::meta::build_frontier(combined_meta);

        (data, combined_meta)
    }
}
