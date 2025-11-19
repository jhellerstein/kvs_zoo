use hydro_lang::prelude::*;
use lattices::Merge;

use crate as kvs_zoo;
use crate::background::BackgroundMetaStream;
use crate::kvs_core::events::MetaEvent;
use crate::values::vector_clock::VCWrapper;

/// Wrapper around the per-key merged frontier state used inside `q!` closures.
///
/// See `docs/dev/hydro_q_macro_tips.md` for why we wrap state for stable codegen.
#[derive(Clone, Debug, Default)]
pub struct FrontierState {
    pub inner: ::std::collections::BTreeMap<String, VCWrapper>,
}

/// Construct a fresh `FrontierState` for scans/aggregations.
pub fn new_frontier_state() -> FrontierState {
    FrontierState::default()
}

/// Consume VectorClockSnapshot events from `meta`, merge a local frontier per key,
/// and return:
/// - The original `meta` stream (passthrough, no additional interleaving)
/// - A stream of merged frontier snapshots (one per update)
///
/// Callers that need frontier updates should subscribe to the second stream.
pub fn build_frontier<'a>(
    meta: BackgroundMetaStream<'a>,
) -> (BackgroundMetaStream<'a>, BackgroundMetaStream<'a>) {
    // Consume VectorClockSnapshot events and merge into frontier state.
    let frontier_snapshots = meta
        .clone()
        .filter_map(q!(|event| match event {
            MetaEvent::VectorClockSnapshot { key, clock } => Some((key, clock)),
            _ => None,
        }))
        .scan(
            q!(|| kvs_zoo::after_storage::meta::vector_frontier::new_frontier_state()),
            q!(
                |state: &mut kvs_zoo::after_storage::meta::vector_frontier::FrontierState,
                 (key, clock_in)| {
                    let entry = state.inner.entry(key.clone()).or_default();
                    entry.merge(clock_in);
                    Some(MetaEvent::VectorClockSnapshot {
                        key,
                        clock: entry.clone(),
                    })
                }
            ),
        );

    (meta, frontier_snapshots)
}
