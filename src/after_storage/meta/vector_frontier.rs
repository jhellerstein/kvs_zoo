use hydro_lang::prelude::*;
use lattices::Merge;

use crate as kvs_zoo;
use crate::background::{BackgroundMetaStream, VectorClockSnapshot};
use crate::kvs_core::events::{MetaDigestFormat, MetaEvent};
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

/// Consume VC digests from background meta, merge into a local frontier per key,
/// and return the original meta stream along with a stream of merged snapshots
/// (one per update). The caller can ignore the snapshots and instead read the
/// frontier via other means if desired.
pub fn build_frontier<'a>(
    meta: BackgroundMetaStream<'a>,
) -> (
    BackgroundMetaStream<'a>,
    Stream<VectorClockSnapshot, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
) {
    let snapshots = meta
        .clone()
        .filter_map(q!(|event| match event {
            MetaEvent::CompactionDigest { format, bytes } => match format {
                MetaDigestFormat::VectorClockJsonV1 => {
                    let decoded: Result<VectorClockSnapshot, _> = serde_json::from_slice(&bytes);
                    decoded.ok()
                }
                _ => None,
            },
            _ => None,
        }))
        .scan(
            q!(|| kvs_zoo::after_storage::meta::vector_frontier::new_frontier_state()),
            q!(|state: &mut kvs_zoo::after_storage::meta::vector_frontier::FrontierState, snapshot: VectorClockSnapshot| {
                let key = snapshot.key.clone();
                let clock_in = snapshot.clock.clone();
                let entry = state
                    .inner
                    .entry(key.clone())
                    .or_insert_with(kvs_zoo::values::VCWrapper::new);
                entry.merge(clock_in);
                Some(kvs_zoo::background::VectorClockSnapshot { key, clock: entry.clone() })
            }),
        );

    (meta, snapshots)
}
