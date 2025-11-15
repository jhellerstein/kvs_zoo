use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::background::MetaBackground;
use crate::events::{DataEvent, MetaEvent};

/// Snapshot of tomb metadata accumulated by the background indexer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TombIndexStats {
    pub total_tombs: usize,
    pub last_tomb_key: Option<String>,
}

/// Simple background stage that indexes tomb metadata and optionally emits summaries.
#[derive(Clone, Debug)]
pub struct TombIndexBackground {
    log_snapshots: bool,
    emit_summaries: bool,
}

impl Default for TombIndexBackground {
    fn default() -> Self {
        Self {
            log_snapshots: false,
            emit_summaries: false,
        }
    }
}

impl TombIndexBackground {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable stdout logging of aggregated tomb stats for observability during demos.
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.log_snapshots = enabled;
        self
    }

    /// Emit `MetaEvent::TombSummary` snapshots downstream for consumers/tests.
    pub fn with_summaries(mut self, enabled: bool) -> Self {
        self.emit_summaries = enabled;
        self
    }

    pub fn transform_meta_stream<'a>(
        &self,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded> {
        let stats_stream = meta.clone().scan(
            q!(|| TombIndexStats::default()),
            q!(|stats: &mut TombIndexStats, event: MetaEvent| {
                if let MetaEvent::Tomb { key } = event {
                    stats.total_tombs += 1;
                    stats.last_tomb_key = Some(key);
                }
                Some(stats.clone())
            }),
        );

        if self.log_snapshots {
            stats_stream
                .clone()
                .for_each(q!(|snapshot: TombIndexStats| {
                    println!(
                        "[bg] tomb_index total={} last={:?}",
                        snapshot.total_tombs, snapshot.last_tomb_key
                    );
                }));
        }

        if self.emit_summaries {
            let summaries =
                stats_stream.map(q!(|snapshot: TombIndexStats| MetaEvent::TombSummary {
                    total_tombs: snapshot.total_tombs,
                    last_tomb_key: snapshot.last_tomb_key.clone(),
                }));

            meta.interleave(summaries)
                .assume_ordering(nondet!(/** tomb meta with summaries */))
        } else {
            meta
        }
    }
}

impl<V> MetaBackground<V> for TombIndexBackground
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
        data: Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        meta: Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) -> (
        Stream<DataEvent<V>, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
        Stream<MetaEvent, Cluster<'a, crate::kvs_core::KVSNode>, Unbounded>,
    ) {
        let meta = self.transform_meta_stream(meta);
        (data, meta)
    }
}
