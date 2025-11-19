//! Skeleton events & consumer traits for the metadata-background-pipeline restart.
//! Phase 0: minimal enums + borrow-first consumer traits.

use serde::{Deserialize, Serialize};

use crate::values::VCWrapper;

/// DataEvent captures the observable outcome of an operation.
/// (Future: add Scan variants.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataEvent<V> {
    Put { key: String, value: V },
    Delete { key: String },
    Get { key: String, value: Option<V> },
}

/// Wire format for background digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaDigestFormat {
    /// JSON serialization of `TombIndexStats`; stable, human-readable.
    TombIndexJsonV1,
}

/// MetaEvent carries maintenance/system metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaEvent {
    Tomb {
        key: String,
    },
    TombSummary {
        total_tombs: usize,
        last_tomb_key: Option<String>,
    },
    ReclaimFrontier {
        frontier_seq: Option<u64>,
        epoch: u64,
    },
    CompactionDigest {
        format: MetaDigestFormat,
        bytes: Vec<u8>,
    },
    /// Typed tomb prune notification: key tombstone proven reclaimable.
    TombPruned {
        key: String,
    },
    VectorClock {
        key: String,
        member: u32,
        clock: VCWrapper,
    },
    /// Direct per-key merged vector clock snapshot (no manual JSON serialization).
    VectorClockSnapshot {
        key: String,
        clock: VCWrapper,
    },
}

/// A consumer of DataEvents (After or Background pipeline stage).
pub trait DataConsumer<D> {
    fn on_data(&mut self, ev: &D);
}

/// A consumer of MetaEvents.
pub trait MetaConsumer<M> {
    fn on_meta(&mut self, meta: &M);
}

impl<D> DataConsumer<D> for () {
    fn on_data(&mut self, _ev: &D) {}
}

impl<M> MetaConsumer<M> for () {
    fn on_meta(&mut self, _meta: &M) {}
}

/// Convenience trait for stages that consume both.
pub trait DataMetaConsumer<D, M>: DataConsumer<D> + MetaConsumer<M> {}
impl<T, D, M> DataMetaConsumer<D, M> for T where T: DataConsumer<D> + MetaConsumer<M> {}

/// Phase 0 Tee skeleton: duplicates DataEvent into optional background consumer.
/// This is intentionally minimal — no cloning beyond reference passing.
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
    pub fn new(next: Next, bg_data: Option<BgData>) -> Self {
        Self {
            next,
            bg_data,
            _pd: std::marker::PhantomData,
        }
    }

    pub fn on_data(&mut self, ev: &D) {
        self.next.on_data(ev);
        if let Some(bg) = &mut self.bg_data {
            bg.on_data(ev);
        }
    }
}

/// Phase 0 Tee for MetaEvent stream so background stages can observe metadata too.
pub struct MetaTeeAfter<M, Next, BgMeta> {
    pub next: Next,
    pub bg_meta: Option<BgMeta>,
    _pm: std::marker::PhantomData<M>,
}

impl<M, Next, BgMeta> MetaTeeAfter<M, Next, BgMeta>
where
    Next: MetaConsumer<M>,
    BgMeta: MetaConsumer<M>,
{
    pub fn new(next: Next, bg_meta: Option<BgMeta>) -> Self {
        Self {
            next,
            bg_meta,
            _pm: std::marker::PhantomData,
        }
    }

    pub fn on_meta(&mut self, meta: &M) {
        self.next.on_meta(meta);
        if let Some(bg) = &mut self.bg_meta {
            bg.on_meta(meta);
        }
    }
}

/// Simple dispatcher (future enhancement) — currently unused.
#[allow(dead_code)]
pub struct EventDispatcher<D, M> {
    data_consumers: Vec<Box<dyn DataConsumer<D>>>,
    meta_consumers: Vec<Box<dyn MetaConsumer<M>>>,
}

impl<D, M> Default for EventDispatcher<D, M> {
    fn default() -> Self {
        Self {
            data_consumers: Vec::new(),
            meta_consumers: Vec::new(),
        }
    }
}

impl<D: Clone, M: Clone> EventDispatcher<D, M> {
    #[allow(dead_code)]
    pub fn emit_data(&mut self, ev: D) {
        if self.data_consumers.is_empty() {
            return;
        }
        for c in &mut self.data_consumers {
            c.on_data(&ev);
        }
    }
    #[allow(dead_code)]
    pub fn emit_meta(&mut self, meta: M) {
        if self.meta_consumers.is_empty() {
            return;
        }
        for c in &mut self.meta_consumers {
            c.on_meta(&meta);
        }
    }
}
