//! Skeleton events & consumer traits for the metadata-background-pipeline restart.
//! Phase 0: minimal enums + borrow-first consumer traits.

use serde::{Deserialize, Serialize};

/// DataEvent captures the observable outcome of an operation.
/// (Future: add Scan variants.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataEvent<K, V> {
    Put { key: K, value: V },
    Delete { key: K },
    Get { key: K, value: Option<V> },
}

/// Wire format for background digests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetaDigestFormat {
    /// JSON serialization of `TombIndexStats`; stable, human-readable.
    TombIndexJsonV1,
}

/// MetaEvent carries maintenance/system metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetaEvent<K> {
    Tomb {
        key: K,
    },
    TombSummary {
        total_tombs: usize,
        last_tomb_key: Option<K>,
    },
    CompactionDigest {
        format: MetaDigestFormat,
        bytes: Vec<u8>,
    },
    // Note: All tombstone GC and vector-clock metadata removed for simplicity.
}

/// A consumer of DataEvents (After or Background pipeline stage).
pub trait DataConsumer<K, V> {
    fn on_data(&mut self, ev: &DataEvent<K, V>);
}

/// A consumer of MetaEvents.
pub trait MetaConsumer<K> {
    fn on_meta(&mut self, meta: &MetaEvent<K>);
}

impl<K, V> DataConsumer<K, V> for () {
    fn on_data(&mut self, _ev: &DataEvent<K, V>) {}
}

impl<K> MetaConsumer<K> for () {
    fn on_meta(&mut self, _meta: &MetaEvent<K>) {}
}

/// Convenience trait for stages that consume both.
pub trait DataMetaConsumer<K, V>: DataConsumer<K, V> + MetaConsumer<K> {}
impl<T, K, V> DataMetaConsumer<K, V> for T where T: DataConsumer<K, V> + MetaConsumer<K> {}

/// Phase 0 Tee skeleton: duplicates DataEvent into optional background consumer.
/// This is intentionally minimal — no cloning beyond reference passing.
pub struct TeeAfter<K, V, Next, BgData> {
    pub next: Next,
    pub bg_data: Option<BgData>,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V, Next, BgData> TeeAfter<K, V, Next, BgData>
where
    Next: DataConsumer<K, V>,
    BgData: DataConsumer<K, V>,
{
    pub fn new(next: Next, bg_data: Option<BgData>) -> Self {
        Self {
            next,
            bg_data,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn on_data(&mut self, ev: &DataEvent<K, V>) {
        self.next.on_data(ev);
        if let Some(bg) = &mut self.bg_data {
            bg.on_data(ev);
        }
    }
}

/// Phase 0 Tee for MetaEvent stream so background stages can observe metadata too.
pub struct MetaTeeAfter<K, Next, BgMeta> {
    pub next: Next,
    pub bg_meta: Option<BgMeta>,
    _phantom: std::marker::PhantomData<K>,
}

impl<K, Next, BgMeta> MetaTeeAfter<K, Next, BgMeta>
where
    Next: MetaConsumer<K>,
    BgMeta: MetaConsumer<K>,
{
    pub fn new(next: Next, bg_meta: Option<BgMeta>) -> Self {
        Self {
            next,
            bg_meta,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn on_meta(&mut self, meta: &MetaEvent<K>) {
        self.next.on_meta(meta);
        if let Some(bg) = &mut self.bg_meta {
            bg.on_meta(meta);
        }
    }
}

/// Simple dispatcher (future enhancement) — currently unused.
#[allow(dead_code)]
pub struct EventDispatcher<K, V> {
    data_consumers: Vec<Box<dyn DataConsumer<K, V>>>,
    meta_consumers: Vec<Box<dyn MetaConsumer<K>>>,
}

impl<K, V> Default for EventDispatcher<K, V> {
    fn default() -> Self {
        Self {
            data_consumers: Vec::new(),
            meta_consumers: Vec::new(),
        }
    }
}

impl<K: Clone, V: Clone> EventDispatcher<K, V> {
    #[allow(dead_code)]
    pub fn emit_data(&mut self, ev: DataEvent<K, V>) {
        if self.data_consumers.is_empty() {
            return;
        }
        for c in &mut self.data_consumers {
            c.on_data(&ev);
        }
    }
    #[allow(dead_code)]
    pub fn emit_meta(&mut self, meta: MetaEvent<K>) {
        if self.meta_consumers.is_empty() {
            return;
        }
        for c in &mut self.meta_consumers {
            c.on_meta(&meta);
        }
    }
}
