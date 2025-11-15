//! Skeleton events & consumer traits for the metadata-background-pipeline restart.
//! Phase 0: minimal enums + borrow-first consumer traits.

/// DataEvent captures the observable outcome of an operation.
/// (Future: add Scan variants.)
#[derive(Debug, Clone)]
pub enum DataEvent<V> {
    Put { key: String, value: V },
    Delete { key: String },
    Get { key: String, value: Option<V> },
}

/// MetaEvent carries maintenance/system metadata.
#[derive(Debug, Clone)]
pub enum MetaEvent {
    Tomb { key: String },
    // Future: Reclaim { key: String },
    // Future: TombSummary { format_id: u16, bytes: Vec<u8> },
}

/// A consumer of DataEvents (After or Background pipeline stage).
pub trait DataConsumer<D> {
    fn on_data(&mut self, ev: &D);
}

/// A consumer of MetaEvents.
pub trait MetaConsumer<M> {
    fn on_meta(&mut self, meta: &M);
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
    pub fn on_data(&mut self, ev: &D) {
        self.next.on_data(ev);
        if let Some(bg) = &mut self.bg_data {
            bg.on_data(ev);
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
        Self { data_consumers: Vec::new(), meta_consumers: Vec::new() }
    }
}

impl<D: Clone, M: Clone> EventDispatcher<D, M> {
    #[allow(dead_code)]
    pub fn emit_data(&mut self, ev: D) {
        if self.data_consumers.is_empty() { return; }
        for c in &mut self.data_consumers { c.on_data(&ev); }
    }
    #[allow(dead_code)]
    pub fn emit_meta(&mut self, meta: M) {
        if self.meta_consumers.is_empty() { return; }
        for c in &mut self.meta_consumers { c.on_meta(&meta); }
    }
}