//! Phase 0 skeleton example: replicated cluster with tombstone delete semantics.
//! Demonstrates ergonomics of DataEvent / MetaEvent and TeeAfter without full logic.

use kvs_zoo::{DataEvent, MetaEvent, DataConsumer, MetaConsumer, TeeAfter};

// Dummy value type for illustration.
#[derive(Debug, Clone)]
struct Val(String);

// Stub After stage that just logs data events.
struct LogAfter;
impl DataConsumer<DataEvent<Val>> for LogAfter {
    fn on_data(&mut self, ev: &DataEvent<Val>) {
        println!("[After] data -> {:?}", ev);
    }
}
impl MetaConsumer<MetaEvent> for LogAfter {
    fn on_meta(&mut self, meta: &MetaEvent) {
        println!("[After] meta -> {:?}", meta);
    }
}

// Background stage collecting tomb keys.
struct TombIndex { tombs: Vec<String> }
impl TombIndex { fn new() -> Self { Self { tombs: Vec::new() } } }
impl DataConsumer<DataEvent<Val>> for TombIndex {
    fn on_data(&mut self, _ev: &DataEvent<Val>) { /* ignore data in Phase 0 */ }
}
impl MetaConsumer<MetaEvent> for TombIndex {
    fn on_meta(&mut self, meta: &MetaEvent) {
        if let MetaEvent::Tomb { key } = meta { self.tombs.push(key.clone()); }
    }
}

// Minimal KVS skeleton: apply operations and emit events.
struct KVSCoreSkeleton;
impl KVSCoreSkeleton {
    fn put(&mut self, key: &str, value: Val) -> DataEvent<Val> {
        // Phase 0: no real storage, just echo
        DataEvent::Put { key: key.to_string(), value }
    }
    fn get(&mut self, key: &str) -> DataEvent<Val> {
        DataEvent::Get { key: key.to_string(), value: None } // always none in skeleton
    }
    fn delete(&mut self, key: &str) -> (DataEvent<Val>, MetaEvent) {
        (DataEvent::Delete { key: key.to_string() }, MetaEvent::Tomb { key: key.to_string() })
    }
}

fn main() {
    let mut core = KVSCoreSkeleton;
    let mut after = LogAfter;
    let mut background = TombIndex::new();
    let mut tee = TeeAfter { next: after, bg_data: Some(background), _pd: std::marker::PhantomData::<DataEvent<Val>>() };

    // Put
    let put_ev = core.put("k1", Val("v1".into()));
    tee.on_data(&put_ev);

    // Get
    let get_ev = core.get("k1");
    tee.on_data(&get_ev);

    // Delete + tomb meta
    let (del_ev, tomb_meta) = core.delete("k1");
    tee.on_data(&del_ev);
    // Phase 0: Meta not dispatched through tee yet; call background & after manually for ergonomics preview.
    // In future: unified dispatcher will route this automatically.
    // After meta
    // (Would normally iterate registered meta consumers.)
    let mut meta_consumers: Vec<&mut dyn MetaConsumer<MetaEvent>> = vec![&mut tee.next];
    if let Some(bg) = &mut tee.bg_data { meta_consumers.push(bg as &mut dyn MetaConsumer<MetaEvent>); }
    for c in meta_consumers { c.on_meta(&tomb_meta); }

    // Show tomb index contents (need mutable access to background stored in tee)
    if let Some(bg) = tee.bg_data.as_ref() {
        println!("[Background] tombs = {:?}", bg.tombs);
    }
}
