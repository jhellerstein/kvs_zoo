stageleft::stageleft_no_entry_crate!();

// Terminology: legacy "dispatch" → `before_storage`; legacy "maintenance" → `after_storage`.
pub mod after_storage;
pub mod background;
pub mod before_storage;
pub mod kvs_core;

// Inline module declaration to avoid ambiguity between `kvs_layer.rs` and `kvs_layer/` dir.
// Re-export the directory-based module structure explicitly.
pub mod kvs_layer {
    #[path = "plumb_after.rs"]
    pub mod plumb_after;
    #[path = "plumb_before.rs"]
    pub mod plumb_before;
    #[path = "spec.rs"]
    pub mod spec;
    #[path = "types.rs"]
    pub mod types;

    pub use plumb_after::AfterPlumb;
    pub use plumb_before::KVSPlumb;
    pub use spec::KVSSpec;
    pub use types::{KVSCluster, KVSClusters, KVSNode};
}

pub mod cross_layer_flow;
pub use cross_layer_flow::CrossLayerFlowResult;
pub mod plumbing;
pub mod protocol;
pub mod store_state;
pub mod values;

// New Phase 0 events & consumer traits.
pub mod events;
pub use background::{BackgroundPlumb, MetaBackground};
pub use events::{
    DataConsumer, DataEvent, DataMetaConsumer, MetaConsumer, MetaEvent, MetaTeeAfter, TeeAfter,
};

#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::deploy::init_test();
    }
}
