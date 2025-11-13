stageleft::stageleft_no_entry_crate!();

// Legacy dispatch module has been migrated to `before_storage` and removed from the public API.
pub mod after_storage;
pub mod before_storage;
pub mod kvs_core;
// The kvs_layer module is implemented as a directory of submodules (mod.rs + files)
// to keep the types/traits (spec, wire_down, wire_up) small and readable.
pub mod kvs_layer;
pub mod layer_flow;
pub mod protocol;
pub mod server;
pub mod values;

#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::deploy::init_test();
    }
}
