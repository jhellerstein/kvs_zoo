stageleft::stageleft_no_entry_crate!();

// Legacy dispatch module has been migrated to `before_storage` and removed from the public API.
pub mod before_storage;
pub mod after_storage;
pub mod kvs_core;
pub mod kvs_layer;
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
