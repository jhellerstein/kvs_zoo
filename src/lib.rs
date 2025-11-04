stageleft::stageleft_no_entry_crate!();

pub mod client;
pub mod core;
pub mod driver;
pub mod kvs_types;
pub mod linearizable;
pub mod lww;
pub mod protocol;
pub mod replicated;
pub mod routers;
pub mod server;
pub mod sharded;
pub mod values;

// The run_kvs_demo macro is exported via #[macro_export] in driver.rs
// and is available as kvs_zoo::run_kvs_demo! in examples

#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::deploy::init_test();
    }
}
