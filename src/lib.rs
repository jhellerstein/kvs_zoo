stageleft::stageleft_no_entry_crate!();

pub mod config;
pub mod core;
pub mod kvs_types;
pub mod lww;
pub mod interception;
pub mod linearizable;
pub mod protocol;
pub mod replicated;
pub mod replication;
pub mod sequential;
pub mod sequential_concrete;
pub mod server;
pub mod sharded;
pub mod values;

#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::deploy::init_test();
    }
}
