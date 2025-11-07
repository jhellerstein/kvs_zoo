stageleft::stageleft_no_entry_crate!();

pub mod config;
pub mod core;
pub mod interception;
pub mod kvs_types;
pub mod linearizable;
pub mod lww;
pub mod protocol;
pub mod replicated;
pub mod maintain;
pub mod sequencing;
pub mod sequential;
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
