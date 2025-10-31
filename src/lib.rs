stageleft::stageleft_no_entry_crate!();

pub mod client;
pub mod core;
pub mod kvs_variants;
pub mod lattice_core;
pub mod local;
pub mod protocol;
pub mod proxy;
pub mod replicated;
pub mod sharded;
pub mod sharded_replicated;
pub mod vector_clock;
pub mod examples_support;

#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::deploy::init_test();
    }
}
