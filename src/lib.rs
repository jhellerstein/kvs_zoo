stageleft::stageleft_no_entry_crate!();

pub mod client;
pub mod core;
// pub mod kvs_types; // Temporarily disabled due to Hydro compilation issues with crate:: paths
pub mod linearizable;
pub mod lww;
pub mod protocol;
pub mod replicated;
pub mod routers;
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
