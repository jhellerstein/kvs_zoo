stageleft::stageleft_no_entry_crate!();

pub mod cluster_spec;
pub mod cluster_spec_recursive;
pub mod demo_driver;
pub mod dispatch;
pub mod kvs_core;
pub mod maintenance;
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
