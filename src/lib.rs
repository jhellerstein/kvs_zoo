stageleft::stageleft_no_entry_crate!();

pub mod dispatch;
pub mod kvs_core;
pub mod kvs_types;
pub mod linearizable;
pub mod maintain;
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
