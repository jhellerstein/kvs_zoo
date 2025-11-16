use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::kvs_layer::{spec::KVSSpec, types::KVSNode};
use kvs_zoo::values::LwwWrapper;

struct LeafLayer;

fn main() {
    fn _assert_spec()
    where
        KVSNode<LeafLayer, (), Responder>: KVSSpec<LwwWrapper<String>>,
    {
    }

    _assert_spec();
}
