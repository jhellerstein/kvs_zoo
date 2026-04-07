use kvs_zoo::after_storage::replication::BroadcastReplication;
use kvs_zoo::kvs_layer::{spec::KVSSpec, types::KVSNode};

struct LeafLayer;

fn main() {
    fn _assert_spec()
    where
        KVSNode<LeafLayer, (), BroadcastReplication<String, String>>: KVSSpec<String>,
    {
    }

    _assert_spec();
}
