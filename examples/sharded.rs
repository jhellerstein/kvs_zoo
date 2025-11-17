mod demo;

use clap::Parser;
use demo::{DemoArgs, run_demo};
use kvs_zoo::before_storage::routing::ShardedRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;
use std::time::Duration;

// Marker type naming this example layer.
#[derive(Clone)]
struct Shard;

// Sharded architecture: hash-based ShardedRouter Before storage, no After storage, no Child layers.
type ShardedKVS = KVSCluster<Shard, ShardedRouter, (), ()>;

const START_BANNER: &str = "🚀 Sharded Local KVS Demo";
const FINISH_BANNER: &str = "✅ Sharded local demo complete";
const SHARDS: usize = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = DemoArgs::parse();

    run_example(&args, operations()).await
}

async fn run_example(
    args: &DemoArgs,
    operations: Vec<KVSOperation<LwwWrapper<String>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_demo(
        START_BANNER,
        FINISH_BANNER,
        ShardedKVS::default(),
        |spec| spec.before = ShardedRouter::new(SHARDS),
        |host| vec![host.clone(); SHARDS],
        |layers| layers.get::<Shard>(),
        |_, _| Vec::new(),
        operations,
        annotate_shard,
        Duration::from_millis(500),
        |_, _| None,
        &args.graph,
    )
    .await
}

fn operations() -> Vec<KVSOperation<LwwWrapper<String>>> {
    use kvs_zoo::protocol::KVSOperation as Op;

    fn wrap(_node: &str, value: &str) -> LwwWrapper<String> {
        LwwWrapper::new(value.to_string())
    }

    vec![
        Op::Put("user:1".into(), wrap("client", "alice")),
        Op::Put("user:2".into(), wrap("client", "bob")),
        Op::Get("user:1".into()),
        Op::Get("user:2".into()),
    ]
}

fn annotate_shard(op: &KVSOperation<LwwWrapper<String>>) -> Vec<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) | KVSOperation::Delete(key) => {
            let shard_id = ShardedRouter::calculate_shard_id(key, SHARDS);
            vec![format!("→ shard {} for '{}'", shard_id, key)]
        }
    }
}
