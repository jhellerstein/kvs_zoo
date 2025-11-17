mod demo;

use clap::Parser;
use demo::{DemoArgs, run_demo};
use kvs_zoo::before_storage::routing::SingleNodeRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;
use std::time::Duration;

// Marker type naming this example layer.
#[derive(Clone)]
struct LocalStorage;

// Single-node architecture: SingleNodeRouter Before storage, no After storage, no Child layers.
type LocalKVS = KVSCluster<LocalStorage, SingleNodeRouter, (), ()>;

const START_BANNER: &str = "🚀 Local KVS Demo (single node)";
const FINISH_BANNER: &str = "✅ Local demo complete";

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
        LocalKVS::default(),
        |_| {},
        |host| vec![host.clone()],
        |layers| layers.get::<LocalStorage>(),
        |_, _| Vec::new(),
        operations,
        |_| Vec::new(),
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
