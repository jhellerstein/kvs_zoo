mod demo;

use clap::Parser;
use demo::{DemoArgs, run_demo};
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::{CausalString, VCWrapper};
use std::time::Duration;

// Marker type naming this example layer.
#[derive(Clone)]
struct Replica;

// Replicated architecture: RoundRobinRouter Before to any replica, SimpleGossip After to replicate, no Child layers.
type ReplicatedKVS<V> = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<V>, ()>;

const START_BANNER: &str = "🚀 Replicated KVS Demo (gossip, causal)";
const FINISH_BANNER: &str = "✅ Replicated (gossip) demo complete";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = DemoArgs::parse();

    run_example::<CausalString, _, _>(&args, operations(), |_| Vec::new(), gossip_pause).await
}

async fn run_example<V, Annotate, PostStep>(
    args: &DemoArgs,
    operations: Vec<KVSOperation<V>>,
    annotate: Annotate,
    post_step: PostStep,
) -> Result<(), Box<dyn std::error::Error>>
where
    V: Clone
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + PartialEq
        + Eq
        + Default
        + std::fmt::Debug
        + std::fmt::Display
        + lattices::Merge<V>
        + Send
        + Sync
        + std::hash::Hash
        + 'static,
    Annotate: Fn(&KVSOperation<V>) -> Vec<String>,
    PostStep: Fn(usize, &KVSOperation<V>) -> Option<Duration>,
{
    run_demo(
        START_BANNER,
        FINISH_BANNER,
        ReplicatedKVS::<V>::default(),
        |spec| spec.after = SimpleGossip::new(100usize),
        |host| vec![host.clone(), host.clone(), host.clone()],
        |layers| layers.get::<Replica>(),
        |_, _| Vec::new(),
        operations,
        annotate,
        Duration::from_millis(500),
        post_step,
        &args.graph,
    )
    .await
}

fn operations() -> Vec<KVSOperation<CausalString>> {
    use kvs_zoo::protocol::KVSOperation as Op;

    fn wrap(node: &str, value: &str) -> CausalString {
        let mut clock = VCWrapper::new();
        clock.bump(node.to_string());
        CausalString::new(clock, value.to_string())
    }

    vec![
        Op::Put("user:1".into(), wrap("client", "alice")),
        Op::Put("user:2".into(), wrap("client", "bob")),
        Op::Get("user:1".into()),
        Op::Get("user:2".into()),
    ]
}

fn gossip_pause<V>(index: usize, _: &KVSOperation<V>) -> Option<Duration> {
    if index == 0 || index == 2 {
        Some(Duration::from_millis(350))
    } else {
        None
    }
}
