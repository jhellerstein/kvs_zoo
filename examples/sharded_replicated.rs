mod demo;

use clap::Parser;
use demo::{DemoArgs, run_demo};
use kvs_zoo::after_storage::replication::{BroadcastReplication, BroadcastReplicationConfig};
use kvs_zoo::before_storage::routing::{RoundRobinRouter, ShardedRouter};
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::{CausalString, VCWrapper};
use std::time::Duration;

#[derive(Clone)]
struct Shard;

#[derive(Clone)]
struct Replica;

type ShardedReplicatedKVS<V> = KVSCluster<
    Shard,
    ShardedRouter,
    (),
    KVSCluster<Replica, RoundRobinRouter, BroadcastReplication<V>, ()>,
>;

const START_BANNER: &str = "🚀 Sharded + Replicated KVS Demo";
const FINISH_BANNER: &str = "✅ Sharded+Replicated demo complete";
const SHARDS: usize = 3;
const REPLICAS: usize = 3;
const INITIAL_DELAY_MS: u64 = 600;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = DemoArgs::parse();

    run_example::<CausalString, _, _>(&args, operations(), annotate_shard, |_, _| None).await
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
        ShardedReplicatedKVS::<V>::default(),
        |spec| {
            spec.child.after =
                BroadcastReplication::with_config(BroadcastReplicationConfig::low_latency());
        },
        |host| vec![host.clone(); SHARDS],
        |layers| layers.get::<Shard>(),
        |layers, host| vec![(layers.get::<Replica>(), vec![host.clone(); REPLICAS])],
        operations,
        annotate,
        Duration::from_millis(INITIAL_DELAY_MS),
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

fn annotate_shard(op: &KVSOperation<CausalString>) -> Vec<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) | KVSOperation::Delete(key) => {
            let shard_id = ShardedRouter::calculate_shard_id(key, SHARDS);
            vec![format!("→ shard {} for '{}'", shard_id, key)]
        }
    }
}
