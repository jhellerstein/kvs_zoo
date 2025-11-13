//! Sharded KVS (hash-partitioned)

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use kvs_zoo::kvs_core::{KVSCore, KVSNode};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

// Marker type naming this KVS layer
#[allow(dead_code)]
#[derive(Clone)]
struct Shard;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded Local KVS Demo");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture
    let shards = flow.cluster::<KVSNode>();

    // Build a Hydro graph for the ShardedKVS type, return layer handles and client I/O ports
    let (port, operations_stream, _membership, complete_sink) =
        proxy.bidi_external_many_bincode::<_, KVSOperation<LwwWrapper<String>>, String>(&client_external);

    let initial_ops = operations_stream
        .entries()
        .map(q!(|(_client_id, op)| op))
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** client op stream */),
        );

    // Route each op by hashing its key to one of 3 shards
    let routed_ops = initial_ops
        .map(q!(|op| {
            match op {
                KVSOperation::Put(k, v) => {
                    let idx = kvs_zoo::before_storage::routing::ShardedRouter::calculate_shard_id(&k, 3usize);
                    (hydro_lang::location::MemberId::from_raw(idx), KVSOperation::Put(k, v))
                }
                KVSOperation::Get(k) => {
                    let idx = kvs_zoo::before_storage::routing::ShardedRouter::calculate_shard_id(&k, 3usize);
                    (hydro_lang::location::MemberId::from_raw(idx), KVSOperation::Get(k))
                }
            }
        }))
        .into_keyed()
        .demux_bincode(&shards)
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** routed to one shard */),
        );

    // Per-node processing in total order
    let ordered_ops = routed_ops.assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
        nondet!(/** sequential processing per node */),
    );

    let responses = KVSCore::process(ordered_ops);

    // Send responses back to proxy and complete the client request
    let proxy_responses = responses.send_bincode(&proxy);
    let to_complete = proxy_responses
        .entries()
        .map(q!(|(_member_id, response)| (0u64, response)))
        .into_keyed();
    complete_sink.complete(to_complete);

    // Deploy: 3 shards, 1 node each
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&shards, vec![localhost.clone(), localhost.clone(), localhost.clone()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run demo operations
    let ops = vec![
        KVSOperation::Put("user:1".into(), LwwWrapper::new("alice".into())),
        KVSOperation::Put("user:2".into(), LwwWrapper::new("bob".into())),
        KVSOperation::Get("user:1".into()),
        KVSOperation::Get("user:2".into()),
    ];
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Sharded local demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<LwwWrapper<String>>, shards: u64) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            let shard_id =
                kvs_zoo::before_storage::routing::ShardedRouter::calculate_shard_id(key, shards as usize);
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
