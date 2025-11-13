//! Replicated KVS (RoundRobin + Gossip)

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

// Marker type naming this KVS layer
#[allow(dead_code)]
#[derive(Clone)]
struct Replica;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(" Replicated KVS Demo (gossip)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture
    let replicas = flow.cluster::<kvs_zoo::kvs_core::KVSNode>();

    // Build a Hydro graph for the ReplicatedKVS type, return layer handles and client I/O ports
    let (port, operations_stream, _membership, complete_sink) =
        proxy.bidi_external_many_bincode::<_, KVSOperation<LwwWrapper<String>>, String>(&client_external);

    let initial_ops = operations_stream
        .entries()
        .map(q!(|(_client_id, op)| op))
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** client op stream */),
        );

    let routed_ops = initial_ops
        .map(q!(|op| (hydro_lang::location::MemberId::from_raw(0u32), op)))
        .into_keyed()
        .demux_bincode(&replicas)
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** routed to single member */),
        );

    let local_tagged = routed_ops.clone().map(q!(|op| (true, op)));

    let local_puts = routed_ops.filter_map(q!(|op| match op {
        KVSOperation::Put(k, v) => Some((k, v)),
        KVSOperation::Get(_) => None,
    }));

    let cluster_members = replicas
        .source_cluster_members(&replicas)
        .map_with_key(q!(|(member_id, _event)| member_id))
        .values();

    let gossip_sent = local_puts
        .clone()
        .cross_product(cluster_members.clone().assume_retries(nondet!(/** member list OK */)))
        .map(q!(|(tuple, member_id)| (member_id, tuple)))
        .into_keyed()
        .demux_bincode(&replicas);

    let replicated_puts = gossip_sent
        .values()
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** gossip messages unordered */),
        )
        .assume_retries::<hydro_lang::live_collections::stream::ExactlyOnce>(
            nondet!(/** gossip retries OK */),
        );

    let replicated_tagged = replicated_puts.map(q!(|(k, v)| (false, KVSOperation::Put(k, v))));

    let all_tagged = local_tagged
        .interleave(replicated_tagged)
        .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
            nondet!(/** sequential processing per node */),
        );

    let responses = KVSCore::process_with_responses(all_tagged);

    let proxy_responses = responses.send_bincode(&proxy);
    let to_complete = proxy_responses
        .entries()
        .map(q!(|(_member_id, response)| (0u64, response)))
        .into_keyed();
    complete_sink.complete(to_complete);

    // Deploy: 3 replicas for the cluster
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&replicas, vec![localhost.clone(), localhost.clone(), localhost.clone()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Run demo operations
    use kvs_zoo::protocol::KVSOperation as Op;
    let ops = vec![
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Put("beta".into(), LwwWrapper::new("two".into())),
        Op::Get("beta".into()),
    ];

    for (i, op) in ops.into_iter().enumerate() {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
        if i == 0 || i == 2 {
            // brief pause after first PUTs for gossip
            tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        }
    }

    deployment.stop().await?;
    println!("✅ Replicated (gossip) demo complete");
    Ok(())
}
