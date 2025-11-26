//! Local KVS (detailed wiring, single node)

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("🚀 Local KVS Demo (detailed)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Single-node cluster
    let local = flow.cluster::<kvs_zoo::kvs_core::KVSNode>();

    // Build client I/O ports
    let (port, operations_stream, _membership, complete_sink) = proxy
        .bidi_external_many_bincode::<_, KVSOperation<String, LwwWrapper<String>>, String>(
            &client_external,
        );

    // Route all ops to the single member (id 0), attaching client_id to operations
    let routed_ops = operations_stream
        .entries()
        .map(q!(|(client_id, op)| (
            hydro_lang::location::MemberId::from_raw_id(0u32),
            op.with_client_id(Some(client_id))
        )))
        .into_keyed()
        .demux_bincode(&local);

    // Per-node total ordering for correctness
    let ordered_ops = routed_ops
        .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
            nondet!(/** sequential processing via a single node */),
        );

    // No replication: just process operations and emit responses
    let kvs_zoo::kvs_core::CoreOutput {
        responses,
        data,
        meta,
    } = KVSCore::process_hashmap::<_, _, _>(ordered_ops);
    data.for_each(q!(|_data| ())); // Local demo currently ignores data events
    meta.for_each(q!(|_meta| ())); // Local demo currently ignores metadata

    // Send responses back to proxy and complete the client request
    let proxy_responses = responses.send_bincode(&proxy);
    let to_complete = proxy_responses
        .entries()
        .filter_map(q!(|(_member_id, response)| {
            response.client_id().map(|cid| (cid, response.to_string()))
        }))
        .into_keyed();
    complete_sink.complete(to_complete);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;
    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: single node
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(&local, vec![localhost.clone()])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Run demo operations
    use kvs_zoo::protocol::KVSOperation as Op;
    let ops = vec![
        Op::Put("alpha".into(), LwwWrapper::new("one".into()), None),
        Op::Get("alpha".into(), None),
        Op::Put("alpha".into(), LwwWrapper::new("two".into()), None),
        Op::Get("alpha".into(), None),
    ];
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    println!("✅ Local detailed demo complete");
    deployment.stop().await?;
    Ok(())
}
