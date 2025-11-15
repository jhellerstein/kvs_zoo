//! Local KVS (detailed wiring, single node)

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        .bidi_external_many_bincode::<_, KVSOperation<LwwWrapper<String>>, String>(
            &client_external,
        );

    let initial_ops = operations_stream
        .entries()
        .map(q!(|(_client_id, op)| op))
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
        nondet!(/** client op stream */),
    );

    // Route all ops to the single member (id 0)
    let routed_ops = initial_ops
        .map(q!(|op| (
            hydro_lang::location::MemberId::from_raw(0u32),
            op
        )))
        .into_keyed()
        .demux_bincode(&local)
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** routed to single member */),
        );

    // Per-node total ordering for correctness
    let ordered_ops = routed_ops
        .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
            nondet!(/** sequential processing per node */),
        );

    // No replication: just process operations and emit responses
    let tagged = ordered_ops.map(q!(|op| kvs_zoo::protocol::Envelope::new(true, op)));
    let kvs_zoo::kvs_core::CoreOutput {
        responses,
        data,
        meta,
    } = KVSCore::process(tagged);
    data.for_each(q!(|_data| ())); // Local demo currently ignores data events
    meta.for_each(q!(|_meta| ())); // Local demo currently ignores metadata

    // Send responses back to proxy and complete the client request
    let proxy_responses = responses.send_bincode(&proxy);
    let to_complete = proxy_responses
        .entries()
        .map(q!(|(_member_id, response)| (0u64, response)))
        .into_keyed();
    complete_sink.complete(to_complete);

    // Deploy: single node
    let nodes = flow
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
        Op::Put("alpha".into(), LwwWrapper::new("one".into())),
        Op::Get("alpha".into()),
        Op::Put("alpha".into(), LwwWrapper::new("two".into())),
        Op::Get("alpha".into()),
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
