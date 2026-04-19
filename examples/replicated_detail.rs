//! Replicated KVS (single shard + After-stage gossip replication)
//!
//! Uses CausalWrapper<String> values so gossip replication can merge
//! via lattice semantics (coordination-free convergence).

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use kvs_zoo::after_storage::ReplicationStrategy;
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::plumbing::extract_put_deltas;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::CausalWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS Demo (gossip + CausalWrapper)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let mut flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    let replicas = flow.cluster::<kvs_zoo::kvs_core::KVSNode>();

    let (port, operations_stream, _membership, complete_sink) = proxy
        .bidi_external_many_bincode::<_, KVSOperation<String, CausalWrapper<String>>, String>(
            &client_external,
        );

    let initial_ops = operations_stream
        .entries()
        .map(q!(|(client_id, op)| op.with_client_id(Some(client_id))))
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** client op stream */),
        );

    let routed_ops = initial_ops
        .map(q!(|op| (
            hydro_lang::location::MemberId::from_raw_id(0u32),
            op
        )))
        .into_keyed()
        .demux(&replicas, TCP.fail_stop().bincode())
        .assume_ordering::<hydro_lang::live_collections::stream::NoOrder>(
            nondet!(/** routed to single member */),
        );

    // After-storage flow: derive applied PUT deltas from the core and replicate them
    let (_ops_clone, local_put_deltas) = extract_put_deltas(routed_ops.clone());

    // Use the After-stage gossip strategy to replicate PUT deltas to peers
    let gossip = SimpleGossip::<String, CausalWrapper<String>>::default();
    let replicated_puts = gossip.replicate_data(&replicas, local_put_deltas);

    // Merge local ops (with client_id) with replicated PUTs (client_id=None, no respond)
    let replicated_ops = replicated_puts.map(q!(|(k, v)| kvs_zoo::protocol::KVSOperation::Put(k, v, 0, None)));
    let all_ops = routed_ops
        .merge_unordered(replicated_ops);

    // Lattice merge path: CausalWrapper values converge via commutative merge
    let kvs_zoo::kvs_core::CoreOutput {
        responses,
        data,
        meta,
    } = KVSCore::process_lattice(all_ops);
    let _data_keep_alive = data.inspect(q!(|event| println!("[after] data {:?}", event)));
    let _meta_keep_alive = meta.inspect(q!(|event| println!("[after] meta {:?}", event)));

    let proxy_responses = responses.send(&proxy, TCP.fail_stop().bincode());
    let to_complete = proxy_responses
        .entries()
        .filter_map(q!(|(_member_id, response)| {
            response.client_id().map(|cid| (cid, response.to_string()))
        }))
        .into_keyed();
    complete_sink.complete(to_complete);

    let built = flow.finalize();

    // Deploy: 3 replicas for the cluster
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            &replicas,
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run demo operations
    use kvs_zoo::protocol::KVSOperation as Op;
    use kvs_zoo::values::VCWrapper;
    let ops = vec![
        Op::Put("alpha".into(), CausalWrapper::new(VCWrapper::new(), "one".to_string()), 1, None),
        Op::Get("alpha".into(), 2, None),
        Op::Put("beta".into(), CausalWrapper::new(VCWrapper::new(), "two".to_string()), 3, None),
        Op::Get("beta".into(), 4, None),
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
