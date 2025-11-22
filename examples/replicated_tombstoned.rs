//! Replicated KVS with Tombstone-Based Deletion (RoundRobin + Gossip)
//!
//! This example demonstrates tombstone-based deletion in a replicated KVS.
//! Unlike standard deletion (which removes keys), tombstone deletion marks
//! keys as permanently deleted. Once tombstoned, a key cannot be resurrected
//! by subsequent PUT operations.

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::after_storage::replication::SimpleGossip;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::KVSCluster;
use kvs_zoo::values::LwwWrapper;

// Marker type naming this KVS layer
#[derive(Clone)]
struct Replica;

// KVS architecture type: single layer with RoundRobin + Gossip
type ReplicatedTombstoneKVS<V> = KVSCluster<Replica, RoundRobinRouter, SimpleGossip<String, V>, ()>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(flatten)]
    graph: GraphConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run_example(&args).await
}

async fn run_example(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS with Tombstones Demo");
    println!("   This demo shows permanent tombstone-based deletion\n");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture via defaults; override only the gossip interval
    let mut kvs_spec: ReplicatedTombstoneKVS<LwwWrapper<String>> = Default::default();
    kvs_spec.after = SimpleGossip::new(100usize); // 100ms gossip interval

    // Build a Hydro graph for the ReplicatedTombstoneKVS type
    let (layers, port) = plumb_kvs_dataflow_with_tombstones(&proxy, &client_external, &flow, kvs_spec);

    let built = flow.finalize();
    built.generate_graph_with_config(&args.graph, None)?;

    if args.graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    // Deploy: 3 replicas for the cluster
    let nodes = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<Replica>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run demo operations showing tombstone behavior
    let ops = tombstone_demo_ops();
    
    for (i, (op, description)) in ops.into_iter().enumerate() {
        println!("Step {}: {}", i + 1, description);
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("   → {}", resp);
        }
        
        // Pause after operations to allow gossip propagation
        if i == 0 || i == 2 || i == 4 {
            tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        }
    }

    deployment.stop().await?;
    println!("\n✅ Replicated tombstone demo complete");
    println!("   Key observation: After deletion, PUT cannot resurrect the key!");
    Ok(())
}

/// Generate operations that demonstrate tombstone permanence
fn tombstone_demo_ops() -> Vec<(kvs_zoo::protocol::KVSOperation<String, LwwWrapper<String>>, &'static str)> {
    use kvs_zoo::protocol::KVSOperation as Op;

    vec![
        (
            Op::Put("x".to_string(), LwwWrapper::new("1".into()), None),
            "PUT x = \"1\"",
        ),
        (
            Op::Get("x".to_string(), None),
            "GET x (expect \"1\")",
        ),
        (
            Op::Delete("x".to_string(), None),
            "DELETE x (tombstone created)",
        ),
        (
            Op::Get("x".to_string(), None),
            "GET x (expect None - tombstoned)",
        ),
        (
            Op::Put("x".to_string(), LwwWrapper::new("2".into()), None),
            "PUT x = \"2\" (attempt resurrection)",
        ),
        (
            Op::Get("x".to_string(), None),
            "GET x (expect None - tombstone is permanent!)",
        ),
    ]
}

/// Custom plumbing function for tombstone-based storage.
///
/// This is a specialized version of `plumb_kvs_dataflow` that uses
/// `LocalHashMapFst<V>` for tombstone-based deletion instead of
/// standard `HashMap<String, V>`.
fn plumb_kvs_dataflow_with_tombstones<'a, V, K>(
    proxy: &hydro_lang::prelude::Process<'a, ()>,
    client_external: &hydro_lang::prelude::External<'a, ()>,
    flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    mut kvs: K,
) -> (
    kvs_zoo::kvs_layer::KVSClusters<'a>,
    hydro_lang::location::external_process::ExternalBincodeBidi<
        kvs_zoo::protocol::KVSOperation<String, V>,
        String,
        hydro_lang::location::external_process::Many,
    >,
)
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
        + lattices::LatticeFrom<V>
        + lattices::IsBot
        + Send
        + Sync
        + 'static,
    K: kvs_zoo::kvs_layer::KVSSpec<V>
        + kvs_zoo::kvs_layer::KVSPlumb<String, V>
        + kvs_zoo::kvs_layer::AfterPlumb<V>
        + kvs_zoo::kvs_layer::ReplicationPlumb<String, V>
        + kvs_zoo::background::BackgroundPlumb<String, V>,
{
    use hydro_lang::live_collections::stream::TotalOrder;
    use hydro_lang::prelude::*;
    use kvs_zoo::protocol::KVSOperation;

    // Create all clusters for all layers
    let mut layers = kvs_zoo::kvs_layer::KVSClusters::new();
    let _entry_cluster = kvs.create_clusters(flow, &mut layers);

    // Create bidirectional external connection
    let (bidi_port, operations_stream, _membership, complete_sink) =
        proxy.bidi_external_many_bincode::<_, KVSOperation<String, V>, String>(client_external);

    // Build initial operation stream from external input
    let initial_ops = operations_stream
        .entries()
        .map(q!(|(client_id, op)| op.with_client_id(Some(client_id))))
        .assume_ordering(nondet!(/** client op stream */));

    // Downward pass via before_storage chain (KVSPlumb)
    let routed_ops = kvs.plumb_from_process(&layers, initial_ops);

    // Split client operations so we can extract PUT deltas for replication
    let (client_ops, local_put_deltas) = kvs_zoo::plumbing::extract_put_deltas(routed_ops);

    // Fan out PUT deltas through any configured replication layers
    let (_pass_up, replication_ops) = kvs.replicate_puts(&layers, local_put_deltas);

    // Core processing for client-originating operations with TOMBSTONE STORAGE
    let kvs_zoo::kvs_core::CoreOutput {
        responses: client_responses,
        data: client_data_events,
        meta: client_meta_stream,
    } = kvs_zoo::kvs_core::KVSCore::process_tombstone_fst::<V, _>(client_ops);

    // Replicated operations flow through the same core path with TOMBSTONE STORAGE
    let kvs_zoo::kvs_core::CoreOutput {
        responses: replica_responses,
        data: replica_data_events,
        meta: replica_meta_stream,
    } = kvs_zoo::kvs_core::KVSCore::process_tombstone_fst::<V, _>(
        replication_ops.assume_ordering::<TotalOrder>(nondet!(/** replicated op order */)),
    );

    let combined_responses = client_responses
        .interleave(replica_responses)
        .assume_ordering::<TotalOrder>(nondet!(/** combined client+replica responses */));

    let data_events = client_data_events
        .interleave(replica_data_events)
        .assume_ordering::<TotalOrder>(nondet!(/** combined data events */));
    let meta_stream = client_meta_stream
        .interleave(replica_meta_stream)
        .assume_ordering::<TotalOrder>(nondet!(/** combined meta events */));

    // Background plumbing
    let (bg_data, bg_meta) = kvs.plumb_background(&layers, data_events, meta_stream);
    bg_data.for_each(q!(|_data| ()));
    bg_meta.for_each(q!(|_meta| ()));

    // Send KVSResponse structs to proxy
    let proxy_responses = combined_responses.send_bincode(proxy);

    // Extract client IDs and format responses for completion
    let to_complete = proxy_responses
        .entries()
        .filter_map(q!(|(_member_id, response)| {
            response.client_id().map(|cid| (cid, response.to_string()))
        }))
        .into_keyed();

    // Complete the bidirectional connection
    complete_sink.complete(to_complete);

    (layers, bidi_port)
}
