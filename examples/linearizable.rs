//! Linearizable KVS Example
//!
//! Architecture: Paxos (global ordering) + RoundRobin (per-replica execution) +
//! LogBased<BroadcastReplication> (replication & slot gap handling if slotted).
//!
//! This example does ONLY the Paxos ordering externally and then defers routing
//! and replication to standard KVS Zoo layer logic. Proposer/Acceptor clusters
//! are the only Paxos-specific wiring required. Ordered ops are handed to the
//! normal KVS pipeline.

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*; // macros q!, nondet!, stream/cluster traits
use kvs_zoo::dispatch::ordering::paxos::{PaxosConfig, PaxosDispatcher, paxos_order_to_proxy};
use kvs_zoo::dispatch::ordering::paxos_core::{Acceptor, Proposer};
use kvs_zoo::dispatch::routing::{RoundRobinRouter, SingleNodeRouter};
use kvs_zoo::kvs_layer::{AfterWire, KVSCluster, KVSSpec, KVSWire};
use kvs_zoo::maintenance::{BroadcastReplication, LogBased, Responder};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct ReplicaCluster;
#[derive(Clone)]
struct ReplicaLeaf;

// Single-layer linearizable KVS: RoundRobin routing + LogBased replication.
// Paxos ordering is applied before entering this layer (not part of the spec).
type LinearizableKVS = KVSCluster<
    ReplicaCluster,
    RoundRobinRouter,
    LogBased<BroadcastReplication<LwwWrapper<String>>>,
    kvs_zoo::kvs_layer::KVSNode<ReplicaLeaf, SingleNodeRouter, Responder>,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo (Paxos + LogBased<Broadcast>)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture: routing + replication handled inside KVS Zoo.
    let kvs_spec = LinearizableKVS::new(
        RoundRobinRouter::new(),
        LogBased::new(BroadcastReplication::<LwwWrapper<String>>::new()),
        kvs_zoo::kvs_layer::KVSNode::<ReplicaLeaf, SingleNodeRouter, Responder>::new(
            SingleNodeRouter::new(),
            Responder::new(),
        ),
    );

    // Create clusters for KVS and Paxos roles
    let mut layers = kvs_zoo::kvs_layer::KVSClusters::new();
    let _entry = kvs_spec.create_clusters(&flow, &mut layers);
    let proposers = flow.cluster::<Proposer>();
    let acceptors = flow.cluster::<Acceptor>();

    // External bidirectional port
    let (bidi_port, operations_stream, _membership, complete_sink) = proxy
        .bidi_external_many_bincode::<_, KVSOperation<LwwWrapper<String>>, String>(
            &client_external,
        );

    // Build client operations stream
    let initial_ops = operations_stream
        .entries()
        .map(q!(|(_client_id, op)| op))
        .assume_ordering(nondet!(/** client op stream */));

    // Impose total order via Paxos and bring ordered ops back to the proxy.
    let dispatcher = PaxosDispatcher::<LwwWrapper<String>>::with_config(PaxosConfig::default());
    let ordered_ops_at_proxy = paxos_order_to_proxy(
        &dispatcher,
        initial_ops,
        &proposers,
        &acceptors,
        &proxy,
    );

    // Server generic wiring: downward dispatch -> leaf core -> upward maintenance
    let routed = kvs_spec.wire_from_process(&layers, ordered_ops_at_proxy);
    let (responses, _puts) = kvs_zoo::kvs_core::KVSCore::process_with_deltas(routed);
    let responses = kvs_spec.after_responses(&layers, responses);

    // Send responses back to proxy and complete the bidi connection (optional member id stamping)
    let proxy_responses = responses.send_bincode(&proxy);
    let stamp_member = std::env::var("KVS_STAMP_MEMBER").map(|v| v != "0").unwrap_or(false);
    let to_complete = if stamp_member {
        proxy_responses
            .entries()
            .map(q!(|(member_id, response)| (0u64, format!("[{}] {}", member_id, response))))
            .into_keyed()
    } else {
        proxy_responses
            .entries()
            .map(q!(|(_member_id, response)| (0u64, response)))
            .into_keyed()
    };
    complete_sink.complete(to_complete);

    // Deploy: 3 KVS replicas
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<ReplicaCluster>(),
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            &proposers,
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_cluster(
            &acceptors,
            vec![localhost.clone(), localhost.clone(), localhost.clone()],
        )
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(bidi_port).await;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Demo operations
    let ops = vec![
        KVSOperation::Put("acct".into(), LwwWrapper::new("100".into())),
        KVSOperation::Get("acct".into()),
        KVSOperation::Put("acct".into(), LwwWrapper::new("200".into())),
        KVSOperation::Get("acct".into()),
    ];
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }

    deployment.stop().await?;
    println!("✅ Linearizable demo complete");
    Ok(())
}
