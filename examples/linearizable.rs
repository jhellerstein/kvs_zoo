//! Linearizable KVS (Paxos ordering as a top layer feeding slotted replication)

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*; // macros q!, nondet!, stream/cluster traits
// Paxos ordering and roles
use kvs_zoo::before_storage::ordering::paxos_core::{Acceptor, Proposer};
use kvs_zoo::before_storage::ordering::paxos::{PaxosConfig, PaxosDispatcher, paxos_order_slotted};
use kvs_zoo::before_storage::ordering::SlotOrderEnforcer;
use kvs_zoo::before_storage::routing::RoundRobinRouter;
use kvs_zoo::kvs_layer::{KVSSpec, KVSCluster};
use kvs_zoo::after_storage::replication::{LogBasedDelivery, SlottedBroadcastReplication as BroadcastReplication};
use kvs_zoo::after_storage::responders::Responder;
use kvs_zoo::protocol::{Envelope, KVSOperation};
use kvs_zoo::server::wire_two_layer_from_enveloped;
use kvs_zoo::values::LwwWrapper;

#[derive(Clone)]
struct OrderedCluster;
#[derive(Clone)]
struct ReplicaLeaf;

// Two-layer linearizable KVS:
// - Cluster layer: RoundRobin routing + slotted log-based replication (delivery in slot order)
// - Leaf layer: SlotOrderEnforcer (ensures per-member application in slot order) + simple responder
type LinearizableKVS = KVSCluster<
    OrderedCluster,
    RoundRobinRouter,
    LogBasedDelivery<BroadcastReplication<LwwWrapper<String>>>,
    kvs_zoo::kvs_layer::KVSNode<ReplicaLeaf, SlotOrderEnforcer, Responder>,
>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Linearizable KVS Demo (Paxos → LogBased<SlottedBroadcast> → SlotEnforce)");

    // Standard Hydro deployment
    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    // Define KVS architecture: cluster replication handles slotted log-based delivery.
    let kvs_spec = LinearizableKVS::new(
        RoundRobinRouter::new(),
        LogBasedDelivery::new(BroadcastReplication::<LwwWrapper<String>>::new()),
        kvs_zoo::kvs_layer::KVSNode::<ReplicaLeaf, SlotOrderEnforcer, Responder>::new(
            SlotOrderEnforcer::new(),
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

    // Impose total order via Paxos (produce slot-numbered operations at Proposers)
    let dispatcher = PaxosDispatcher::<LwwWrapper<String>>::with_config(PaxosConfig::default());
    let slotted_ops = paxos_order_slotted(&dispatcher, initial_ops, &proposers, &acceptors);

    // Preserve slot metadata to the leaf using Envelope<slot, op>, and wire two-layer pipeline.
    // Note: Convert to the proxy for wiring convenience (Process-located stream).
    let enveloped_at_proxy = slotted_ops
        .map(q!(|(slot, op)| Envelope::new(slot, op)))
        .send_bincode(&proxy)
        .entries()
        .map(q!(|(_member_id, env)| env))
        .assume_ordering(nondet!(/** paxos ordered at proxy */));

    // Wire: cluster layer routing/replication -> leaf slot enforcement -> core -> after_storage up
    let responses = wire_two_layer_from_enveloped(&proxy, &layers, &kvs_spec, enveloped_at_proxy);

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

    // Deploy: 3 KVS replicas + 3 proposers + 3 acceptors
    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(
            layers.get::<OrderedCluster>(),
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
