use clap::Parser;
use futures::{SinkExt, StreamExt};
use hydro_deploy::localhost::LocalhostHost;
use hydro_lang::location::cluster::Cluster;
use hydro_lang::viz::config::GraphConfig;
use kvs_zoo::background::BackgroundPlumb;
use kvs_zoo::kvs_core::KVSNode;
use kvs_zoo::kvs_layer::{AfterPlumb, KVSClusters, KVSPlumb, KVSSpec, ReplicationPlumb};
use kvs_zoo::plumbing::plumb_kvs_dataflow;
use kvs_zoo::protocol::KVSOperation;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
pub struct DemoArgs {
    #[clap(flatten)]
    pub graph: GraphConfig,
}

#[allow(dead_code)]
pub async fn run_demo<V, K, Configure, Hosts, ClusterSel, ExtraClusters, Annotate, PostStep>(
    start_banner: &str,
    finish_banner: &str,
    mut spec: K,
    configure_spec: Configure,
    host_layout: Hosts,
    cluster_selector: ClusterSel,
    extra_clusters: ExtraClusters,
    operations: Vec<KVSOperation<V>>,
    annotate: Annotate,
    initial_delay: Duration,
    post_step_delay: PostStep,
    graph: &GraphConfig,
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
    K: KVSSpec<V> + KVSPlumb<V> + AfterPlumb<V> + ReplicationPlumb<V> + BackgroundPlumb<V>,
    Configure: FnOnce(&mut K),
    Hosts: Fn(&Arc<LocalhostHost>) -> Vec<Arc<LocalhostHost>>,
    ClusterSel: for<'a> Fn(&'a KVSClusters<'a>) -> &'a Cluster<'a, KVSNode>,
    ExtraClusters: for<'a> Fn(
        &'a KVSClusters<'a>,
        &Arc<LocalhostHost>,
    ) -> Vec<(&'a Cluster<'a, KVSNode>, Vec<Arc<LocalhostHost>>)>,
    Annotate: Fn(&KVSOperation<V>) -> Vec<String>,
    PostStep: Fn(usize, &KVSOperation<V>) -> Option<Duration>,
{
    println!("{start_banner}");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();

    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    configure_spec(&mut spec);

    let (layers, port) = plumb_kvs_dataflow::<V, _>(&proxy, &client_external, &flow, spec);

    let built = flow.finalize();
    built.generate_graph_with_config(graph, None)?;
    if graph.should_exit_after_graph_generation() {
        return Ok(());
    }

    let hosts = host_layout(&localhost);
    let mut deploy_builder = built
        .with_default_optimize()
        .with_process(&proxy, localhost.clone())
        .with_cluster(cluster_selector(&layers), hosts);

    for (cluster, cluster_hosts) in extra_clusters(&layers, &localhost) {
        deploy_builder = deploy_builder.with_cluster(cluster, cluster_hosts);
    }

    let nodes = deploy_builder
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (mut out, mut input) = nodes.connect_bincode(port).await;

    deployment.start().await?;
    tokio::time::sleep(initial_delay).await;

    for (i, op) in operations.into_iter().enumerate() {
        for line in annotate(&op) {
            println!("{line}");
        }
        let delay = post_step_delay(i, &op);
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {resp}");
        }
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }

    deployment.stop().await?;
    println!("{finish_banner}");
    Ok(())
}
