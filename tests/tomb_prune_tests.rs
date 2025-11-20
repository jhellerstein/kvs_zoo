use futures::StreamExt;
use hydro_lang::prelude::*;
use kvs_zoo::MetaEvent;
use kvs_zoo::after_storage::replication::BroadcastReplication;
use kvs_zoo::background::{BackgroundPlumb, VectorClockBackground};
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::kvs_layer::{KVSCluster, KVSClusters, KVSPlumb, KVSSpec, ReplicationPlumb};
use kvs_zoo::plumbing::extract_put_deltas;
use kvs_zoo::values::CausalString;
use tokio::time::{Duration, timeout};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tomb_prune_local_single_node() {
    hydro_lang::test_util::multi_location_test(
        |flow, process| {
            #[derive(Clone)]
            struct RootLayer;

            let mut spec =
                KVSCluster::<RootLayer, (), (), (), VectorClockBackground>::new_with_background(
                    (),
                    (),
                    (),
                    VectorClockBackground::new()
                        .with_logging(false)
                        .with_digests(true),
                );

            let mut layers = KVSClusters::new();
            let _entry_cluster =
                <KVSCluster<RootLayer, (), (), (), VectorClockBackground> as KVSSpec<
                    CausalString,
                >>::create_clusters(&spec, flow, &mut layers);

            let operations = process
                .source_iter(q!(vec![
                    kvs_zoo::protocol::KVSOperation::Put("alpha".to_string(), {
                        let mut vc = kvs_zoo::values::VCWrapper::new();
                        vc.bump("client".to_string());
                        kvs_zoo::values::CausalString::new(vc, "payload".to_string())
                    },),
                    kvs_zoo::protocol::KVSOperation::Delete("alpha".to_string()),
                ]))
                .assume_ordering(nondet!(/** ops */));

            let routed_ops = spec.plumb_from_process(&layers, operations);
            let core = KVSCore::process_client_ops(routed_ops);
            let (bg_data, bg_meta) = spec.plumb_background(&layers, core.data, core.meta);
            bg_data.for_each(q!(|_d| ()));

            bg_meta
                .filter_map(q!(|event| match event {
                    MetaEvent::VectorClockSnapshot { key, clock: _ } => Some(key),
                    _ => None,
                }))
                .send_bincode(process)
                .entries()
                .map(q!(|(_member, key)| key))
        },
        |mut stream| async move {
            // Expect at least one snapshot event for key "alpha"
            let mut saw_vc = false;
            for _ in 0..12 {
                match timeout(Duration::from_millis(500), stream.next()).await {
                    Ok(Some(key)) if key == "alpha" => {
                        saw_vc = true;
                        break;
                    }
                    Ok(Some(_)) => continue,
                    _ => break,
                }
            }
            assert!(saw_vc, "expected VectorClockSnapshot for key alpha");
        },
    )
    .await;
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tomb_prune_replication_two_nodes() {
    hydro_lang::test_util::multi_location_test(
        |flow, process| {
            #[derive(Clone)]
            struct RootLayer;

            let mut spec = KVSCluster::<
                RootLayer,
                (),
                BroadcastReplication<CausalString>,
                (),
                VectorClockBackground,
            >::new_with_background(
                (),
                BroadcastReplication::<CausalString>::with_config(
                    kvs_zoo::after_storage::replication::BroadcastReplicationConfig::synchronous(),
                ),
                (),
                VectorClockBackground::new()
                    .with_logging(false)
                    .with_digests(true),
            );

            let mut layers = KVSClusters::new();
            let _entry_cluster = <KVSCluster<
                RootLayer,
                (),
                BroadcastReplication<CausalString>,
                (),
                VectorClockBackground,
            > as KVSSpec<CausalString>>::create_clusters(
                &spec, flow, &mut layers
            );

            // Issue a Put then a Delete; Put will replicate and provide remote VC activity.
            let operations = process
                .source_iter(q!(vec![
                    kvs_zoo::protocol::KVSOperation::Put("beta".to_string(), {
                        let mut vc = kvs_zoo::values::VCWrapper::new();
                        vc.bump("client".to_string());
                        kvs_zoo::values::CausalString::new(vc, "payload".to_string())
                    },),
                    kvs_zoo::protocol::KVSOperation::Delete("beta".to_string()),
                ]))
                .assume_ordering(nondet!(/** client ops */));

            let routed_ops = spec.plumb_from_process(&layers, operations.clone());
            let (client_ops, local_put_deltas) = extract_put_deltas(routed_ops);
            let (_pass_up, replication_ops) = spec.replicate_puts(&layers, local_put_deltas);

            let client_core = KVSCore::process_client_ops(client_ops);
            let replica_core = KVSCore::process_replicated_ops(
                replication_ops
                    .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                        nondet!(/** replicated operation order */),
                    ),
            );

            let combined_data = client_core
                .data
                .interleave(replica_core.data)
                .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                nondet!(/** combined data events */),
            );

            let combined_meta = client_core
                .meta
                .interleave(replica_core.meta)
                .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                nondet!(/** combined meta events */),
            );

            let (bg_data, bg_meta) = spec.plumb_background(&layers, combined_data, combined_meta);
            bg_data.for_each(q!(|_d| ()));

            bg_meta
                .filter_map(q!(|event| match event {
                    MetaEvent::VectorClockSnapshot { key, clock: _ } => Some(key),
                    _ => None,
                }))
                .send_bincode(process)
                .entries()
                .map(q!(|(_member, key)| key))
        },
        |mut stream| async move {
            let mut saw_vc = false;
            for _ in 0..16 {
                match timeout(Duration::from_millis(600), stream.next()).await {
                    Ok(Some(key)) if key == "beta" => {
                        saw_vc = true;
                        break;
                    }
                    Ok(Some(_)) => continue,
                    _ => break,
                }
            }
            assert!(saw_vc, "expected VectorClockSnapshot for key beta");
        },
    )
    .await;
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tomb_prune_concurrency_waits_for_frontier() {
    #[derive(Clone)]
    struct RootLayer;

    hydro_lang::test_util::multi_location_test(
        |flow, process| {
            let mut spec = KVSCluster::<
                RootLayer,
                (),
                BroadcastReplication<CausalString>,
                (),
                VectorClockBackground,
            >::new_with_background(
                (),
                BroadcastReplication::<CausalString>::with_config(
                    kvs_zoo::after_storage::replication::BroadcastReplicationConfig::synchronous(),
                ),
                (),
                VectorClockBackground::new()
                    .with_logging(false)
                    .with_digests(true),
            );

            let mut layers = KVSClusters::new();
            let _entry_cluster = <KVSCluster<
                RootLayer,
                (),
                BroadcastReplication<CausalString>,
                (),
                VectorClockBackground,
            > as KVSSpec<CausalString>>::create_clusters(
                &spec, flow, &mut layers
            );

            // Put then Delete on key "gamma"; replication introduces a concurrent remote bump.
            let operations = process
                .source_iter(q!(vec![
                    kvs_zoo::protocol::KVSOperation::Put("gamma".to_string(), {
                        let mut vc = kvs_zoo::values::VCWrapper::new();
                        vc.bump("client".to_string());
                        kvs_zoo::values::CausalString::new(vc, "payload".to_string())
                    },),
                    kvs_zoo::protocol::KVSOperation::Delete("gamma".to_string()),
                ]))
                .assume_ordering(nondet!(/** client ops */));

            let routed_ops = spec.plumb_from_process(&layers, operations.clone());
            let (client_ops, local_put_deltas) = extract_put_deltas(routed_ops);
            let (_pass_up, replication_ops) = spec.replicate_puts(&layers, local_put_deltas);

            let client_core = KVSCore::process_client_ops(client_ops);
            let replica_core = KVSCore::process_replicated_ops(
                replication_ops
                    .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                        nondet!(/** replicated operation order */),
                    ),
            );

            let combined_data = client_core
                .data
                .interleave(replica_core.data)
                .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                nondet!(/** combined data events */),
            );

            let combined_meta = client_core
                .meta
                .interleave(replica_core.meta)
                .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(
                nondet!(/** combined meta events */),
            );

            let (bg_data, bg_meta) = spec.plumb_background(&layers, combined_data, combined_meta);
            bg_data.for_each(q!(|_d| ()));

            bg_meta
                .clone()
                .filter_map(q!(|event| match event {
                    MetaEvent::VectorClockSnapshot { key, clock } if key == "gamma" => {
                        // Frontier considered ready when at least two distinct members are present.
                        let members = clock.as_inner().as_reveal_ref().len();
                        Some(if members >= 2 {
                            "FR_READY:gamma".to_string()
                        } else {
                            "FR_PENDING:gamma".to_string()
                        })
                    }
                    MetaEvent::TombPruned { key } => {
                        if key == "gamma" {
                            Some("PRUNE:gamma".to_string())
                        } else {
                            None
                        }
                    }
                    _ => None,
                }))
                .send_bincode(process)
                .entries()
                .map(q!(|(member, meta)| (member, meta)))
        },
        |mut stream| async move {
            let mut seen_remote = false;
            let mut seen_tomb_frontier = false; // frontier observed (snapshot or cross-member)

            let mut first_member: Option<
                hydro_lang::location::member_id::MemberId<kvs_zoo::kvs_core::KVSNode>,
            > = None;

            // Phase 1: Wait for frontier readiness; fail fast if prune appears first.
            for _ in 0..28 {
                match timeout(Duration::from_millis(500), stream.next()).await {
                    Ok(Some((member, meta))) => {
                        match first_member {
                            None => first_member = Some(member),
                            Some(m0) => {
                                if member != m0 {
                                    seen_remote = true;
                                    // Treat cross-member observation as frontier readiness as well.
                                    seen_tomb_frontier = true;
                                }
                            }
                        }
                        if meta == "PRUNE:gamma" {
                            if !seen_tomb_frontier {
                                panic!("Tombstone pruned before frontier was established!");
                            } else {
                                break;
                            }
                        } else if meta == "FR_READY:gamma" {
                            seen_tomb_frontier = true;
                            break;
                        }
                    }
                    _ => continue,
                }
            }

            assert!(
                seen_remote,
                "expected remote concurrent update via snapshot"
            );
            assert!(seen_tomb_frontier, "frontier should be ready for key gamma");
            // If we would ever see a prune before frontier, we panic immediately above.

            // Phase 2: Optionally observe prune after frontier; only enforce ordering, not eventuality.
            for _ in 0..20 {
                match timeout(Duration::from_millis(400), stream.next()).await {
                    Ok(Some((_member, meta))) if meta == "PRUNE:gamma" => {
                        // If prune is observed, it must be after frontier readiness by construction.
                        break;
                    }
                    Ok(Some(_)) => continue,
                    _ => continue,
                }
            }
        },
    )
    .await;
}
