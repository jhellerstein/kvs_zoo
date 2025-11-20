/// Tests for Vector Clock implementation
use kvs_zoo::background::VectorClockSnapshot;
use kvs_zoo::values::vector_clock::VCWrapper;
use lattices::Merge;

#[test]
fn test_new_vector_clock() {
    let vc = VCWrapper::new();
    assert_eq!(vc.get("node1"), None);
}

#[test]
fn test_bump() {
    let mut vc = VCWrapper::new();
    vc.bump("node1".to_string());
    assert_eq!(vc.get("node1"), Some(1));

    vc.bump("node1".to_string());
    assert_eq!(vc.get("node1"), Some(2));

    vc.bump("node2".to_string());
    assert_eq!(vc.get("node2"), Some(1));
}

#[test]
fn test_merge() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());
    vc2.bump("node1".to_string());

    vc1.merge(vc2);
    assert_eq!(vc1.get("node1"), Some(2)); // max(2, 1) = 2
    assert_eq!(vc1.get("node2"), Some(1));
}

#[test]
fn test_happened_before() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = vc1.clone();
    vc2.bump("node1".to_string());

    assert!(vc1.happened_before(&vc2));
    assert!(!vc2.happened_before(&vc1));
}

#[test]
fn test_concurrent() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    assert!(vc1.is_concurrent(&vc2));
    assert!(vc2.is_concurrent(&vc1));
}

#[test]
fn test_merge_is_idempotent() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let vc2 = vc1.clone();
    let vc1_before = vc1.clone();

    vc1.merge(vc2);
    assert_eq!(vc1, vc1_before);
}

#[test]
fn test_merge_is_commutative() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    let mut result1 = vc1.clone();
    result1.merge(vc2.clone());

    let mut result2 = vc2.clone();
    result2.merge(vc1.clone());

    assert_eq!(result1, result2);
}

#[test]
fn test_merge_is_associative() {
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let mut vc2 = VCWrapper::new();
    vc2.bump("node2".to_string());

    let mut vc3 = VCWrapper::new();
    vc3.bump("node3".to_string());

    // (vc1 ∨ vc2) ∨ vc3
    let mut result1 = vc1.clone();
    result1.merge(vc2.clone());
    result1.merge(vc3.clone());

    // vc1 ∨ (vc2 ∨ vc3)
    let mut temp = vc2.clone();
    temp.merge(vc3.clone());
    let mut result2 = vc1.clone();
    result2.merge(temp);

    assert_eq!(result1, result2);
}

#[test]
fn test_missing_entries_are_zero() {
    // Test that missing entries are semantically equivalent to 0
    let mut vc1 = VCWrapper::new();
    vc1.bump("node1".to_string());

    let vc2 = VCWrapper::new(); // Empty, all entries implicitly 0

    // vc2 has 0 for all nodes, so it should have happened before vc1
    assert!(vc2.happened_before(&vc1));
    assert!(!vc1.happened_before(&vc2));

    // Getting a non-existent node should return None (but semantically it's 0)
    assert_eq!(vc1.get("nonexistent"), None);
    assert_eq!(vc2.get("node1"), None);
}

#[test]
fn vector_clock_snapshot_roundtrip() {
    let mut clock = VCWrapper::new();
    clock.bump("node1".to_string());

    let snapshot = VectorClockSnapshot {
        key: "user:1".to_string(),
        clock: clock.clone(),
    };

    let bytes = serde_json::to_vec(&snapshot).expect("serialize vector clock snapshot");
    let decoded: VectorClockSnapshot =
        serde_json::from_slice(&bytes).expect("deserialize vector clock snapshot");

    assert_eq!(decoded, snapshot);
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn vector_clock_background_tracks_replication() {
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
                BroadcastReplication::<CausalString>::new(),
                (),
                VectorClockBackground::new()
                    .with_logging(false)
                    .with_digests(false),
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

            let operations = process
                .source_iter(q!(vec![kvs_zoo::protocol::KVSOperation::Put(
                    "alpha".to_string(),
                    {
                        let mut vc = kvs_zoo::values::VCWrapper::new();
                        vc.bump("client".to_string());
                        kvs_zoo::values::CausalString::new(vc, "payload".to_string())
                    },
                )]))
                .assume_ordering(nondet!(/** single client operation */));

            let routed_ops = spec.plumb_from_process(&layers, operations);
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
            bg_data.for_each(q!(|_data| ()));

            bg_meta
                .filter_map(q!(|event| match event {
                    MetaEvent::VectorClock { key, member, clock } => Some((key, member, clock)),
                    _ => None,
                }))
                .send_bincode(process)
                .entries()
                .map(q!(|(_member, tuple)| tuple))
        },
        |mut stream| async move {
            use std::collections::HashSet;

            let mut observed_members: HashSet<u32> = HashSet::new();

            for _ in 0..8 {
                match timeout(Duration::from_millis(800), stream.next()).await {
                    Ok(Some((key, member, clock))) => {
                        assert_eq!(key, "alpha");
                        let member_key = member.to_string();
                        assert!(matches!(clock.get(&member_key), Some(c) if c >= 1));
                        observed_members.insert(member);

                        if observed_members.len() >= 2 {
                            break;
                        }
                    }
                    _ => break,
                }
            }

            assert!(observed_members.contains(&0), "local member update missing");
            assert!(
                observed_members.iter().any(|member| *member != 0),
                "expected replication to surface a remote member update"
            );
        },
    )
    .await;
}
