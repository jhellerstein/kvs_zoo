use futures::StreamExt;
use hydro_lang::prelude::*;
use kvs_zoo::background::TombIndexBackground;
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::kvs_layer::{KVSCluster, KVSClusters, KVSSpec};
use kvs_zoo::values::LwwWrapper;
use kvs_zoo::{BackgroundPlumb, MetaEvent};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delete_emits_tomb_meta_event() {
    hydro_lang::test_util::stream_transform_test(
        |process| {
            let ops = process
                .source_iter(q!(vec![
                    kvs_zoo::protocol::Envelope::new(
                        true,
                        kvs_zoo::protocol::KVSOperation::Put(
                            "alpha".to_string(),
                            kvs_zoo::values::LwwWrapper::new("one".to_string()),
                            Some(1),
                        ),
                    ),
                    kvs_zoo::protocol::Envelope::new(
                        true,
                        kvs_zoo::protocol::KVSOperation::Delete("alpha".to_string(), Some(1)),
                    ),
                ]))
                .assume_ordering(nondet!(/** deterministic demo ops */));

            let kvs_zoo::kvs_core::CoreOutput { data, meta, .. } = KVSCore::process(ops);
            data.for_each(q!(|_data| ()));
            meta
        },
        |mut stream| async move {
            let event = stream
                .next()
                .await
                .expect("expected tomb meta event from delete");
            assert_eq!(
                event,
                MetaEvent::Tomb {
                    key: "alpha".into()
                }
            );
            let maybe_extra = timeout(Duration::from_millis(50), stream.next()).await;
            if let Ok(Some(extra)) = maybe_extra {
                panic!("unexpected meta event after tomb: {:?}", extra);
            }
        },
    )
    .await;
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn background_plumb_routes_meta_events() {
    #[derive(Clone)]
    struct TestLayer;

    hydro_lang::test_util::multi_location_test(
        move |flow, process| {
            let mut spec =
                KVSCluster::<TestLayer, (), (), (), TombIndexBackground>::new_with_background(
                    (),
                    (),
                    (),
                    TombIndexBackground::new().with_summaries(true),
                );

            let mut layers = KVSClusters::new();
            let cluster_entry =
                <KVSCluster<TestLayer, (), (), (), TombIndexBackground> as KVSSpec<
                    LwwWrapper<String>,
                >>::create_clusters(&spec, flow, &mut layers);

            let data_stream = cluster_entry
                .source_iter(q!(
                    Vec::<(String, kvs_zoo::values::LwwWrapper<String>,)>::new()
                ))
                .map(q!(|(key, value)| kvs_zoo::DataEvent::Put { key, value }))
                .assume_ordering(nondet!(/** no foreground data */));

            let meta_stream = cluster_entry
                .source_iter(q!(vec!["alpha".to_string()]))
                .map(q!(|key: String| kvs_zoo::MetaEvent::Tomb { key }))
                .assume_ordering(nondet!(/** single tomb meta event */));

            let (bg_data, bg_meta) = spec.plumb_background(&layers, data_stream, meta_stream);
            bg_data.for_each(q!(|_data| ()));

            bg_meta
                .send_bincode(process)
                .entries()
                .map(q!(|(_member, event)| event))
        },
        |mut stream| async move {
            let mut tomb_observed = false;
            let mut summary_observed = false;
            let mut digest_observed = false;

            for _ in 0..16 {
                let maybe_event = timeout(Duration::from_millis(300), stream.next()).await;
                match maybe_event {
                    Ok(Some(MetaEvent::Tomb { ref key })) if key == "alpha" => {
                        tomb_observed = true;
                    }
                    Ok(Some(MetaEvent::TombSummary {
                        total_tombs,
                        ref last_tomb_key,
                    })) if total_tombs == 1 && last_tomb_key.as_deref() == Some("alpha") => {
                        summary_observed = true;
                    }
                    Ok(Some(MetaEvent::CompactionDigest { .. })) => {
                        digest_observed = true;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }

                if tomb_observed && summary_observed && digest_observed {
                    break;
                }
            }

            assert!(tomb_observed, "background should surface tomb event");
            assert!(summary_observed, "background should emit tomb summary");
            assert!(digest_observed, "background should emit compaction digest");
        },
    )
    .await;
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tomb_index_background_emits_summary() {
    hydro_lang::test_util::multi_location_test(
        |flow, process| {
            let cluster = flow.cluster::<kvs_zoo::kvs_core::KVSNode>();
            let ops = cluster
                .source_iter(q!(vec![kvs_zoo::protocol::Envelope::new(
                    true,
                    kvs_zoo::protocol::KVSOperation::<kvs_zoo::values::LwwWrapper<String>>::Delete(
                        "alpha".to_string(),
                        Some(1),
                    ),
                )]))
                .assume_ordering(nondet!(/** single delete */));

            let kvs_zoo::kvs_core::CoreOutput { data, meta, .. } = KVSCore::process(ops);
            let background = TombIndexBackground::new().with_summaries(true);
            let meta = background.transform_meta_stream(meta);
            data.for_each(q!(|_data| ()));
            meta.send_bincode(process)
                .entries()
                .map(q!(|(_member, event)| event))
        },
        |mut stream| async move {
            let mut seen_tomb = false;
            let mut seen_summary = false;

            while let Some(event) = stream.next().await {
                match event {
                    MetaEvent::Tomb { ref key } if key == "alpha" => {
                        seen_tomb = true;
                    }
                    MetaEvent::TombSummary {
                        total_tombs,
                        ref last_tomb_key,
                    } if total_tombs == 1 && last_tomb_key.as_deref() == Some("alpha") => {
                        seen_summary = true;
                    }
                    _ => {}
                }

                if seen_tomb && seen_summary {
                    break;
                }
            }

            assert!(seen_tomb, "original tomb event should be preserved");
            assert!(
                seen_summary,
                "summary event should reflect accumulated tombs"
            );
        },
    )
    .await;
}
