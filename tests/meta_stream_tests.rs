use futures::StreamExt;
use hydro_lang::prelude::*;
use kvs_zoo::MetaEvent;
use kvs_zoo::background::TombIndexBackground;
use kvs_zoo::kvs_core::KVSCore;
use kvs_zoo::protocol::{Envelope, KVSOperation};
use kvs_zoo::values::LwwWrapper;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delete_emits_tomb_meta_event() {
    hydro_lang::test_util::stream_transform_test(
        |process| {
            let ops = process
                .source_iter(q!(vec![
                    Envelope::new(
                        true,
                        KVSOperation::Put("alpha".to_string(), LwwWrapper::new("one".to_string()),)
                    ),
                    Envelope::new(true, KVSOperation::Delete("alpha".to_string())),
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
            assert!(stream.next().await.is_none());
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tomb_index_background_emits_summary() {
    hydro_lang::test_util::multi_location_test(
        |flow, process| {
            let cluster = flow.cluster::<kvs_zoo::kvs_core::KVSNode>();
            let ops = cluster
                .source_iter(q!(vec![Envelope::new(
                    true,
                    KVSOperation::<LwwWrapper<String>>::Delete("alpha".to_string()),
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
