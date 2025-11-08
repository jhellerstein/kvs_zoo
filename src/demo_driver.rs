//! Shared demo driver utilities for examples.
//! Each example chooses architecture setup; this module standardizes operation scripts
//! and the send/receive loop.

use crate::protocol::KVSOperation;
use crate::values::{CausalString, LwwWrapper, VCWrapper};
use futures::{SinkExt, StreamExt};

/// Generic loop to execute a vector of operations against a connected KVS.
/// Prints each response as it arrives.
pub async fn run_ops<V, S, T, E>(
    mut out: S,
    mut input: T,
    ops: Vec<KVSOperation<V>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: futures::Stream + Unpin,
    S::Item: std::fmt::Display,
    T: futures::Sink<KVSOperation<V>, Error = E> + Unpin,
    E: std::error::Error + 'static,
{
    for op in ops {
        input.send(op).await?;
        if let Some(resp) = out.next().await {
            println!("→ {}", resp);
        }
    }
    Ok(())
}

/// Helper to build a causal value with a single vector clock bump.
pub fn causal(node: &str, v: &str) -> CausalString {
    let mut vc = VCWrapper::new();
    vc.bump(node.to_string());
    CausalString::new(vc, v.to_string())
}

// --- Operation script builders per variant ---

pub fn ops_local() -> Vec<KVSOperation<LwwWrapper<String>>> {
    vec![
        KVSOperation::Put("alpha".into(), LwwWrapper::new("one".into())),
        KVSOperation::Get("alpha".into()),
        KVSOperation::Put("alpha".into(), LwwWrapper::new("two".into())),
        KVSOperation::Get("alpha".into()),
        KVSOperation::Get("missing".into()),
    ]
}

pub fn ops_replicated_gossip() -> Vec<KVSOperation<CausalString>> {
    vec![
        KVSOperation::Put("doc".into(), causal("a", "x")),
        KVSOperation::Put("doc".into(), causal("b", "y")),
        KVSOperation::Get("doc".into()),
    ]
}

pub fn ops_replicated_broadcast() -> Vec<KVSOperation<CausalString>> {
    vec![
        KVSOperation::Put("broadcast".into(), causal("a", "x")),
        KVSOperation::Put("broadcast".into(), causal("b", "y")),
        KVSOperation::Get("broadcast".into()),
    ]
}

pub fn ops_sharded_local() -> Vec<KVSOperation<LwwWrapper<String>>> {
    vec![
        KVSOperation::Put("user:1".into(), LwwWrapper::new("alice".into())),
        KVSOperation::Put("user:2".into(), LwwWrapper::new("bob".into())),
        KVSOperation::Get("user:1".into()),
        KVSOperation::Get("user:2".into()),
    ]
}

pub fn ops_sharded_replicated_broadcast() -> Vec<KVSOperation<CausalString>> {
    vec![
        KVSOperation::Put("user:alice".into(), causal("a", "x")),
        KVSOperation::Put("user:bob".into(), causal("b", "y")),
        KVSOperation::Get("user:alice".into()),
        KVSOperation::Get("user:bob".into()),
    ]
}

pub fn ops_linearizable() -> Vec<KVSOperation<LwwWrapper<String>>> {
    vec![
        KVSOperation::Put("acct".into(), LwwWrapper::new("100".into())),
        KVSOperation::Get("acct".into()),
        KVSOperation::Put("acct".into(), LwwWrapper::new("75".into())),
        KVSOperation::Get("acct".into()),
    ]
}
