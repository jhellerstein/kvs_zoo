//! Acker Dispatcher
//!
//! Simple dispatcher that handles two-channel processing:
//! - Operations from local channel (via cluster dispatch) → ACK client
//! - Operations from replication channel (from other members) → NO ACK
//!
//! This dispatcher doesn't know about slots, ordering, or deduplication.
//! It just decides which operations should generate client responses.

use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::dispatch::OpDispatch;
use crate::kvs_core::KVSNode;
use crate::protocol::KVSOperation;

/// Acker dispatcher - handles ACK/no-ACK decision for two-channel processing
#[derive(Clone, Debug, Default)]
pub struct Acker;

impl Acker {
    pub fn new() -> Self {
        Self
    }

    /// Process operations from two channels: local (ACK) and replicated (no ACK).
    ///
    /// Returns two streams:
    /// - Local ops (should generate responses)
    /// - Replicated ops (should NOT generate responses)
    pub fn dispatch_from_two_channels<'a, V>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        local_ops: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        replicated_ops: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
    ) -> (
        Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
    )
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Simply return both streams as-is
        // The caller will process local ops with responses, replicated ops without
        (local_ops, replicated_ops)
    }
}

impl<V> OpDispatch<V> for Acker {
    fn dispatch_from_process<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Forward to member 0
        operations
            .map(q!(|op| (hydro_lang::location::MemberId::from_raw(0u32), op)))
            .into_keyed()
            .demux_bincode(target_cluster)
    }

    fn dispatch_from_cluster<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>,
        _source_cluster: &Cluster<'a, KVSNode>,
        _target_cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // No-op: operations stay within cluster
        operations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acker_creation() {
        let _acker = Acker::new();
        let _acker_default = Acker::default();
    }

    #[test]
    fn test_implements_opdispatch() {
        fn _check<V>(_d: impl OpDispatch<V>) {}
        _check::<String>(Acker::new());
    }
}
