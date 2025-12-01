//! Leaf-level after hooks (after-storage, native)

use hydro_lang::prelude::*;

use crate::after_storage::{
    AfterResponses, ClusterCommunication, LeafCompatible, ReplicationStrategy,
};
use crate::kvs_core::KVSNode;
use serde::{Deserialize, Serialize};

/// Trait for leaf after-hooks that decide how to emit responses locally.
pub trait LeafAfterHook {
    fn respond<'a>(
        &self,
        leaf_cluster: &Cluster<'a, KVSNode>,
        tagged_responses: Stream<(bool, String), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<
        String,
        Cluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >;
}

/// Simple responder that only forwards responses for original (non-replica) ops.
#[derive(Clone, Debug, Default)]
pub struct Responder;

impl Responder {
    pub fn new() -> Self {
        Self
    }
}

impl LeafAfterHook for Responder {
    fn respond<'a>(
        &self,
        _leaf_cluster: &Cluster<'a, KVSNode>,
        tagged_responses: Stream<(bool, String), Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<
        String,
        Cluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    > {
        tagged_responses
            .filter_map(q!(|(is_replica, resp)| if !is_replica {
                Some(resp)
            } else {
                None
            }))
            .weakest_ordering()
    }
}

/// Responder also customizes the upward response pass: already handled filtering
/// in `respond`, so we just pass through here (explicit override for clarity).
impl AfterResponses for Responder {
    fn after_responses<'a>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
    ) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
        responses
    }
}

// Allow Responder to serve as a leaf "maintenance" component by also implementing
// the replication trait as a no-op. This satisfies existing generic bounds while
// we evolve the separation between replication and after-hooks.
impl<K, V> ReplicationStrategy<K, V> for Responder
where
    K: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    fn is_active() -> bool {
        false
    }

    fn replicate_data<'a, O>(
        &self,
        _cluster: &Cluster<'a, KVSNode>,
        local_data: Stream<(K, V), Cluster<'a, KVSNode>, Unbounded, O>,
    ) -> Stream<
        (K, V),
        Cluster<'a, KVSNode>,
        Unbounded,
        hydro_lang::live_collections::stream::NoOrder,
    >
    where
        O: hydro_lang::live_collections::stream::Ordering,
    {
        // No replication at leaf - just relax ordering to match trait signature
        local_data.weakest_ordering()
    }
}

impl ClusterCommunication for Responder {
    fn requires_cluster_scope() -> bool {
        false
    }
}

impl LeafCompatible for Responder {}
