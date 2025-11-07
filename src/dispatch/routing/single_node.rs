//! Single-Node Router Operation Interceptor
//!
//! The SingleNodeRouter is intended for usage over a single node and acts as a
//! no-op router: it does not fan out or load-balance. It simply routes all
//! operations to a single local member. In a single-node cluster this is
//! effectively an identity. In a multi-node cluster this pins all operations
//! to member 0 and should generally be avoided.

use crate::core::KVSNode;
use crate::dispatch::OpIntercept;
use crate::protocol::KVSOperation;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// SingleNodeRouter sends every operation to a single node (member 0).
///
/// This is appropriate for:
/// - Single-node deployments (where the cluster contains just one node)
/// - Development and testing scenarios where you want no routing semantics
///
/// ## Usage
///
/// ```rust
/// use kvs_zoo::dispatch::routing::SingleNodeRouter;
/// use kvs_zoo::dispatch::OpIntercept;
///
/// let router = SingleNodeRouter::new();
/// // Operations will be routed to member 0 (no broadcast or load-balancing)
/// ```
///
/// ## Behavior
///
/// For local development, the "cluster" typically contains just one node,
/// making this effectively an identity router. In multi-node scenarios,
/// all operations will be routed to member 0 (no broadcast, no load-balancing).
#[derive(Clone, Debug, Default)]
pub struct SingleNodeRouter;

impl SingleNodeRouter {
    /// Create a new single-node router
    pub fn new() -> Self {
        Self
    }
}

impl<V> OpIntercept<V> for SingleNodeRouter {
    fn intercept_operations<'a>(
        &self,
        operations: Stream<KVSOperation<V>, Process<'a, ()>, Unbounded>,
        cluster: &Cluster<'a, KVSNode>,
    ) -> Stream<KVSOperation<V>, Cluster<'a, KVSNode>, Unbounded>
    where
        V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // Route all operations to a single local member (member 0)
        // This avoids broadcast and acts as a no-op in single-node clusters.
        operations
            .map(q!(|op| (
                hydro_lang::location::MemberId::from_raw(0u32),
                op
            )))
            .into_keyed()
            .demux_bincode(cluster)
    }
}
