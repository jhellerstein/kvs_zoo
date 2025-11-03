/// Common KVS type aliases and variant implementations
///
/// This module provides convenient type aliases for commonly used KVS configurations.

use crate::local::KVSServer;
use crate::vector_clock::VectorClock;
use lattices::DomPair;

/// Type alias for String-based KVS server (most common case)
///
/// This is a KVS that maps String keys to String values, which is the most
/// common use case for testing and simple applications.
pub type StringKVSServer = KVSServer<String>;

/// Causally-consistent KVS using vector clocks
///
/// This KVS stores values as a dominating pair (DomPair) of:
/// - A vector clock (VectorClock) tracking causal dependencies
/// - A payload of generic type V (which must implement Merge)
///
/// When conflicts occur, the DomPair lattice will:
/// - If one vector clock dominates (is causally after) the other, keep that value
/// - If vector clocks are concurrent, merge both the clocks and the payloads
///
/// This provides causal consistency where causally-related updates are properly ordered,
/// and concurrent updates are merged using the payload's lattice semantics.
///
/// # Type Requirements
/// The value type `V` must implement:
/// - `Merge<V>` - for merging concurrent updates
/// - `Clone` - required by KVSServer
/// - `Default` - required by KVSServer
/// - `PartialEq + Eq` - required by KVSServer
/// - Serialization traits for network transmission
///
/// # Example
/// ```ignore
/// use ide_test::kvs_variants::CausalKVS;
/// use lattices::set_union::SetUnionHashSet;
/// use lattices::Merge;
///
/// // A causally-consistent set where concurrent adds are unioned
/// let kvs = CausalKVS::<SetUnionHashSet<String>>::default();
/// ```
pub struct CausalKVS<V>
where
    V: lattices::Merge<V>,
{
    _inner: KVSServer<DomPair<VectorClock, V>>,
}

impl<V> Default for CausalKVS<V>
where
    V: lattices::Merge<V>,
{
    fn default() -> Self {
        Self {
            _inner: KVSServer::default(),
        }
    }
}

// Note: To use CausalKVS in practice, you would typically use LatticeKVSCore
// for the core operations, since DomPair<VectorClock, V> is a lattice type
// and benefits from proper lattice merge semantics rather than last-write-wins.
