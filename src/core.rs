use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::{Location, NoTick, Tick};
use hydro_lang::{live_collections, prelude::*};
use lattices::Merge;

// Type aliases to reduce complexity warnings
type KVSPutStream<V, L, B, O> = Stream<(String, V), L, B, O>;
type KVSGetStream<L, B, O> = Stream<String, L, B, O>;
type KVSPutGetStreams<V, L, B, O> = (KVSPutStream<V, L, B, O>, KVSGetStream<L, B, O>);

/// Represents an individual KVS node in the cluster
pub struct KVSNode {}

/// General KVS core operations using lattice merge semantics
/// This is the main KVS implementation that works with any value type implementing Merge
pub struct KVSCore;

impl KVSCore {
    /// PUT (key, value) pairs using lattice merge semantics
    /// Generic over value type V (which must implement Merge), location L, and ordering O
    pub fn put<'a, V, L, O>(
        put_tuples: Stream<(String, V), L, Unbounded, O>,
    ) -> KeyedSingleton<String, V, L, Unbounded>
    where
        V: Clone + Default + PartialEq + Eq + Merge<V>,
        L: Location<'a> + Clone + NoTick + 'a,
        O: live_collections::stream::Ordering,
    {
        // Insert into the KVS store - no demuxing needed!
        put_tuples.into_keyed().fold_commutative(
            q!(|| Default::default()),
            q!(|acc, i| {
                Merge::merge(acc, i);
            }),
        )
    }

    /// Query the snapshotted hashtable with a bounded batch of GET keys.
    /// Returns an unordered stream with Some(value) for found keys and None for missing keys
    /// Generic over value type V and location L, which could be Process or Cluster
    pub fn get<'a, V, L>(
        keys: Stream<String, Tick<L>, Bounded>,
        ht: KeyedSingleton<String, V, Tick<L>, Bounded>,
    ) -> Stream<(String, Option<V>), Tick<L>, Bounded, NoOrder>
    where
        V: Clone + std::fmt::Debug,
        L: Location<'a> + 'a + Clone,
    {
        // Convert keys to KeyedSingleton for get_from()
        // Hydro should offer a more efficient "outer join" than get_from()!
        let key_batch = keys
            .map(q!(|k| (k.clone(), k)))
            .into_keyed()
            .reduce(q!(|acc, x| *acc = x));

        key_batch
            .get_from(ht)
            .entries()
            .map(q!(|(key, (_original_key, value))| (key, value)))
    }

    /// Split operations into separate PUT and GET streams
    /// Returns PUT operations as (key, value) tuples and GET operations as keys
    /// This eliminates the need for pattern matching in callers
    pub fn demux_ops<'a, V, L, B, O>(
        operations: Stream<KVSOperation<V>, L, B, O>,
    ) -> KVSPutGetStreams<V, L, B, O>
    where
        V: Clone,
        L: Location<'a> + Clone + 'a,
        B: live_collections::boundedness::Boundedness,
        O: live_collections::stream::Ordering,
    {
        let puts = operations.clone().filter_map(q!(|op| match op {
            KVSOperation::Put(key, value) => Some((key, value)),
            KVSOperation::Get(_) => None,
        }));

        let gets = operations.filter_map(q!(|op| match op {
            KVSOperation::Get(key) => Some(key),
            KVSOperation::Put(_, _) => None,
        }));

        (puts, gets)
    }
}
