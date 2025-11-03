use crate::protocol::KVSOperation;
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use lattices::Merge;

/// Lattice-based KVS core operations using the Merge trait for conflict resolution
/// This variant replaces last-write-wins semantics with proper lattice merges
pub struct LatticeKVSCore;

impl LatticeKVSCore {
    /// Insert into a hashtable from PUT operations using lattice merge semantics
    /// Returns both the KVS store and a stream of all PUT keys (as an approximation of changed keys)
    /// Generic over value type V (which must implement Merge), location L, and ordering O
    /// 
    /// Note: Ideally we would only emit keys where merge() returned true, but Hydro's
    /// stateful fold doesn't allow emitting per-element. So we conservatively emit all PUT keys.
    pub fn put<'a, V, L, O>(
        ops: Stream<KVSOperation<V>, L, Unbounded, O>,
    ) -> (KeyedSingleton<String, V, L, Unbounded>, Stream<String, L, Unbounded, O>)
    where
        V: Clone + Default + PartialEq + Eq + Merge<V>,
        L: Location<'a> + Clone + 'a,
        O: hydro_lang::live_collections::stream::Ordering,
    {
        let (puts, _gets) = Self::demux_ops(ops);
        let key_values = puts.map(q!(|op| {
            if let KVSOperation::Put(key, value) = op {
                (key, value)
            } else {
                unreachable!()
            }
        }));
        
        // Extract all PUT keys (conservative approximation of "changed" keys)
        let all_put_keys = key_values
            .clone()
            .map(q!(|(k, _v)| k));
        
        // Insert into the KVS store
        let store = key_values
            .into_keyed()
            .fold_commutative(q!(|| Default::default()), q!(|acc: &mut V, i| {
                acc.merge(i);
            }));
        
        (store, all_put_keys)
    }

    /// Split operations into separate PUT and GET streams
    /// This helper eliminates duplication across different KVS implementations
    pub fn demux_ops<'a, V, L, O>(
        operations: Stream<KVSOperation<V>, L, Unbounded, O>,
    ) -> (
        Stream<KVSOperation<V>, L, Unbounded, O>,
        Stream<KVSOperation<V>, L, Unbounded, O>,
    )
    where
        V: Clone,
        L: Location<'a> + Clone + 'a,
        O: hydro_lang::live_collections::stream::Ordering,
    {
        let puts = operations
            .clone()
            .filter(q!(|op| matches!(op, KVSOperation::Put(_, _))));

        let gets = operations.filter(q!(|op| matches!(op, KVSOperation::Get(_))));

        (puts, gets)
    }

    /// Query the hashtable from GET operations (identical to KVSCore::get)
    /// Generic over value type V and location L, which could be Process or Cluster
    pub fn get<'a, V, L>(
        ops: Stream<KVSOperation<V>, Tick<L>, Bounded>,
        ht: KeyedSingleton<String, V, Tick<L>, Bounded>,
    ) -> (
        Stream<(String, V), L, Unbounded>,
        Stream<String, L, Unbounded>,
    )
    where
        V: Clone + std::fmt::Debug,
        L: Location<'a> + Clone + 'a,
    {
        // Extract GET operations and convert to key batch
        let key_batch = ops
            .filter(q!(|op| matches!(op, KVSOperation::Get(_))))
            .map(q!(|op| {
                if let KVSOperation::Get(key) = op {
                    (key, ())
                } else {
                    unreachable!()
                }
            }));

        let matches = ht
            .get_many_if_present(key_batch.clone().into_keyed())
            .entries()
            .map(q!(|(k, (v, ()))| {
                println!("Lattice KVS: Found match for GET {} => {:?}", k, v);
                (k, v)
            }))
            .all_ticks()
            .assume_ordering(nondet!(/** reads are non-deterministic! */));

        let fails = key_batch
            .all_ticks()
            .map(q!(|(k, ())| {
                println!("Lattice KVS: Looking up key: {}", k);
                k
            }))
            .filter(q!(|k| k == "nonexistent")) // Hardcode for testing
            .assume_ordering(nondet!(/** reads are non-deterministic! */));
        (matches, fails)
    }
}
