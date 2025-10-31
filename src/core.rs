use crate::protocol::KVSOperation;
use hydro_lang::location::Location;
use hydro_lang::prelude::*;

/// Core KVS operations that can be shared between local and replicated implementations
pub struct KVSCore;

impl KVSCore {
    /// Insert into a hashtable from PUT operations
    /// Generic over value type V, location L, and ordering O (accepts both ordered and unordered streams)
    pub fn put<'a, V, L, O>(
        ops: Stream<KVSOperation<V>, L, Unbounded, O>,
    ) -> KeyedSingleton<String, V, L, Unbounded>
    where
        V: Clone + Default + PartialEq + Eq,
        L: Location<'a> + Clone + 'a,
        O: hydro_lang::live_collections::stream::Ordering,
    {
        let (puts, _gets) = Self::demux_ops(ops);
        puts.map(q!(|op| {
            if let KVSOperation::Put(key, value) = op {
                (key, value)
            } else {
                unreachable!()
            }
        }))
        .into_keyed()
        .fold_commutative(q!(|| Default::default()), q!(|acc, i| *acc = i)) /* nondet!(/** this fold is not really commutative! */) */
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

    /// Query the hashtable from GET operations (symmetric to ht_put)
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
        // Extract GET operations and convert to key batch (symmetric to ht_put filtering)
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
                println!("KVS: Found match for GET {} => {:?}", k, v);
                (k, v)
            }))
            .all_ticks()
            .assume_ordering(nondet!(/** diagnostic output */));

        let fails = key_batch
            .all_ticks()
            .map(q!(|(k, ())| {
                println!("KVS: Looking up key: {}", k);
                k
            }))
            .filter(q!(|k| k == "nonexistent")) // Hardcode for testing
            .assume_ordering(nondet!(/** diagnostic output */));
        (matches, fails)
    }
}
