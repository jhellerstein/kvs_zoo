use crate::values::LwwWrapper;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::{Location, NoTick};
use hydro_lang::prelude::*;

/// Last-Writer-Wins KVS implementation
/// 
/// This storage implementation uses last-writer-wins semantics for conflict resolution.
/// It wraps all values with `LwwWrapper` and delegates to the core KVS operations.
pub struct KVSLww;



impl KVSLww {
    /// Insert into a hashtable from PUT operations using last-writer-wins semantics
    /// This delegates to the general KVSCore with LastWriterWins wrapper
    /// Generic over value type V, location L, and ordering O (accepts both ordered and unordered streams)
    pub fn put<'a, V, L, O>(
        put_tuples: Stream<(String, V), L, Unbounded, O>,
    ) -> KeyedSingleton<String, LwwWrapper<V>, L, Unbounded>
    where
        V: Clone
            + Default
            + PartialEq
            + Eq
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + 'static,
        L: Location<'a> + Clone + NoTick + 'a,
        O: hydro_lang::live_collections::stream::Ordering,
    {
        // Wrap values with LwwWrapper for last-writer-wins semantics
        let wrapped_tuples = put_tuples.map(q!(|(key, value)| (key, LwwWrapper::new(value))));
        
        // Delegate to the core KVS implementation
        crate::core::KVSCore::put(wrapped_tuples)
    }

    /// Query the hashtable from GET operations
    /// Works with LwwWrapper and delegates to the general KVSCore
    /// Generic over value type V and location L, which could be Process or Cluster
    pub fn get<'a, V, L>(
        keys: Stream<String, Tick<L>, Bounded>,
        ht: KeyedSingleton<String, LwwWrapper<V>, Tick<L>, Bounded>,
    ) -> Stream<(String, Option<V>), Tick<L>, Bounded, NoOrder>
    where
        V: Clone + std::fmt::Debug,
        L: Location<'a> + Clone + NoTick + 'a,
    {
        // Delegate to the general KVSCore and unwrap the results
        let wrapped_results = crate::core::KVSCore::get(keys, ht);

        wrapped_results.map(q!(|(k, wrapped_v_opt)| {
            (k, wrapped_v_opt.map(|wrapped_v| wrapped_v.into_inner())) // Unwrap the LwwWrapper
        }))
    }
}


