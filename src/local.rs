use std::collections::HashMap;

use crate::core::KVSCore;
use crate::protocol::KVSOperation;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::Location;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;

/// Type alias for a key-value store that maps String keys to generic values
pub type LocalKVS<V> = HashMap<String, V>;

/// Represents a KVS node in the gossip cluster
pub struct KVSNode {}



/// KVS server implementation using HashMap directly
pub struct KVSServer<V> {
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Default for KVSServer<V> {
    fn default() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> KVSServer<V>
where
    V: Clone
        + std::fmt::Display
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
        + PartialEq
        + Eq
        + Default,
{
    /// A KVS server that receives operations from external clients
    ///
    /// This demonstrates using Hydro's External interface to receive operations
    /// from clients running outside the dataflow graph.
    pub fn run_local_kvs<'a>(
        server_process: &Process<'a>,
        client_external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<KVSOperation<V>>,
        ExternalBincodeStream<(String, V), NoOrder>,
        ExternalBincodeStream<String, NoOrder>,
    ) {
        // Network sources from clients
        let (input_port, operations) = server_process.source_external_bincode(client_external);

        // puts are streaming and eventually consistent, but
        // we will need ticks for batching our gets
        let gets_ticker = &server_process.tick();

        // Pass the puts and gets to KVSCore
        let ht = KVSCore::put(operations.clone());

        let (get_results, get_fails) = KVSCore::get(
            operations
                .clone()
                .batch(gets_ticker, nondet!(/** gets are non-deterministic */)),
            ht.snapshot(gets_ticker, nondet!(/** gets are non-deterministic! */)),
        );

        // Send results/fails to client. We downcast to an Unordered stream here
        // for uniformity with the distributed cases, where clients receive interleaved responses from many servers
        let get_results_port = get_results.assume_ordering::<NoOrder>(nondet!(/** */)).send_bincode_external(client_external);
        let get_fails_port = get_fails.assume_ordering::<NoOrder>(nondet!(/** */)).send_bincode_external(client_external);

        // Return the input port
        (input_port, get_results_port, get_fails_port)
    }
}

// Re-export StringKVSServer from kvs_variants for backward compatibility
pub use crate::kvs_variants::StringKVSServer;

