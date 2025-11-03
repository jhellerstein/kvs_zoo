/// External Communication Proxy Utilities
/// 
/// This module provides utilities for creating thin proxy processes that enable
/// communication between Hydro clusters and external clients. Since clusters
/// cannot directly communicate with external processes in the current Hydro
/// implementation, these proxies serve as lightweight intermediaries.

use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use serde::{Serialize, de::DeserializeOwned};

/// A thin proxy process that forwards operations from external clients to a cluster
/// and returns results back to the external clients.
/// 
/// This is a minimal intermediary that performs no business logic - it simply
/// routes messages between external clients and cluster processes.
pub struct ExternalProxy;

impl ExternalProxy {
    /// Create a bidirectional proxy between external clients and a cluster.
    /// 
    /// This sets up:
    /// 1. Input port for receiving operations from external clients
    /// 2. Forwarding operations to the cluster (round-robin distribution)
    /// 3. Collecting results from the cluster
    /// 4. Output ports for sending results back to external clients
    /// 
    /// # Type Parameters
    /// - `Op`: The operation type (e.g., KVSOperation)
    /// - `Success`: The success result type (e.g., (String, Value))
    /// - `Failure`: The failure result type (e.g., String)
    /// - `L`: The cluster location type
    /// 
    /// # Returns
    /// A tuple of (input_port, success_output_port, failure_output_port)
    pub fn create_bidi_proxy<'a, Op, Success, Failure, L>(
        proxy_process: &Process<'a, ()>,
        _cluster: &Cluster<'a, L>,
        external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<Op>,
        ExternalBincodeStream<Success>,
        ExternalBincodeStream<Failure>,
    )
    where
        Op: Clone + Serialize + DeserializeOwned + 'static,
        Success: Clone + Serialize + DeserializeOwned + 'static,
        Failure: Clone + Serialize + DeserializeOwned + 'static,
        L: Location<'a> + Clone + 'static,
    {
        // Proxy receives operations from external clients
        let (input_port, _operations) = proxy_process.source_external_bincode(external);

        // This method signature indicates that the cluster should return two streams:
        // - A stream of successful results of type Success
        // - A stream of failure results of type Failure
        // The actual cluster processing logic is implemented by the caller.
        
        // For now, we'll create placeholder streams that the caller can replace
        // with their actual cluster processing logic
        let success_results = proxy_process.source_iter(q!(std::iter::empty::<Success>()));
        let failure_results = proxy_process.source_iter(q!(std::iter::empty::<Failure>()));

        // Send results back to external clients
        let success_output_port = success_results
            .assume_ordering(nondet!(/** results from cluster are non-deterministic */))
            .send_bincode_external(external);
        let failure_output_port = failure_results
            .assume_ordering(nondet!(/** failures from cluster are non-deterministic */))
            .send_bincode_external(external);

        (input_port, success_output_port, failure_output_port)
    }

    /// Create a simple proxy that forwards a single stream type between external and cluster.
    /// 
    /// This is a simpler version for cases where you only need to handle one result type.
    pub fn create_simple_proxy<'a, Op, Result, L>(
        proxy_process: &Process<'a, ()>,
        cluster: &Cluster<'a, L>,
        external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<Op>,
        ExternalBincodeStream<Result>,
    )
    where
        Op: Clone + Serialize + DeserializeOwned + 'static,
        Result: Clone + Serialize + DeserializeOwned + 'static,
        L: Location<'a> + Clone + 'static,
    {
        // Proxy receives operations from external clients
        let (input_port, operations) = proxy_process.source_external_bincode(external);

        // Forward operations to cluster for processing (round-robin to any member)
        let _cluster_operations = operations.round_robin_bincode(cluster, nondet!(/** distribute operations */));

        // Placeholder for results - the caller will replace this with actual cluster processing
        let results = proxy_process.source_iter(q!(std::iter::empty::<Result>()));

        // Send results back to external clients
        let output_port = results
            .assume_ordering(nondet!(/** results from cluster are non-deterministic */))
            .send_bincode_external(external);

        (input_port, output_port)
    }

    /// Create a specialized proxy for KVS operations
    /// 
    /// This is a convenience method specifically designed for Key-Value Store operations
    /// that handles the common pattern of operations with success/failure results.
    pub fn create_kvs_proxy<'a, V, L>(
        proxy_process: &Process<'a, ()>,
        cluster: &Cluster<'a, L>,
        external: &External<'a, ()>,
    ) -> (
        ExternalBincodeSink<crate::protocol::KVSOperation<V>>,
        ExternalBincodeStream<(String, V)>,
        ExternalBincodeStream<String>,
    )
    where
        V: Clone + Serialize + DeserializeOwned + 'static,
        L: Location<'a> + Clone + 'static,
    {
        Self::create_bidi_proxy::<crate::protocol::KVSOperation<V>, (String, V), String, L>(
            proxy_process,
            cluster,
            external,
        )
    }
}