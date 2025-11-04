//! # Replicated KVS Architecture Example
//!
//! This example demonstrates a replicated KVS architecture:
//! - **Full replication**: All data is replicated across all nodes
//! - **Broadcast operations**: All operations are sent to all replicas
//! - **Strong consistency**: All replicas process operations in the same order
//! - **Fault tolerance**: Can tolerate node failures (as long as one replica survives)
//!
//! **Architecture**: Broadcast router with full replication
//! **Trade-offs**: Strong consistency and fault tolerance, but limited scalability
//! **Use case**: Critical data that needs high availability and consistency

use futures::{SinkExt, StreamExt};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use kvs_zoo::driver::KVSDemo;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::replicated::KVSReplicated;
use kvs_zoo::routers::{BroadcastReplication, KVSRouter, RoundRobinRouter};
use kvs_zoo::run_kvs_demo_impl;
use kvs_zoo::values::{CausalString, generate_causal_operations};

/// Type alias for replicated KVS with broadcast replication
type ReplicatedKVS = KVSReplicated<BroadcastReplication<CausalString>>;

struct ReplicatedDemo;

impl Default for ReplicatedDemo {
    fn default() -> Self {
        ReplicatedDemo
    }
}

impl KVSDemo for ReplicatedDemo {
    type Value = CausalString;
    type Storage = ReplicatedKVS;
    type Router = RoundRobinRouter;

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        RoundRobinRouter // Distributes operations across replicas
    }

    fn cluster_size(&self) -> usize {
        3 // 3 replicas
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Broadcast router with full replication\n\
         🔄 Consistency: Causal consistency with vector clocks\n\
         🛡️ Fault tolerance: Can survive node failures\n\
         🎯 Use case: Critical data requiring high availability"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        generate_causal_operations()
    }

    fn name(&self) -> &'static str {
        "Replicated KVS"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo_impl!(ReplicatedDemo)
}
