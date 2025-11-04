//! # Local KVS Architecture Example
//!
//! This example demonstrates the simplest KVS architecture:
//! - **Single process**: No distribution, no networking between KVS nodes
//! - **Last-Writer-Wins**: Deterministic conflict resolution
//! - **No replication**: Data exists on one node only
//! - **No sharding**: All data on the same node
//!
//! **Architecture**: Process-to-cluster communication (even for single node)
//! **Use case**: Development, testing, or scenarios where simplicity > availability

use futures::{SinkExt, StreamExt};
use hydro_lang::prelude::*;
use kvs_zoo::driver::KVSDemo;
use kvs_zoo::lww::KVSLww;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::routers::{KVSRouter, LocalRouter};
use kvs_zoo::run_kvs_demo_impl;

#[derive(Default)]
struct LocalDemo;

impl KVSDemo for LocalDemo {
    type Value = String;
    type Storage = KVSLww;
    type Router = LocalRouter;

    fn cluster_size(&self) -> usize {
        1 // Single node
    }

    fn description(&self) -> &'static str {
        "📋 Architecture: Single process, no networking\n\
         🔒 Consistency: Strong (deterministic)\n\
         🎯 Use case: Development, testing, simple applications"
    }

    fn operations(&self) -> Vec<KVSOperation<Self::Value>> {
        vec![
            KVSOperation::Put("key1".to_string(), "value1".to_string()),
            KVSOperation::Put("key2".to_string(), "value2".to_string()),
            KVSOperation::Get("key1".to_string()),
            KVSOperation::Get("nonexistent".to_string()),
            KVSOperation::Put("key1".to_string(), "updated_value1".to_string()),
            KVSOperation::Get("key1".to_string()),
        ]
    }

    fn name(&self) -> &'static str {
        "Local KVS"
    }

    fn create_router<'a>(
        &self,
        _flow: &hydro_lang::compile::builder::FlowBuilder<'a>,
    ) -> Self::Router {
        LocalRouter
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_kvs_demo_impl!(LocalDemo)
}
