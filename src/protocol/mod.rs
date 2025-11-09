//! Protocol definitions for KVS systems
//!
//! This module defines message types and handlers for different protocols:
//! - **kvs_ops**: Core KVS operations (Get, Put, Delete)
//! - **session**: Session management (Login, Logout, Heartbeat) [Future]
//! - **admin**: Administrative operations (Stats, Health, Config) [Future]

pub mod admin;
pub mod kvs_ops;
pub mod session;

// Re-export core KVS types for convenience
pub use kvs_ops::{KVSOperation, KVSResponse};
