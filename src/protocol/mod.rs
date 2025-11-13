//! Protocol definitions for KVS systems
//!
//! This module defines message types and handlers for different protocols:
//! - **kvs_ops**: Core KVS operations (Get, Put, Delete)
//! - **envelope**: Generic metadata wrapper for operations
//! - **session**: Session management (Login, Logout, Heartbeat) [Future]
//! - **admin**: Administrative operations (Stats, Health, Config) [Future]

pub mod admin;
pub mod envelope;
pub mod kvs_ops;
pub mod routing;
pub mod session;
pub mod tagged;

// Re-export core KVS types for convenience
pub use envelope::{Envelope, SlottedOperation};
pub use kvs_ops::{KVSOperation, KVSResponse};
pub use routing::{HasSequence, RoutingKey};
pub use tagged::TaggedOperation;
