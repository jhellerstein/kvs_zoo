//! Node-level maintenance strategies
//!
//! This module contains maintenance strategies that operate independently
//! on individual nodes.

pub mod tombstone_cleanup;

pub use tombstone_cleanup::{TombstoneCleanup, TombstoneCleanupConfig};
