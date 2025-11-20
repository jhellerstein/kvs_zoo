//! Metadata utilities for After-storage stages.
//!
//! This module intentionally contains only the vector-clock frontier builder.
//! All production metadata lives in `kvs_core::events::MetaEvent`.
//! The previous `MetaMessage`/handler compatibility layer has been removed
//! to avoid confusion and keep the surface minimal while the project is young.

// Vector frontier (merged per-key VCs) built from background VC snapshots
pub mod vector_frontier;
pub use vector_frontier::{FrontierState, build_frontier, new_frontier_state};
