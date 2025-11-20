//! Metadata utilities for After-storage stages.
//!
//! This module intentionally contains only the vector-clock frontier builder.
//! All production metadata lives in `kvs_core::events::MetaEvent`.

// Vector frontier (merged per-key VCs) built from background VC snapshots
pub mod vector_frontier;
pub use vector_frontier::{FrontierState, build_frontier, new_frontier_state};
