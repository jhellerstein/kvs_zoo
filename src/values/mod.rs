//! # Value Wrappers and Consistency Semantics
//!
//! This module provides different value wrapper types that implement various
//! consistency semantics for distributed key-value stores. Each wrapper
//! implements the `lattices::Merge` trait to define how conflicting values
//! should be resolved.
//!
//! ## Consistency Models
//!
//! - **Last-Writer-Wins (LWW)** - Simple overwrite semantics
//! - **Causal Consistency** - Vector clock based causal ordering
//! - **Set Union** - Merge conflicting values into sets
//!
//! ## Value Wrappers
//!
//! - [`LwwWrapper<T>`] - Last-writer-wins semantics
//! - [`CausalWrapper<T>`] - Causal consistency with vector clocks
//! - [`VCWrapper`] - Vector clock for causal ordering
//!
//! ## Usage Patterns
//!
//! ```rust
//! use kvs_zoo::values::{LwwWrapper, CausalWrapper, VCWrapper};
//! use lattices::Merge;
//!
//! // Last-writer-wins: simple overwrite
//! let mut lww1 = LwwWrapper::new("value1".to_string());
//! let lww2 = LwwWrapper::new("value2".to_string());
//! lww1.merge(lww2); // lww1 now contains "value2"
//!
//! // Causal consistency: preserves causal relationships
//! let mut causal1 = CausalWrapper::new(VCWrapper::new(), "value1".to_string());
//! let causal2 = CausalWrapper::new(VCWrapper::new(), "value2".to_string());
//! causal1.merge(causal2); // Merges based on vector clock dominance
//! ```
//!
//! ## Architecture Integration
//!
//! These value wrappers work seamlessly with the KVS storage implementations:
//!
//! ```rust
//! use kvs_zoo::values::{LwwWrapper, CausalWrapper};
//! use kvs_zoo::server::LocalKVSServer;
//!
//! // Use with any storage type
//! type LwwKVS = LocalKVSServer<LwwWrapper<String>>;
//! // Note: For more complex examples, see the individual module documentation
//! ```

pub mod causal;
pub mod lww;
pub mod vector_clock;

// Re-export main types for convenience
pub use causal::{CausalString, CausalWrapper};
pub use lww::LwwWrapper;
pub use vector_clock::VCWrapper;

// Re-export lattice traits for convenience
pub use lattices::Merge;
