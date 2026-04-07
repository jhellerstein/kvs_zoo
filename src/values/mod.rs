//! # Value Types and Consistency Semantics
//!
//! This module provides value wrapper types for distributed key-value stores.
//!
//! ## Two Storage Paths
//!
//! The KVS core supports two storage strategies:
//!
//! - **Lattice merge**: Values implement [`LatticeValue`] (which requires
//!   `lattices::Merge`). Storage uses a commutative, idempotent fold —
//!   replicas converge without ordering. Use [`CausalWrapper`] for this path.
//!
//! - **Overwrite**: Plain Rust types (e.g. `String`). Storage uses last-writer-wins
//!   assignment. Deterministic only when the architecture provides ordering
//!   (single-node, Paxos-sequenced, etc.).
//!
//! ## Value Types
//!
//! - [`CausalWrapper<T>`] - Causal consistency with vector clocks (lattice)
//! - [`VCWrapper`] - Vector clock for causal ordering
//! - Plain `String`, `i64`, etc. - Overwrite semantics
//!
//! ## Example
//!
//! ```rust
//! use kvs_zoo::values::{CausalWrapper, VCWrapper};
//! use lattices::Merge;
//!
//! // Causal consistency: preserves causal relationships (lattice)
//! let mut causal1 = CausalWrapper::new(VCWrapper::new(), "value1".to_string());
//! let causal2 = CausalWrapper::new(VCWrapper::new(), "value2".to_string());
//! causal1.merge(causal2); // Merges based on vector clock dominance
//!
//! // Overwrite: just use a plain type — no wrapper needed
//! let _v: String = "hello".to_string();
//! ```

pub mod causal;
pub mod vector_clock;

// Re-export main types for convenience
pub use causal::{CausalString, CausalWrapper};
pub use vector_clock::VCWrapper;

// Re-export lattice traits for convenience
pub use lattices::Merge;

/// Marker trait for value types whose `Merge` is a proper lattice
/// (commutative, associative, idempotent).
///
/// Types implementing this trait can be used in the coordination-free
/// lattice merge storage path, where replicas converge without ordering.
///
/// Do NOT implement this for types with non-commutative merge (e.g.,
/// last-writer-wins overwrite). Use the overwrite storage path instead.
pub trait LatticeValue:
    Clone
    + lattices::Merge<Self>
    + lattices::LatticeFrom<Self>
    + lattices::IsBot
    + Default
    + Send
    + Sync
    + 'static
{
}

// CausalWrapper is a proper lattice via DomPair<VCWrapper, SetUnionHashSet<T>>
impl<T> LatticeValue for CausalWrapper<T> where
    T: Clone
        + std::hash::Hash
        + Eq
        + std::fmt::Debug
        + Default
        + Send
        + Sync
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + 'static
{
}
