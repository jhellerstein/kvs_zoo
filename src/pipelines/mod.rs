//! Reusable Hydro wiring pipelines (before_storage + after_storage)
//!
//! Small, well-named building blocks that wire common dataflow patterns.
//! These make the before_storage (routing/ordering) and after_storage
//! (replication/cleanup/responders) passes explicit and easy to study.

pub mod two_layer;

pub use two_layer::pipeline_two_layer;
