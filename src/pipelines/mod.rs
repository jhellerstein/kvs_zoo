//! Reusable Hydro wiring pipelines (before_storage + after_storage)
//!
//! Small, well-named building blocks that wire common dataflow patterns.
//! These make the before_storage (routing/ordering) and after_storage
//! (replication/cleanup/responders) passes explicit and easy to study.

pub mod single_layer;
pub mod two_layer;

pub use single_layer::pipeline_single_layer_from_process;
pub use two_layer::{pipeline_two_layer_from_enveloped, pipeline_two_layer_from_process};
