//! Placeholder pipeline module for Phase 0 restart skeleton.
//! Intentionally minimal; legacy pipeline abstractions are being replaced.
//! We keep re-exports so existing impl paths like `pipeline::MetaMessage` still compile.

// Re-export metadata traits for compatibility during transition.
pub use crate::after_storage::meta::{MetaLocalHandler, MetaMessage};

// Marker struct (unused) to indicate placeholder status.
#[allow(dead_code)]
pub struct PipelinePlaceholder;
