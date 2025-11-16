//! Placeholder pipeline module for Phase 0 restart skeleton.
//! Intentionally minimal; legacy pipeline abstractions are being replaced.
//! We keep re-exports so existing impl paths like `pipeline::MaintenanceMsg` still compile.

// Re-export maintenance metadata traits for compatibility during transition.
pub use crate::after_storage::meta::{MaintenanceLocalHandler, MaintenanceMsg};

// Marker struct (unused) to indicate placeholder status.
#[allow(dead_code)]
pub struct PipelinePlaceholder;
