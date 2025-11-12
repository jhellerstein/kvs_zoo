//! Replication strategies and wrappers (after-storage)

pub mod logbased;
pub mod broadcast;
pub mod gossip;

pub use broadcast::*;
pub use gossip::*;
pub use logbased::LogBasedDelivery;
