//! Routing strategies (before-storage)

pub mod round_robin;
pub mod sharded;
pub mod single_node;

pub use round_robin::RoundRobinRouter;
pub use sharded::ShardedRouter;
pub use single_node::SingleNodeRouter;
