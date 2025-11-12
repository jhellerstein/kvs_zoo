//! Routing strategies (before-storage)

pub mod single_node;
pub mod round_robin;
pub mod sharded;

pub use single_node::SingleNodeRouter;
pub use round_robin::RoundRobinRouter;
pub use sharded::ShardedRouter;
