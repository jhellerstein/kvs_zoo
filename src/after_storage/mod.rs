//! After-storage stages (replication, cleanup, responders)

use crate::kvs_core::KVSNode;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub mod replication;
pub mod cleanup;
pub mod responders;

/* ------------------------------------------------------------------------- */
/* Core after-storage traits and helpers (migrated from legacy `maintenance`) */
/* ------------------------------------------------------------------------- */

/// Compose two maintenance strategies so they can run concurrently.
///
/// This allows, for example, running a replication strategy (with its own
/// tick/period) alongside a tombstone cleanup strategy (with a different
/// tick/period). Each strategy can schedule itself independently; their
/// emitted update streams are merged.
#[derive(Clone, Debug)]
pub struct CombinedMaintenance<A, B> {
	pub a: A,
	pub b: B,
}

impl<A, B> CombinedMaintenance<A, B> {
	pub fn new(a: A, b: B) -> Self {
		Self { a, b }
	}
}

/// Extension trait to ergonomically compose maintenance strategies
pub trait MaintenanceComposeExt: Sized {
	fn and<O>(self, other: O) -> CombinedMaintenance<Self, O> {
		CombinedMaintenance::new(self, other)
	}
}

impl<T> MaintenanceComposeExt for T {}

/// Readability alias for "no maintenance/replication".
///
/// This is equivalent to the unit type `()` which already implements
/// `ReplicationStrategy<V>`. Prefer `ZeroMaintenance` in examples and type
/// signatures when you want to emphasize that there is intentionally no
/// background maintenance.
pub type ZeroMaintenance = ();

/// Unified replication update type
///
/// Replication strategies can accept and emit both unslotted (unordered)
/// and slotted (slot-ordered) updates through a single API via this enum.
#[derive(Clone, Debug)]
pub enum ReplicationUpdate<V> {
	Unslotted((String, V)),
	Slotted((usize, String, V)),
}

/// Core trait for replication strategies
///
/// Replication strategies handle background data synchronization between nodes,
/// operating independently of operation processing. They ensure data consistency
/// and availability across the distributed system.
pub trait ReplicationStrategy<V> {
	/// Unified replication entry: replicate updates (slotted or unslotted)
	///
	/// Implementers may override this to handle both update kinds in one place.
	/// Default dispatch adapters call `replicate_data` and `replicate_slotted_data`
	/// as appropriate and merge the results.
	fn replicate_updates<'a>(
		&self,
		cluster: &Cluster<'a, KVSNode>,
		updates: Stream<ReplicationUpdate<V>, Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<ReplicationUpdate<V>, Cluster<'a, KVSNode>, Unbounded>
	where
		V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
	{
		let unslotted_in = updates
			.clone()
			.filter_map(q!(|u| match u { ReplicationUpdate::Unslotted(t) => Some(t), _ => None }));
		let slotted_in = updates
			.filter_map(q!(|u| match u { ReplicationUpdate::Slotted(t) => Some(t), _ => None }));

		let unslotted_out = self
			.replicate_data(cluster, unslotted_in)
			.map(q!(|t| ReplicationUpdate::Unslotted(t)));
		let slotted_out = self
			.replicate_slotted_data(cluster, slotted_in)
			.map(q!(|t| ReplicationUpdate::Slotted(t)));

		unslotted_out
			.interleave(slotted_out)
			.assume_ordering(nondet!(/** merged replication updates */))
	}

	/// Replicate data across the cluster (unordered)
	///
	/// Takes a stream of local data updates and returns a stream of data
	/// replicated by other nodes. The strategy determines how data is
	/// synchronized (gossip, broadcast, etc.).
	fn replicate_data<'a>(
		&self,
		cluster: &Cluster<'a, KVSNode>,
		local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
	where
		V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
	{
		let updates = local_data.map(q!(|t| ReplicationUpdate::Unslotted(t)));
		self
			.replicate_updates(cluster, updates)
			.filter_map(q!(|u| match u { ReplicationUpdate::Unslotted(t) => Some(t), _ => None }))
	}

	/// Replicate slotted data across the cluster (ordered by slot)
	///
	/// Takes a stream of slot-indexed data updates and returns a stream of
	/// replicated data received from other nodes, maintaining slot ordering.
	/// This is used by consensus protocols like Paxos to ensure operations
	/// are applied in the same order across all replicas.
	///
	/// Default implementation adapts to the unified `replicate_updates` API.
	fn replicate_slotted_data<'a>(
		&self,
		cluster: &Cluster<'a, KVSNode>,
		local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>
	where
		V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
	{
		let updates = local_slotted_data.map(q!(|(s, k, v)| ReplicationUpdate::Slotted((s, k, v))));
		self
			.replicate_updates(cluster, updates)
			.filter_map(q!(|u| match u { ReplicationUpdate::Slotted(t) => Some(t), _ => None }))
	}

}

/// Upward pass (After storage) maintenance hook for responses
///
/// Default implementation is pass-through. Strategies that want to adjust
/// response behavior (e.g., add headers, redact, sample) can override.
pub trait MaintenanceAfterResponses {
	fn after_responses<'a>(
		&self,
		_cluster: &Cluster<'a, KVSNode>,
		responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<String, Cluster<'a, KVSNode>, Unbounded>;
}

// Default pass-through impl for unit type () so examples can use () as maintenance and still participate.
impl MaintenanceAfterResponses for () {
	fn after_responses<'a>(
		&self,
		_cluster: &Cluster<'a, KVSNode>,
		responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
		responses
	}
}

/// No-op replication strategy for single-node systems
///
/// This strategy performs no replication, making it suitable for:
/// - Single-node deployments
/// - Development and testing
/// - Systems that don't require replication
#[derive(Clone, Debug, Default)]
pub struct NoReplication;

impl NoReplication {
	/// Create a new no-replication strategy
	pub fn new() -> Self {
		Self
	}
}

impl<V> ReplicationStrategy<V> for NoReplication {
	fn replicate_data<'a>(
		&self,
		_cluster: &Cluster<'a, KVSNode>,
		local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
	where
		V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
	{
		// No replication - just return the local data stream unchanged
		local_data
	}
}

// Upward pass default for NoReplication: pass-through
impl MaintenanceAfterResponses for NoReplication {
	fn after_responses<'a>(
		&self,
		_cluster: &Cluster<'a, KVSNode>,
		responses: Stream<String, Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<String, Cluster<'a, KVSNode>, Unbounded> {
		responses
	}
}

/// Unit type implementation for no replication
///
/// The unit type `()` can be used as a convenient no-op replication strategy,
/// providing the same behavior as NoReplication with even less overhead.
impl<V> ReplicationStrategy<V> for () {
	fn replicate_data<'a>(
		&self,
		_cluster: &Cluster<'a, KVSNode>,
		local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>
	where
		V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
	{
		// No replication - just return the local data stream unchanged
		local_data
	}
}

// Blanket impls for combining two maintenance strategies
impl<V, A, B> ReplicationStrategy<V> for CombinedMaintenance<A, B>
where
	V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
	A: ReplicationStrategy<V>,
	B: ReplicationStrategy<V>,
{
	fn replicate_data<'a>(
		&self,
		cluster: &Cluster<'a, KVSNode>,
		local_data: Stream<(String, V), Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<(String, V), Cluster<'a, KVSNode>, Unbounded> {
		let a_out = self.a.replicate_data(cluster, local_data.clone());
		let b_out = self.b.replicate_data(cluster, local_data);
		a_out
			.interleave(b_out)
			.assume_ordering(nondet!(/** merged maintenance outputs */))
	}

	fn replicate_slotted_data<'a>(
		&self,
		cluster: &Cluster<'a, KVSNode>,
		local_slotted_data: Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded>,
	) -> Stream<(usize, String, V), Cluster<'a, KVSNode>, Unbounded> {
		let a_out = self
			.a
			.replicate_slotted_data(cluster, local_slotted_data.clone());
		let b_out = self.b.replicate_slotted_data(cluster, local_slotted_data);
		a_out
			.interleave(b_out)
			.assume_ordering(nondet!(/** merged maintenance outputs (slotted) */))
	}
}
