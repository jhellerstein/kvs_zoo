//! A compressed tombstone set using the `fst` crate.
//!
//! Provides a lattice-friendly set type that supports union merges and efficient
//! membership checks using a finite state transducer.

use fst::{Set, Streamer};
use lattices::Merge;

/// FST-backed tombstone set for `String` keys.
#[derive(Debug, Clone)]
pub struct TombSetFst {
	set: Set<Vec<u8>>,
}

impl TombSetFst {
	/// Returns true if the key is contained in the set.
	pub fn contains(&self, key: &str) -> bool {
		self.set.contains(key)
	}

	/// Iterate all keys as owned `String`s.
	pub fn keys(&self) -> Vec<String> {
		let mut out = Vec::new();
		let mut stream = self.set.stream();
		while let Some(bytes) = stream.next() {
			if let Ok(s) = String::from_utf8(bytes.to_vec()) {
				out.push(s);
			}
		}
		out
	}

	fn count(&self) -> usize {
		let mut c = 0usize;
		let mut stream = self.set.stream();
		while let Some(_bytes) = stream.next() {
			c += 1;
		}
		c
	}
}

impl Default for TombSetFst {
	fn default() -> Self {
		// Build an empty set
		let set = Set::from_iter::<&str, _>(std::iter::empty()).expect("empty set");
		TombSetFst { set }
	}
}

impl FromIterator<String> for TombSetFst {
	fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
		// Collect, sort, and dedup keys as required by SetBuilder
		let mut keys: Vec<String> = iter.into_iter().collect();
		keys.sort();
		keys.dedup();
		let set = Set::from_iter::<&str, _>(keys.iter().map(|s| s.as_str())).expect("set from iter");
		TombSetFst { set }
	}
}

impl Merge<TombSetFst> for TombSetFst {
	fn merge(&mut self, other: TombSetFst) -> bool {
		// Union by rebuilding from combined keys; simple and correct.
		let before = self.count();
		let mut keys = self.keys();
		keys.extend(other.keys().into_iter());
		let merged = TombSetFst::from_iter(keys);
		*self = merged;
		self.count() > before
	}
}
