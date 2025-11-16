
Optional: Audit other routers (RoundRobin, SingleNode) for generic affordances to unify sequenced and non-sequenced streams. They don’t inspect the op today, so no change is necessary functionally.

---

It's not pretty that the examples have to wrap an Envelope around the requests to get responses. Let's make that happen in a common place.

--- 

Right now the crate hard-wires TombSet to a HashSet<String> even when you flip the tombstone_fst feature, so the FST path is effectively just a placeholder. I left the HashSet feature as the default for two reasons:

The FST-backed lattices::FstTombstoneSet only works for String keys and pulls in the heavier fst dependency; the HashSet fallback is still the simplest “just works everywhere” option while the FST plumbing is being finished.
We haven’t actually switched StoreState over to use FstTombstoneSet<String> yet, so flipping the default feature today wouldn’t change behavior—it would just suggest we’re shipping the compressed implementation when we aren’t.
Once we update StoreState (and the rest of the pipeline) to instantiate FstTombstoneSet<String> under the tombstone_fst flag and confirm there aren’t performance or build regressions, we can drop the HashSet default and make the FST feature the norm, keeping the HashSet version only as a fallback for non-String keys or minimal builds.
---

Let's think about learning goals. This will affect the organization of the book as well as the repo.

1. There are many familiar KVS patterns in the literature we want to show we can cover: replicated, sharded, replicated+sharded, different consistency levels (lww, causal, linearizable), different replication schemes, etc.

2. There are different orthogonal parameters we expose in our design that allow building those patterns (and more, presumably).

We want to help map between (1) and (2), and we want to do it incrementally so we don't confuse our audience.
