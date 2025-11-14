
Optional: Audit other routers (RoundRobin, SingleNode) for generic affordances to unify sequenced and non-sequenced streams. They don’t inspect the op today, so no change is necessary functionally.

---

the tip of main for hydro (cached in this workspace) now has good support for tombstoned sets. Let's use them in KVSCore, and then implement a proper Cluster-layer tombstone cleanup.  We need to think about how we want to implement that. We can use our vector clock implementation in src/values to ensure a given node knows a high-watermark on other node's clocks. Then the question is how we efficiently track timestamps for the tombstone sets... 

An open design question is whether this logic should be in Hydro's lattice crate as part of the tombstone sets, or here.

Let's discuss before going forward.

--- 

Let's think about learning goals. This will affect the organization of the book as well as the repo.

1. There are many familiar KVS patterns in the literature we want to show we can cover: replicated, sharded, replicated+sharded, different consistency levels (lww, causal, linearizable), different replication schemes, etc.

2. There are different orthogonal parameters we expose in our design that allow building those patterns (and more, presumably).

We want to help map between (1) and (2), and we want to do it incrementally so we don't confuse our audience.
