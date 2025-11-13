Opportunistic slot-aware replication:
Consider extending after-storage path (e.g., a generic variant of LogBasedDelivery) to accept any message implementing a HasSequence-like trait and sequence when available, bypassing otherwise. This is a slightly larger refactor since current slotted APIs are (usize, String, V)-specific.
Optional: Audit other routers (RoundRobin, SingleNode) for similar generic affordances. They don’t inspect the op today, so no change is necessary functionally.


---
B. Layers with multiple clusters (e.g., Paxos roles)

Also not hard; moderate but localized:
Extend KVSClusters to register/retrieve role-specific clusters for a layer Name. For example: insert/get_role::<Name, Role>() keyed by (Name, RoleTypeId).
Specialize KVSSpec for the Paxos layer to create and register both Proposers and Acceptors under the same Name.
The rest of the stack doesn’t need to know these roles exist; the Paxos layer’s KVSWire impl can look them up internally.

---

Let's think about learning goals. This will affect the organization of the book as well as the repo.

1. There are many familiar KVS patterns in the literature we want to show we can cover: replicated, sharded, replicated+sharded, different consistency levels (lww, causal, linearizable), different replication schemes, etc.

2. There are different orthogonal parameters we expose in our design that allow building those patterns (and more, presumably).

We want to help map between (1) and (2), and we want to do it incrementally so we don't confuse our audience.
