let's get rid of demo_driver.rs and inline the ops into the examples' `main` function

---

Let's think about learning goals. This will affect the organization of the book as well as the repo.

1. There are many familiar KVS patterns in the literature we want to show we can cover: replicated, sharded, replicated+sharded, different consistency levels (lww, causal, linearizable), different replication schemes, etc.

2. There are different orthogonal parameters we expose in our design that allow building those patterns (and more, presumably).

We want to help map between (1) and (2), and we want to do it incrementally so we don't confuse our audience.
