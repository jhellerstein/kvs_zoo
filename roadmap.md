why do the single_layer.rs and two_layer.rs files both discuss "replication"? Is that just legacy and they should be renamed "after_storage"? 

And why do these layers split out the puts? First, cloning these streams is expensive and I'd like to avoid it. Second, architecturally it seems to me the payloads to be processed with after_storage should come from the KVSCore, not the layer pipelines. The layer pipelines should just pass the messages up and down and invoke the before/after logic.

---

Let's think about learning goals. This will affect the organization of the book as well as the repo.

1. There are many familiar KVS patterns in the literature we want to show we can cover: replicated, sharded, replicated+sharded, different consistency levels (lww, causal, linearizable), different replication schemes, etc.

2. There are different orthogonal parameters we expose in our design that allow building those patterns (and more, presumably).

We want to help map between (1) and (2), and we want to do it incrementally so we don't confuse our audience.
