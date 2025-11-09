# Deployment API Redesign

## Problem

The current `Deployment` enum is overfitted to specific architectures:

- `SingleCluster` for simple cases
- `ShardedClusters` for sharded+replicated
- `MultiCluster` with hardcoded Paxos types

This violates the principle that dispatch/maintenance strategies should determine cluster usage.

## Proposed Solution

### 1. Generic Deployment Structure

```rust
/// A deployment is simply a collection of Hydro clusters
pub struct Deployment<'a> {
    /// Named clusters - dispatch strategy knows which it needs
    pub clusters: HashMap<String, Box<dyn ClusterLike<'a>>>,
}
```

Or even simpler - the deployment just provides clusters, and the dispatch/maintenance
strategies know how to wire them:

```rust
pub struct Deployment<'a> {
    /// All clusters in this deployment
    /// Dispatch strategies access them by index or pattern matching
    pub clusters: Vec<Cluster<'a, KVSNode>>,

    /// Optional: auxiliary clusters for protocols like Paxos
    /// Type-erased to avoid hardcoding specific types
    pub aux_clusters: Vec<Box<dyn Any>>,
}
```

### 2. Dispatch Determines Routing

Each dispatch strategy knows:

- How many clusters it needs
- How to route operations to them
- How messages flow between clusters

For example:

- `SingleNodeRouter`: Uses 1 cluster
- `RoundRobinRouter`: Uses 1 cluster, routes round-robin across its nodes
- `ShardedRouter`: Uses N clusters (one per shard)
- `Pipeline<ShardedRouter(3), RoundRobinRouter>`: Uses 3 clusters, routes to shard then round-robin within it
- `PaxosInterceptor`: Uses 1 KVS cluster + 2 aux clusters (proposers, acceptors)

### 3. Builder Configures Clusters

```rust
Server::builder()
    .with_clusters(vec![3, 3, 3])  // 3 clusters of 3 nodes each (sharded+replicated)
    .build_with(dispatch, maintenance)
```

Or more explicitly:

```rust
Server::builder()
    .with_kvs_clusters(3)  // 3 KVS clusters
    .with_nodes_per_cluster(3)  // 3 nodes in each
    .build_with(
        Pipeline::new(ShardedRouter::new(3), RoundRobinRouter::new()),
        BroadcastReplication::default()
    )
```

### 4. Examples

#### Sharded + Replicated

```rust
// 3 shards, 3 replicas per shard = 9 nodes total
let dispatch = Pipeline::new(
    ShardedRouter::new(3),  // Routes to 3 different clusters
    RoundRobinRouter::new(), // Within each cluster, round-robin
);

let (deployment, out, input) = Server::builder()
    .with_kvs_clusters(3)      // 3 separate KVS clusters (one per shard)
    .with_replicas_per_shard(3) // 3 nodes in each cluster
    .build_with(dispatch, maintenance)
    .await?;
```

#### Linearizable (Paxos)

```rust
let dispatch = PaxosInterceptor::with_config(config);

let (deployment, out, input) = Server::builder()
    .with_kvs_replicas(3)    // 3 KVS nodes
    .with_proposers(3)        // 3 Paxos proposers
    .with_acceptors(3)        // 3 Paxos acceptors
    .build_with(dispatch, maintenance)
    .await?;
```

## Implementation Plan

1. Change `Deployment` from enum to struct with generic cluster storage
2. Remove Paxos-specific types from Deployment
3. Update `OpIntercept::Deployment` associated type to be flexible
4. Make dispatch strategies responsible for cluster count/configuration
5. Update builder to expose cluster configuration clearly
6. Remove specialized Pipeline implementation - let dispatch handle it
