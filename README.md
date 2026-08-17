# raft-kv

A distributed key/value storage service written from scratch in Go, built on a full implementation of the [Raft](https://raft.github.io/) consensus algorithm. The sharded service dynamically rebalances data across
independent Raft groups, accounting for crash recovery, network partitions, message loss/reordering, 
and concurrent clients. Written for Distributed Systems (spring 2025).

## Components

### `raft/` - Raft implementation
A full implementation of the [Raft](https://raft.github.io/) consensus algorithm: leader election,
log replication, persistence across restarts, and log compaction via snapshotting.

### `sharded-kv/` - sharded key/value service
A key/value service horizontally partitioned into shards, where each shard is served by an
independent replicated Raft group. A central shard
controller (`sharded-kv/shardctrler/`) coordinates which group owns which shards and handles
changes in that configuration without losing writes or serving stale reads mid-migration.

## Testing

The folders `simulated-rpc/` and `testers/` contain course-provided files for simulating KV clients making requests across an unreliable network (dropped/delayed/reordered messages, partitions), testing the service's correctness, linearizability, and robustness.