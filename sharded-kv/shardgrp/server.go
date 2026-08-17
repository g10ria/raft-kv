package shardgrp

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"raft-kv/sharded-kv/rsm"
	"raft-kv/simulated-rpc/rpc"
	"encoding/gob"
	"raft-kv/simulated-rpc"
	"raft-kv/sharded-kv/shardcfg"
	"raft-kv/sharded-kv/shardgrp/shardrpc"
	tester "raft-kv/testers/testkit"
)

type ValueTuple struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid
	mu   sync.Mutex

	values map[shardcfg.Tshid](map[string]ValueTuple) // map from shard ID to a set of key values

	shards          []shardcfg.Tshid // keeps track of the shards this server is allowed to service
	frozenShards    []shardcfg.Tshid // shards that are currently frozen
	shardConfigNums []shardcfg.Tnum  // highest config number that this server has seen for each config
}

func (kv *KVServer) ensureShardMap(shardID shardcfg.Tshid) {
	_, ok := kv.values[shardID]
	if !ok {
		kv.values[shardID] = make(map[string]ValueTuple)
	}
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.PutArgs:
		if !kv.ownsKey(args.Key) || kv.keyFrozen(args.Key) {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		shardID := shardcfg.Key2Shard(args.Key)
		kv.ensureShardMap(shardID)
		val, ok := kv.values[shardID][args.Key]
		// perform the put if key exists and versions match, otherwise return an error
		if ok {
			if val.Version == args.Version {
				kv.values[shardID][args.Key] = ValueTuple{args.Value, val.Version + 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrVersion}
		} else if args.Version == 0 {
			// creating a new value
			kv.values[shardID][args.Key] = ValueTuple{args.Value, 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	case rpc.GetArgs:
		if !kv.ownsKey(args.Key) || kv.keyFrozen(args.Key) {
			return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrWrongGroup}
		}

		shardID := shardcfg.Key2Shard(args.Key)
		kv.ensureShardMap(shardID)
		val, ok := kv.values[shardID][args.Key]
		// return the value and version if existing, otherwise return ErrNoKey
		if ok {
			return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		}
		return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}

	case shardrpc.FreezeShardArgs:
		if args.Num < kv.shardConfigNums[args.Shard] {
			fmt.Printf("\t\tgrp%d freeze shard %d (rejecting old)\n", kv.gid, args.Shard)
			return shardrpc.FreezeShardReply{Err: rpc.ErrVersion}
		}

		// only freeze shards we currently own
		kv.shardConfigNums[args.Shard] = args.Num

		// Freeze the shard, if we're allowed (it's currently in shards)
		if slices.Contains(kv.shards, args.Shard) {
			if !slices.Contains(kv.frozenShards, args.Shard) {
				kv.frozenShards = append(kv.frozenShards, args.Shard)
			}
		}

		// Retrieve the data!
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.values[args.Shard])

		fmt.Printf("\t\tgrp%d froze shard %d values: %v\n", kv.gid, args.Shard, kv.values[args.Shard])

		return shardrpc.FreezeShardReply{State: w.Bytes(), Num: args.Num, Err: rpc.OK}

	case shardrpc.InstallShardArgs:
		if args.Num < kv.shardConfigNums[args.Shard] {
			fmt.Printf("\t\tgrp%d install shard %d (rejecting old) %v\n", kv.gid, args.Shard, kv.shards)
			return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
		}

		kv.shardConfigNums[args.Shard] = args.Num

		// Append the shard to shard array
		if !slices.Contains(kv.shards, args.Shard) {
			kv.shards = append(kv.shards, args.Shard) // append new shard
		}

		// Attach new values into values map
		r := bytes.NewBuffer(args.State)
		d := gob.NewDecoder(r)
		var values map[string]ValueTuple

		if d.Decode(&values) == nil {
			// check if values is empty
			if len(values) != 0 {
				kv.values[args.Shard] = values
			}
		}
		fmt.Printf("\tgrp%d installed shard %d -> %v\n", kv.gid, args.Shard, kv.shards)
		fmt.Printf("\t\tgrp%d installed shard %d values: %v\n", kv.gid, args.Shard, kv.values[args.Shard])
		return shardrpc.InstallShardReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:
		if args.Num < kv.shardConfigNums[args.Shard] {
			fmt.Printf("\t\tgrp%d delete shard %d (rejecting old) %v\n", kv.gid, args.Shard, kv.shards)
			return shardrpc.DeleteShardReply{Err: rpc.ErrVersion}
		}

		kv.shardConfigNums[args.Shard] = args.Num

		// Delete the shard if we currently own it
		if slices.Contains(kv.shards, args.Shard) {
			kv.shards = slices.DeleteFunc(kv.shards, func(n shardcfg.Tshid) bool {
				return n == args.Shard
			})
			kv.frozenShards = slices.DeleteFunc(kv.frozenShards, func(n shardcfg.Tshid) bool {
				return n == args.Shard
			})
			delete(kv.values, args.Shard)
		}

		fmt.Printf("\tgrp%d delete shard %d -> %v\n", kv.gid, args.Shard, kv.shards)

		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	fmt.Printf("\t\tgrp%d snapshotting\n", kv.gid)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.values)
	e.Encode(kv.shards)
	e.Encode(kv.frozenShards)
	e.Encode(kv.shardConfigNums)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	fmt.Printf("\t\tgrp%d restoring\n", kv.gid)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var values map[shardcfg.Tshid](map[string]ValueTuple)
	var shards []shardcfg.Tshid
	var frozenShards []shardcfg.Tshid
	var shardConfigNums []shardcfg.Tnum

	if d.Decode(&values) == nil &&
		d.Decode(&shards) == nil &&
		d.Decode(&frozenShards) == nil &&
		d.Decode(&shardConfigNums) == nil {
		kv.values = values
		kv.shards = shards
		kv.frozenShards = frozenShards
		kv.shardConfigNums = shardConfigNums
	}

	fmt.Printf("DECODED grp%d shards: %v\n", kv.gid, kv.shards)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rsmReply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		typedReply := rsmReply.(rpc.GetReply)
		reply.Value = typedReply.Value
		reply.Version = typedReply.Version
		reply.Err = typedReply.Err
	}
}

func (kv *KVServer) ownsKey(key string) bool {
	shard := shardcfg.Key2Shard(key)
	return slices.Contains(kv.shards, shard)
}

func (kv *KVServer) keyFrozen(key string) bool {
	shard := shardcfg.Key2Shard(key)
	return slices.Contains(kv.frozenShards, shard)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rsmReply := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		typedReply := rsmReply.(rpc.PutReply)
		reply.Err = typedReply.Err
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rsmReply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		typedReply := rsmReply.(shardrpc.FreezeShardReply)
		reply.Err = typedReply.Err
		reply.State = typedReply.State
		reply.Num = typedReply.Num
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rsmReply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		typedReply := rsmReply.(shardrpc.InstallShardReply)
		reply.Err = typedReply.Err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rsmReply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		typedReply := rsmReply.(shardrpc.DeleteShardReply)
		reply.Err = typedReply.Err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServerShardGrp starts a server for shardgrp `gid`.
//
// StartServerShardGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*simulatedrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(rpc.PutArgs{})
	gob.Register(rpc.GetArgs{})
	gob.Register(shardrpc.FreezeShardArgs{})
	gob.Register(shardrpc.InstallShardArgs{})
	gob.Register(shardrpc.DeleteShardArgs{})
	gob.Register(rsm.Op{})

	kv := &KVServer{
		gid:             gid,
		me:              me,
		values:          make(map[shardcfg.Tshid](map[string]ValueTuple)),
		shardConfigNums: make([]shardcfg.Tnum, shardcfg.NShards),
		shards:          []shardcfg.Tshid{},
		frozenShards:    []shardcfg.Tshid{},
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// first server starts with all the shards
	if gid == shardcfg.Gid1 {
		for shard := range shardcfg.NShards {
			kv.shards = append(kv.shards, shardcfg.Tshid(shard))
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
