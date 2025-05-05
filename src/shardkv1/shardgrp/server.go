package shardgrp

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
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

	// values map[string]ValueTuple // old key value store, without sharding
	values map[shardcfg.Tshid](map[string]ValueTuple) // map from shard ID to a set of key values

	// highestConfigNumSeen shardcfg.Tnum    // highest config number that this server has seen
	shards            []shardcfg.Tshid // keeps track of the shards this server is allowed to service
	frozen_shards     []shardcfg.Tshid // shards that are currently frozen!
	shard_config_nums []shardcfg.Tnum  // highest config number that this server has seen for each config
}

func (kv *KVServer) InitializeShardMapIfApplicable(shard_id shardcfg.Tshid) {
	_, ok := kv.values[shard_id]
	if !ok {
		kv.values[shard_id] = make(map[string]ValueTuple)
	}
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.PutArgs:
		// fmt.Printf("grp%d put\n", kv.gid)

		if !kv.KeyIsOurs(args.Key) || kv.KeyIsFrozen(args.Key) {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		shard_id := shardcfg.Key2Shard(args.Key)
		kv.InitializeShardMapIfApplicable(shard_id)
		val, ok := kv.values[shard_id][args.Key]
		// perform the put if key exists and versions match, otherwise return an error
		if ok {
			// fmt.Printf("\tserver %d applying put %s %s\n", kv.gid, args.Key, args.Value)
			if val.Version == args.Version {
				kv.values[shard_id][args.Key] = ValueTuple{args.Value, val.Version + 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrVersion}
		} else if args.Version == 0 {
			// creating a new value
			kv.values[shard_id][args.Key] = ValueTuple{args.Value, 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	case rpc.GetArgs:
		// fmt.Printf("grp%d get\n", kv.gid)

		if !kv.KeyIsOurs(args.Key) || kv.KeyIsFrozen(args.Key) {
			return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrWrongGroup}
		}

		shard_id := shardcfg.Key2Shard(args.Key)
		kv.InitializeShardMapIfApplicable(shard_id)
		val, ok := kv.values[shard_id][args.Key]
		// return the value and version if existing, otherwise return ErrNoKey
		if ok {
			// fmt.Printf("\tserver %d applying get %s\n", kv.gid, args.Key)
			return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		}
		return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}

	case shardrpc.FreezeShardArgs:
		fmt.Printf("\tgrp%d freeze shard\n", kv.gid)
		if args.Num < kv.shard_config_nums[args.Shard] {
			return rpc.ErrVersion
		}

		kv.shard_config_nums[args.Shard] = args.Num

		// Freeze the shard
		if !slices.Contains(kv.frozen_shards, args.Shard) {
			kv.frozen_shards = append(kv.frozen_shards, args.Shard)
		}

		// Retrieve the data!
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.values[args.Shard])
		// fmt.Printf("grp%d shards: %v\n", kv.gid, kv.shards)
		return shardrpc.FreezeShardReply{State: w.Bytes(), Num: args.Num, Err: rpc.OK}

	case shardrpc.InstallShardArgs:
		if args.Num < kv.shard_config_nums[args.Shard] {
			return rpc.ErrVersion
		}

		kv.shard_config_nums[args.Shard] = args.Num

		// Append the shard to shard array
		if !slices.Contains(kv.shards, args.Shard) {
			kv.shards = append(kv.shards, args.Shard) // append new shard
		}

		// Attach new values into values map
		r := bytes.NewBuffer(args.State)
		d := labgob.NewDecoder(r)
		var values map[string]ValueTuple
		if d.Decode(&values) == nil {
			kv.values[args.Shard] = values
		}
		fmt.Printf("\tgrp%d install shard %d -> %v\n", kv.gid, args.Shard, kv.shards)
		return shardrpc.InstallShardReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:
		if args.Num < kv.shard_config_nums[args.Shard] {
			return rpc.ErrVersion
		}

		kv.shard_config_nums[args.Shard] = args.Num

		// Delete the shard
		if slices.Contains(kv.shards, args.Shard) {
			kv.shards = slices.DeleteFunc(kv.shards, func(n shardcfg.Tshid) bool {
				return n == args.Shard
			})
			kv.frozen_shards = slices.DeleteFunc(kv.frozen_shards, func(n shardcfg.Tshid) bool {
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
	e := labgob.NewEncoder(w)
	e.Encode(kv.values)
	e.Encode(kv.shards)
	e.Encode(kv.frozen_shards)
	e.Encode(kv.shard_config_nums)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	fmt.Printf("\t\tgrp%d restoring\n", kv.gid)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var values map[shardcfg.Tshid](map[string]ValueTuple)
	var shards []shardcfg.Tshid
	var frozen_shards []shardcfg.Tshid
	var shard_config_nums []shardcfg.Tnum

	if d.Decode(&values) == nil &&
		d.Decode(&shards) == nil &&
		d.Decode(&frozen_shards) == nil &&
		d.Decode(&shard_config_nums) == nil {
		kv.values = values
		kv.shards = shards
		kv.frozen_shards = frozen_shards
		kv.shard_config_nums = shard_config_nums
	}

	fmt.Printf("DECODED grp%d shards: %v\n", kv.gid, kv.shards)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rsm_reply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		rsm_reply_cast := rsm_reply.(rpc.GetReply)
		reply.Value = rsm_reply_cast.Value
		reply.Version = rsm_reply_cast.Version
		reply.Err = rsm_reply_cast.Err
	}
}

func (kv *KVServer) KeyIsOurs(key string) bool {
	shard := shardcfg.Key2Shard(key)
	return slices.Contains(kv.shards, shard)
}

func (kv *KVServer) KeyIsFrozen(key string) bool {
	shard := shardcfg.Key2Shard(key)
	return slices.Contains(kv.frozen_shards, shard)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rsm_reply := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		rsm_reply_cast := rsm_reply.(rpc.PutReply)
		reply.Err = rsm_reply_cast.Err
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rsm_reply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		rsm_reply_cast := rsm_reply.(shardrpc.FreezeShardReply)
		reply.Err = rsm_reply_cast.Err
		reply.State = rsm_reply_cast.State
		reply.Num = rsm_reply_cast.Num
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rsm_reply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		rsm_reply_cast := rsm_reply.(shardrpc.InstallShardReply)
		reply.Err = rsm_reply_cast.Err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rsm_reply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		rsm_reply_cast := rsm_reply.(shardrpc.DeleteShardReply)
		reply.Err = rsm_reply_cast.Err
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:               gid,
		me:                me,
		values:            make(map[shardcfg.Tshid](map[string]ValueTuple)),
		shard_config_nums: make([]shardcfg.Tnum, shardcfg.NShards),
		shards:            []shardcfg.Tshid{},
		frozen_shards:     []shardcfg.Tshid{},
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
