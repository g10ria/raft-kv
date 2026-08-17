package shardgrp

import (
	"fmt"
	"sync"
	"time"

	"raft-kv/simulated-rpc/rpc"
	"raft-kv/sharded-kv/shardcfg"
	"raft-kv/sharded-kv/shardgrp/shardrpc"
	tester "raft-kv/testers/testkit"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	mu      sync.Mutex
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// note: respond with ErrWrongGroup if this shardgrp isn't responsible for this key

	// Repeatedly sends the Get request
	// If the leader is wrong, cycle to the next one and try again
	failures := 0
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		leader := ck.leader
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)

		ck.mu.Lock()
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.mu.Unlock()
			return reply.Value, reply.Version, reply.Err
		} else if ck.leader == leader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}

		if !ok {
			failures += 1
		}
		if failures > 30 {
			ck.mu.Unlock()
			return "", 0, rpc.ErrWrongGroup
		}
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// note: respond with ErrWrongGroup if this shardgrp isn't responsible for this key

	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	leader := ck.leader
	ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

	// return same error if it wasn't wrong leader (we should try again in that case)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}

	// otherwise, keep attempting to submit again (to different leaders)
	failures := 0
	for {
		ck.mu.Lock()
		if ck.leader == leader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		ck.mu.Unlock()

		args := rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := rpc.PutReply{}
		leader = ck.leader
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)

		if ok && reply.Err != rpc.ErrWrongLeader {
			if reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return reply.Err
		}

		if !ok {
			failures += 1
		}
		if failures > 30 {
			return rpc.ErrWrongGroup
		}

		time.Sleep(60 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// calls freeze shard on corresponding server
	fmt.Printf("\tissuing freeze shard request for shard %d config %d\n", s, num)

	failures := 0
	for {
		args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
		reply := shardrpc.FreezeShardReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()

		ok := ck.clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers) // leader wrong, keep cycling
				ck.mu.Unlock()
			} else if reply.Err == rpc.ErrVersion {
				return make([]byte, 0), rpc.ErrVersion // config out of date
			} else if reply.Err == rpc.ErrWrongGroup { // wrong group altogether
				return make([]byte, 0), rpc.ErrWrongGroup
			} else {
				// we're good, return the frozen bytes
				return reply.State, rpc.OK
			}
		} else {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
		}

		if !ok {
			failures += 1
		}
		if failures > 15 {
			return make([]byte, 0), rpc.ErrWrongGroup
		}

		time.Sleep(25 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	fmt.Printf("\tissuing install shard request for shard %d config %d\n", s, num)
	failures := 0
	for {
		args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
		reply := shardrpc.InstallShardReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()

		ok := ck.clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers) // leader wrong, keep cycling
				ck.mu.Unlock()
			} else {
				return reply.Err // return whatever happened
			}
		} else {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
		}

		if !ok {
			failures += 1
		}
		if failures > 15 {
			fmt.Printf("returning err wrong group for install shard %d config %d\n", s, num)
			return rpc.ErrWrongGroup
		}

		time.Sleep(25 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	fmt.Printf("\tissuing delete shard request for shard %d config %d\n", s, num)

	failures := 0
	for {
		args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
		reply := shardrpc.DeleteShardReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers) // leader wrong, keep cycling
				ck.mu.Unlock()
			} else {
				return reply.Err // return whatever happened
			}
		} else {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
		}

		if !ok {
			failures += 1
		}
		if failures > 15 {
			return rpc.ErrWrongGroup
		}

		time.Sleep(25 * time.Millisecond)
	}
}
