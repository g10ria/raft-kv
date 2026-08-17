package shardedkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"fmt"
	"sync"
	"time"

	"raft-kv/simulated-rpc/rpc"
	kvtest "raft-kv/testers/kvtest"
	"raft-kv/sharded-kv/shardcfg"
	"raft-kv/sharded-kv/shardctrler"
	"raft-kv/sharded-kv/shardgrp"
	tester "raft-kv/testers/testkit"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	mu   sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	responsibleShard := shardcfg.Key2Shard(key)

	ck.mu.Lock()
	config := ck.sck.Query()
	gid := config.Shards[responsibleShard]
	fmt.Printf("CLI issuing get %s to shard %d in group %d\n", key, responsibleShard, gid)
	_, servers, _ := config.GidServers(responsibleShard)
	clerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.mu.Unlock()

	ret, version, err := clerk.Get(key)

	attempts := 1 // cap retries at 50 attempts
	for err == rpc.ErrWrongGroup {
		ck.mu.Lock()
		fmt.Printf("wrong group %d, retrying get...\n", gid)
		currentConfig := ck.sck.Query()
		_, servers, _ := currentConfig.GidServers(responsibleShard)
		clerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Unlock()

		ret, version, err = clerk.Get(key)

		attempts += 1

		if attempts > 50 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return ret, version, err
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	responsibleShard := shardcfg.Key2Shard(key)

	ck.mu.Lock()
	config := ck.sck.Query()
	gid := config.Shards[responsibleShard]
	fmt.Printf("CLI issuing put %s to shard %d in group %d: %s %d\n", key, responsibleShard, gid, value, version)
	_, servers, _ := config.GidServers(responsibleShard)
	clerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.mu.Unlock()

	err := clerk.Put(key, value, version)

	attempts := 1 // cap retries at 50 attempts
	for err == rpc.ErrWrongGroup {
		fmt.Printf("wrong group %d, retrying put...\n", responsibleShard)

		ck.mu.Lock()
		currentConfig := ck.sck.Query()
		_, servers, _ := currentConfig.GidServers(responsibleShard)
		clerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Unlock()

		err = clerk.Put(key, value, version)
		attempts += 1

		if attempts > 50 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if err == rpc.ErrVersion && attempts > 1 {
		return rpc.ErrMaybe
	} else {
		return err
	}
}
