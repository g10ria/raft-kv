package shardkv

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

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	mu   sync.Mutex

	subClerks map[shardcfg.Tshid]*shardgrp.Clerk // map from a shard group ID to a clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		sck:       sck,
		subClerks: make(map[shardcfg.Tshid]*shardgrp.Clerk),
	}
	return ck
}

func (ck *Clerk) addClerkForShard(shard_id shardcfg.Tshid) {
	current_config := ck.sck.Query()
	_, servers, _ := current_config.GidServers(shard_id)
	ck.subClerks[shard_id] = shardgrp.MakeClerk(ck.clnt, servers)
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	responsible_shard := shardcfg.Key2Shard(key)

	ck.mu.Lock()
	config := ck.sck.Query()
	gid := config.Shards[responsible_shard]
	fmt.Printf("CLI issuing get %s to shard %d in group %d\n", key, responsible_shard, gid)
	_, servers, _ := config.GidServers(responsible_shard)
	clerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.mu.Unlock()

	// now we have the clerk
	ret, version, err := clerk.Get(key)

	num_tries := 1 // tbh only try like 50 times max
	for err == rpc.ErrWrongGroup {
		ck.mu.Lock()
		fmt.Printf("wrong group %d, retrying get...\n", gid)
		current_config := ck.sck.Query()
		_, servers, _ := current_config.GidServers(responsible_shard)
		clerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Unlock()

		ret, version, err = clerk.Get(key)

		num_tries += 1

		if num_tries > 50 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// fmt.Printf("CLI returning %s\n", err)

	return ret, version, err
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	responsible_shard := shardcfg.Key2Shard(key)

	ck.mu.Lock()
	config := ck.sck.Query()
	gid := config.Shards[responsible_shard]
	fmt.Printf("CLI issuing put %s to shard %d in group %d: %s %d\n", key, responsible_shard, gid, value, version)
	_, servers, _ := config.GidServers(responsible_shard)
	clerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.mu.Unlock()

	err := clerk.Put(key, value, version)

	num_tries := 1 // tbh only try like 50 times max
	for err == rpc.ErrWrongGroup {
		fmt.Printf("wrong group %d, retrying put...\n", responsible_shard)

		ck.mu.Lock()
		current_config := ck.sck.Query()
		_, servers, _ := current_config.GidServers(responsible_shard)
		clerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Unlock()

		err = clerk.Put(key, value, version)
		num_tries += 1

		if num_tries > 50 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// fmt.Printf("CLI returning %s\n", err)

	if err == rpc.ErrVersion && num_tries > 1 {
		return rpc.ErrMaybe
	} else {
		return err
	}
}
