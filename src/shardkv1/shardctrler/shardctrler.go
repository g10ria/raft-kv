package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()
	key    string

	mu sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.key = "CONFIG"
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// sck.mu.Lock()
	// defer sck.mu.Unlock()

	shardConfigString := cfg.String()
	err := sck.IKVClerk.Put(sck.key, shardConfigString, rpc.Tversion(cfg.Num-1))

	if err == rpc.OK {
		fmt.Printf("init config was ok!\n")
	} else {
		fmt.Printf("%s", err)
	}

	fmt.Printf("put %s\n", shardConfigString)
}

type ShardMove struct {
	tshid shardcfg.Tshid
	from  tester.Tgid
	to    tester.Tgid
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	fmt.Printf("updating config to %d\n", new.Num)

	sck.mu.Lock()
	defer sck.mu.Unlock()

	old := sck.Query()
	shardMoves := make([]ShardMove, 0) // store all of the shard moves

	// For each shard, if they're assigned to a different shard group now
	for i := 0; i < shardcfg.NShards; i++ {
		if old.Shards[i] != new.Shards[i] {
			shardMoves = append(shardMoves, ShardMove{
				tshid: shardcfg.Tshid(i),
				from:  old.Shards[i],
				to:    new.Shards[i],
			})
		}
	}

	// making new clerks each time
	for _, move := range shardMoves {
		fmt.Printf("EXECUTING SHARDMOVE shard %d from grp%d to grp%d\n", move.tshid, move.from, move.to)
		from := move.from
		to := move.to

		from_clerk := shardgrp.MakeClerk(sck.clnt, old.Groups[from])
		to_clerk := shardgrp.MakeClerk(sck.clnt, new.Groups[to])

		// TODO: handle errors here properly
		state, err := from_clerk.FreezeShard(move.tshid, new.Num)
		if err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			old = sck.Query()
			from_clerk = shardgrp.MakeClerk(sck.clnt, old.Groups[from])
			to_clerk = shardgrp.MakeClerk(sck.clnt, new.Groups[to])
		}
		to_clerk.InstallShard(move.tshid, state, new.Num)
		from_clerk.DeleteShard(move.tshid, new.Num)
	}

	shardConfigString := new.String()
	err := sck.IKVClerk.Put(sck.key, shardConfigString, rpc.Tversion(new.Num-1))

	if err != rpc.OK {
		fmt.Printf("something went wrong when putting the config...\n")
	} else {
		fmt.Printf("prev config %d: %s\n", old.Num, old.String())
		fmt.Printf("put config %d: %s\n", new.Num, shardConfigString)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// sck.mu.Lock()
	// defer sck.mu.Unlock()
	config, _, _ := sck.IKVClerk.Get(sck.key)

	return shardcfg.FromString(config)
}
