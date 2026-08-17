package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"sync"

	kvsrv "raft-kv/sharded-kv/shardctrler/kvsrv"
	"raft-kv/simulated-rpc/rpc"
	kvtest "raft-kv/testers/kvtest"
	"raft-kv/sharded-kv/shardcfg"
	"raft-kv/sharded-kv/shardgrp"
	tester "raft-kv/testers/testkit"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	id string

	killed    int32 // set by Kill()
	key       string
	nextKey   string
	nextKeyID string

	clerks map[tester.Tgid]*shardgrp.Clerk

	mu sync.Mutex
}

// Make a ShardCtrler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.key = "CONFIG"
	sck.nextKey = "NEXT_CONFIG"
	sck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)

	sck.id = kvtest.RandValue(8) // generate random ID string

	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	fmt.Printf("\nNEW CONTROLLER [%s] init\n", sck.id)

	currConfig, _, _ := sck.IKVClerk.Get(sck.key)
	nextConfig, _, nextErr := sck.IKVClerk.Get(sck.nextKey)

	if nextErr == rpc.ErrNoKey {
		// note: this never happens now
	} else {
		curr := shardcfg.FromString(currConfig)
		next := shardcfg.FromString(nextConfig)

		if next.Num > curr.Num {
			fmt.Printf("controller [%s] continuing interrupted config %d\n", sck.id, next.Num)
			// sck.ChangeConfigTo(next)
			sck.applyConfigChange(next, false) // should always try
		}
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	shardConfigString := cfg.String()
	sck.IKVClerk.Put(sck.key, shardConfigString, rpc.Tversion(cfg.Num-1))
	sck.IKVClerk.Put(sck.nextKey, shardConfigString, rpc.Tversion(cfg.Num-1))

	sck.IKVClerk.Put(sck.nextKeyID, sck.id, rpc.Tversion(cfg.Num-1))

	fmt.Printf("put %s\n", shardConfigString)
}

type ShardMove struct {
	tshid shardcfg.Tshid
	from  tester.Tgid
	to    tester.Tgid
}

func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	sck.applyConfigChange(new, true)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) applyConfigChange(new *shardcfg.ShardConfig, checkForConcurrentControllers bool) {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	nextConfigString := new.String()

	if checkForConcurrentControllers {
		currConfig, _, _ := sck.IKVClerk.Get(sck.key)
		nextConfig, _, _ := sck.IKVClerk.Get(sck.nextKey)
		curr := shardcfg.FromString(currConfig)
		next := shardcfg.FromString(nextConfig)
		if curr.Num == next.Num {
			// nothing in progress so we're good
		} else {
			// a reconfig is already in progress; only proceed if we're the one driving it
			nextOwnerID, _, _ := sck.IKVClerk.Get(sck.nextKeyID)
			if nextOwnerID != sck.id {

				fmt.Printf("controller [%s] is too slow, returning\n", sck.id)
				return // do nothing
			}
		}

		// check if put was successful; someone might have beaten them to it in the meantime
		putErr := sck.IKVClerk.Put(sck.nextKey, nextConfigString, rpc.Tversion(new.Num-1))
		sck.IKVClerk.Put(sck.nextKeyID, sck.id, rpc.Tversion(new.Num-1))

		if putErr != rpc.OK {
			fmt.Printf("controller [%s] is too slow LATER, returning\n", sck.id)
			return
		}
	}

	fmt.Printf("\ncontroller [%s] started updating config to %d: %s\n", sck.id, new.Num, nextConfigString)

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

	// using cached clerks
	for _, move := range shardMoves {
		fmt.Printf("CONTROLLER [%s] EXECUTING SHARDMOVE shard %d from grp%d to grp%d\n", sck.id, move.tshid, move.from, move.to)
		from := move.from
		to := move.to

		fromClerk := sck.getOrMakeClerk(from)
		toClerk := shardgrp.MakeClerk(sck.clnt, new.Groups[to])

		state, err := fromClerk.FreezeShard(move.tshid, new.Num)
		for err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			// fromClerk = sck.newClerkForGroup(from)
			state, err = fromClerk.FreezeShard(move.tshid, new.Num)
		}

		err = toClerk.InstallShard(move.tshid, state, new.Num)
		for err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			// toClerk = sck.newClerkForGroup(to)
			err = toClerk.InstallShard(move.tshid, state, new.Num)
		}

		err = fromClerk.DeleteShard(move.tshid, new.Num)
		for err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			// fromClerk = sck.newClerkForGroup(from)
			err = fromClerk.DeleteShard(move.tshid, new.Num)
		}
	}

	fmt.Printf("\nCONTROLLER [%s] finished updating config to %d: %s\n", sck.id, new.Num, nextConfigString)

	sck.IKVClerk.Put(sck.key, nextConfigString, rpc.Tversion(new.Num-1))
}

func (sck *ShardCtrler) getOrMakeClerk(groupID tester.Tgid) *shardgrp.Clerk {
	clerk, ok := sck.clerks[groupID]
	if ok {
		return clerk
	} else {
		return sck.newClerkForGroup(groupID)
	}
}

func (sck *ShardCtrler) newClerkForGroup(groupID tester.Tgid) *shardgrp.Clerk {
	config := sck.Query()
	sck.clerks[groupID] = shardgrp.MakeClerk(sck.clnt, config.Groups[groupID])
	return sck.clerks[groupID]
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config, _, _ := sck.IKVClerk.Get(sck.key)
	return shardcfg.FromString(config)
}
