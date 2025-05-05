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

	id string

	killed   int32 // set by Kill()
	key      string
	next_key string

	next_key_ID string

	clerks map[tester.Tgid]*shardgrp.Clerk

	mu sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.key = "CONFIG"
	sck.next_key = "NEXT_CONFIG"
	sck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)

	sck.id = kvtest.RandValue(8) // generate random ID string

	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	fmt.Printf("\nNEW CONTROLLER [%s] init\n", sck.id)

	curr_config, _, _ := sck.IKVClerk.Get(sck.key)
	next_config, _, next_err := sck.IKVClerk.Get(sck.next_key)

	if next_err == rpc.ErrNoKey {
		// note: this never happens now
	} else {
		curr := shardcfg.FromString(curr_config)
		next := shardcfg.FromString(next_config)

		if next.Num > curr.Num {
			fmt.Printf("controller [%s] continuing interrupted config %d\n", sck.id, next.Num)
			// sck.ChangeConfigTo(next)
			sck.ChangeConfigToHelper(next, false) // should always try
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
	sck.IKVClerk.Put(sck.next_key, shardConfigString, rpc.Tversion(cfg.Num-1))

	sck.IKVClerk.Put(sck.next_key_ID, sck.id, rpc.Tversion(cfg.Num-1))

	fmt.Printf("put %s\n", shardConfigString)
}

type ShardMove struct {
	tshid shardcfg.Tshid
	from  tester.Tgid
	to    tester.Tgid
}

func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	sck.ChangeConfigToHelper(new, true)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigToHelper(new *shardcfg.ShardConfig, check_for_concurrent_controllers bool) {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	nextConfigString := new.String()

	if check_for_concurrent_controllers {
		curr_config, _, _ := sck.IKVClerk.Get(sck.key)
		next_config, _, _ := sck.IKVClerk.Get(sck.next_key)
		curr := shardcfg.FromString(curr_config)
		next := shardcfg.FromString(next_config)
		if curr.Num == next.Num {
			// nothing in progress so we're good
		} else {
			// check ID? check if it's equal (i mean it never should be)

			next_doer_id, _, _ := sck.IKVClerk.Get(sck.next_key_ID)
			if next_doer_id != sck.id {

				fmt.Printf("controller [%s] is too slow, returning\n", sck.id)
				return // do nothing
			}
		}

		// check if put was successful; someone might have beaten them to it in the meantime
		put_err := sck.IKVClerk.Put(sck.next_key, nextConfigString, rpc.Tversion(new.Num-1))
		sck.IKVClerk.Put(sck.next_key_ID, sck.id, rpc.Tversion(new.Num-1))

		if put_err != rpc.OK {
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

		from_clerk := sck.MakeOptionalAndGetClerk(from)
		to_clerk := shardgrp.MakeClerk(sck.clnt, new.Groups[to])

		// possible todo: handle errors here properly
		state, err := from_clerk.FreezeShard(move.tshid, new.Num)
		for err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			// from_clerk = sck.MakeRequiredAndGetClerk(from)
			state, err = from_clerk.FreezeShard(move.tshid, new.Num)
		}

		err = to_clerk.InstallShard(move.tshid, state, new.Num)
		for err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			// to_clerk = sck.MakeRequiredAndGetClerk(to)
			err = to_clerk.InstallShard(move.tshid, state, new.Num)
		}

		err = from_clerk.DeleteShard(move.tshid, new.Num)
		for err == rpc.ErrWrongGroup { // if this is the wrong group, re-query the config and change again
			// from_clerk = sck.MakeRequiredAndGetClerk(from)
			err = from_clerk.DeleteShard(move.tshid, new.Num)
		}
	}

	fmt.Printf("\nCONTROLLER [%s] finished updating config to %d: %s\n", sck.id, new.Num, nextConfigString)

	sck.IKVClerk.Put(sck.key, nextConfigString, rpc.Tversion(new.Num-1))
}

func (sck *ShardCtrler) MakeOptionalAndGetClerk(group_id tester.Tgid) *shardgrp.Clerk {
	clerk, ok := sck.clerks[group_id]
	if ok {
		return clerk
	} else {
		return sck.MakeRequiredAndGetClerk(group_id)
	}
}

func (sck *ShardCtrler) MakeRequiredAndGetClerk(group_id tester.Tgid) *shardgrp.Clerk {
	config := sck.Query()
	sck.clerks[group_id] = shardgrp.MakeClerk(sck.clnt, config.Groups[group_id])
	return sck.clerks[group_id]
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config, _, _ := sck.IKVClerk.Get(sck.key)
	return shardcfg.FromString(config)
}
