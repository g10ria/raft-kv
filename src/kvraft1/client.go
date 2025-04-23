package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	mu      sync.Mutex
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Repeatedly sends the Get request
	// If the leader is wrong, cycle to the next one and try again
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
		ck.mu.Unlock()
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	leader := ck.leader
	ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

	// return same error if it wasn't wrong leader (we should try again in that case)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}

	// otherwise, keep attempting to submit again (to different leaders)
	for {
		ck.mu.Lock()
		if ck.leader == leader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)

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
	}
}
