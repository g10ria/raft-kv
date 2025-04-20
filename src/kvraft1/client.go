package kvraft

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
	mu      sync.Mutex
	// You will have to modify this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}
	// You'll have to add code here.
	return ck
}

var DEBUG = false

func (ck *Clerk) debug(s ...interface{}) {
	if DEBUG {
		fmt.Println(s...)
	}
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	defer ck.debug("return from get")
	ck.debug("called get with key", key)
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}

		l := ck.leader
		ck.debug("sending get to leader", l)
		ok := ck.clnt.Call(ck.servers[l], "KVServer.Get", &args, &reply)
		ck.debug("received response from get", l)
		if !ok || (ok && reply.Err == rpc.ErrWrongLeader) {
			ck.debug("get lock acq")
			ck.mu.Lock()
			if l == ck.leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.mu.Unlock()
			// time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Value, reply.Version, reply.Err
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
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	defer ck.debug("return from put")

	ck.debug("called put with key", key, "| value", value, "| version", version)
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}

	l := ck.leader
	ck.debug("sending put to leader", l)

	ok := ck.clnt.Call(ck.servers[l], "KVServer.Put", &args, &reply)
	ck.debug("received response from put", l)

	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	} else {
		ck.mu.Lock()
		ck.debug("put lock acq")
		if l == ck.leader { // if ck.leader has already been changed, just try again with the new leader
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		ck.mu.Unlock()
	}

	// Dropped message case
	for {
		time.Sleep(100 * time.Millisecond)

		reply := rpc.PutReply{}
		l := ck.leader
		ck.debug("retrying put to leader", l)
		ok := ck.clnt.Call(ck.servers[l], "KVServer.Put", &args, &reply)
		ck.debug("received response from put", l)

		if !ok || (ok && reply.Err == rpc.ErrWrongLeader) {
			ck.mu.Lock()
			ck.debug("put lock acq")
			if l == ck.leader { // if ck.leader has already been changed, just try again with the new leader
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.mu.Unlock()
			continue
		} // try again

		switch reply.Err {
		case rpc.OK:
			return rpc.OK
		case rpc.ErrVersion:
			return rpc.ErrMaybe
		}
	}
}
