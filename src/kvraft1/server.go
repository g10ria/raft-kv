package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValTup struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu  sync.Mutex
	dct map[string]ValTup
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:

		if val, ok := kv.dct[args.Key]; ok {
			return rpc.GetReply{
				Value:   val.Value,
				Version: val.Version,
				Err:     rpc.OK,
			}
		} else {
			return rpc.GetReply{
				Value:   "",
				Version: 0,
				Err:     rpc.ErrNoKey,
			}
		}
	case rpc.PutArgs:
		if val, ok := kv.dct[args.Key]; ok {
			if val.Version == args.Version {
				kv.dct[args.Key] = ValTup{args.Value, val.Version + 1}
				return rpc.PutReply{Err: rpc.OK}
			} else {
				return rpc.PutReply{Err: rpc.ErrVersion}
			}
		} else {
			if args.Version == 0 {
				kv.dct[args.Key] = ValTup{args.Value, 1}
				return rpc.PutReply{Err: rpc.OK}
			} else {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
		}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.dct)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var dct map[string]ValTup
	if d.Decode(&dct) != nil {
		println("failed")
	} else {
		kv.dct = dct
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		repl := rep.(rpc.GetReply)
		reply.Value = repl.Value
		reply.Version = repl.Version
		reply.Err = repl.Err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		repl := rep.(rpc.PutReply)
		reply.Err = repl.Err
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, dct: make(map[string]ValTup)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
