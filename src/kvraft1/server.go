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

type ValueTuple struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me     int
	dead   int32
	rsm    *rsm.RSM
	mu     sync.Mutex
	values map[string]ValueTuple // maps from a key to a value and version
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.PutArgs:
		val, ok := kv.values[args.Key]
		// perform the put if key exists and versions match, otherwise return an error
		if ok {
			if val.Version == args.Version {
				kv.values[args.Key] = ValueTuple{args.Value, val.Version + 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrVersion}
		} else if args.Version == 0 {
			// creating a new value
			kv.values[args.Key] = ValueTuple{args.Value, 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	case rpc.GetArgs:
		val, ok := kv.values[args.Key]
		// return the value and version if existing, otherwise return ErrNoKey
		if ok {
			return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		}
		return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.values)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var values map[string]ValueTuple
	if d.Decode(&values) == nil {
		kv.values = values
	}
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

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rsm_reply := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		rsm_reply_cast := rsm_reply.(rpc.PutReply)
		reply.Err = rsm_reply_cast.Err
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	kv := &KVServer{me: me, values: make(map[string]ValueTuple)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
