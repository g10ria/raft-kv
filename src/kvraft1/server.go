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

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	KVMap map[string]Value
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req := req.(type) {
	case rpc.GetArgs:
		val, ok := kv.KVMap[req.Key]
		if !ok {
			return rpc.GetReply{
				Value:   "",
				Version: 0,
				Err:     rpc.ErrNoKey,
			}
		} else {
			return rpc.GetReply{
				Value:   val.Value,
				Version: val.Version,
				Err:     rpc.OK,
			}
		}
	case rpc.PutArgs:
		val, ok := kv.KVMap[req.Key]
		if !ok {
			if req.Version > 0 {
				return rpc.PutReply{
					Err: rpc.ErrNoKey,
				}
			} else {
				kv.KVMap[req.Key] = Value{
					Value:   req.Value,
					Version: 1,
				}
				return rpc.PutReply{
					Err: rpc.OK,
				}
			}
		} else {
			if req.Version == val.Version {
				kv.KVMap[req.Key] = Value{
					Value:   req.Value,
					Version: val.Version + 1,
				}
				return rpc.PutReply{
					Err: rpc.OK,
				}
			} else {
				return rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			}
		}
	default:
		println("WHATTTTT")
	}

	println("REACHED END")
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.KVMap)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newmap map[string]Value
	if d.Decode(&newmap) != nil {
		println("yikes!")
	} else {
		kv.KVMap = newmap
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		reply.Value = res.(rpc.GetReply).Value
		reply.Version = res.(rpc.GetReply).Version
		reply.Err = res.(rpc.GetReply).Err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		reply.Err = res.(rpc.PutReply).Err
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
	// Your code here, if desired.
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

	kv := &KVServer{me: me}

	kv.KVMap = make(map[string]Value)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
