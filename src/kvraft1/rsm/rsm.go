package rsm

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Me  int
	Id  int
	Req any
}

type OpInfo struct {
	Index int
	Term  int
}

type RaftStatus struct {
	Term   int
	Leader bool
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	waiting_submits             map[int]chan any
	waiting_ops                 map[int]OpInfo
	most_recent_committed_index int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:              me,
		maxraftstate:    maxraftstate,
		applyCh:         make(chan raftapi.ApplyMsg),
		sm:              sm,
		waiting_submits: make(map[int]chan any),
		waiting_ops:     make(map[int]OpInfo),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.ListenForCommits(me, rsm.applyCh)
	go rsm.PruneWaitingSubmits()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) PruneWaitingSubmits() {
	for {
		rsm.mu.Lock()

		term, _ := rsm.rf.GetState()
		for key, waiting_op := range rsm.waiting_ops {
			if waiting_op.Index <= rsm.most_recent_committed_index || waiting_op.Term < term {
				fmt.Printf("\t%d PRUNING op\n", rsm.me)
				rsm.waiting_submits[key] <- -1
				rsm.ClearWaitingSubmit(key)
			}
		}

		rsm.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rsm *RSM) ListenForCommits(me int, ch chan raftapi.ApplyMsg) {
	for commit := range ch {
		op := commit.Command.(Op)
		op_index := commit.CommandIndex
		op_term := commit.CommandTerm
		res := rsm.sm.DoOp(op.Req) // do the operation

		rsm.mu.Lock()
		fmt.Printf("\t%d processing op %d with index %d term %d\n", rsm.me, op.Id, op_index, op_term)

		submit_ch, ok2 := rsm.waiting_submits[op.Id]
		if ok2 && submit_ch != nil {
			submit_ch <- res
			rsm.ClearWaitingSubmit(op.Id)
		}

		rsm.most_recent_committed_index = op_index

		rsm.mu.Unlock()
	}
	fmt.Printf("%d applyCh closed\n", rsm.me)
}

func (rsm *RSM) ClearWaitingSubmit(id int) {
	delete(rsm.waiting_submits, id) // remove value from map
	delete(rsm.waiting_ops, id)
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	op_id := rand.Int()
	op := Op{
		Me:  rsm.me,
		Id:  op_id,
		Req: req,
	}
	index, term, isLeader := rsm.rf.Start(op)

	op_info := OpInfo{
		Index: index,
		Term:  term,
	}

	wait_channel := make(chan any)
	if isLeader {
		fmt.Printf("%d submitting op %d\n", rsm.me, op_id)
		rsm.waiting_submits[op_id] = wait_channel
		rsm.waiting_ops[op_id] = op_info
	}
	rsm.mu.Unlock()

	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	res := <-wait_channel
	if res == -1 {
		return rpc.ErrWrongLeader, nil
	}
	// fmt.Printf("\t\t\t%d RETURNING op %d\n", rsm.me, op.Id)
	return rpc.OK, res
}
