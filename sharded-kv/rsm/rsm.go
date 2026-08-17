package rsm

import (
	"math/rand"
	"sync"
	"time"

	"raft-kv/simulated-rpc/rpc"
	"raft-kv/simulated-rpc"
	raft "raft-kv/raft"
	"raft-kv/raft/raftapi"
	tester "raft-kv/testers/testkit"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Me  int
	Id  int64
	Req any // message value
}

type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// stores results of submitted messages
type opResult struct {
	ok     bool
	result any
}

// stores pending messages
type pendingOp struct {
	index    int
	term     int
	id       int64
	submitCh chan opResult
}

type RSM struct {
	mu            sync.Mutex
	me            int
	rf            raftapi.Raft
	applyCh       chan raftapi.ApplyMsg
	maxraftstate  int // snapshot if log grows this big
	sm            StateMachine
	pendings      []pendingOp
	channelClosed bool
	lastApplied   int
}

var snapshotThreshold = 0.8
var snapshotCheckInterval = 10

func MakeRSM(servers []*simulatedrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendings:     make([]pendingOp, 0),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	go rsm.applyLoop()
	go rsm.snapshotLoop()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Reads all applied messages and funnels them to the correct pending message
func (rsm *RSM) applyLoop() {
	count := 0
	for applyMsg := range rsm.applyCh {
		rsm.mu.Lock()

		if !applyMsg.CommandValid {
			rsm.sm.Restore(applyMsg.Snapshot)
			rsm.mu.Unlock()
			continue
		}

		count++
		op := applyMsg.Command.(Op)
		result := rsm.sm.DoOp(op.Req)

		rsm.lastApplied = applyMsg.CommandIndex
		if rsm.maxraftstate != -1 && count == snapshotCheckInterval {
			count = 0
			if rsm.needsSnapshot() {
				rsm.rf.Snapshot(applyMsg.CommandIndex, rsm.sm.Snapshot())
			}
		}

		if len(rsm.pendings) != 0 {
			oldestPending := rsm.pendings[0]

			if applyMsg.CommandIndex == oldestPending.index {
				if oldestPending.id == op.Id && rsm.me == op.Me {
					oldestPending.submitCh <- opResult{ok: true, result: result}
					rsm.pendings = rsm.pendings[1:]
				} else {
					rsm.abortPending()
				}
			} else {
				if applyMsg.CommandTerm > oldestPending.term {
					rsm.abortPending()
				}
			}
		}
		rsm.mu.Unlock()
	}

	rsm.channelClosed = true

	rsm.mu.Lock()
	rsm.abortPending()
	rsm.mu.Unlock()
}

func (rsm *RSM) abortPending() {
	for _, pending := range rsm.pendings {
		pending.submitCh <- opResult{ok: false, result: nil}
	}
	rsm.pendings = make([]pendingOp, 0)
}

func (rsm *RSM) snapshotLoop() {
	// Periodically checks for snapshots and prunes the pending array if our term is too high
	for {
		rsm.mu.Lock()

		if len(rsm.pendings) != 0 {
			pending := rsm.pendings[0]
			term, _ := rsm.rf.GetState()

			if term > pending.term {
				rsm.abortPending()
			}

			if rsm.maxraftstate != -1 {
				if rsm.needsSnapshot() {
					rsm.rf.Snapshot(rsm.lastApplied, rsm.sm.Snapshot())
				}
			}
		}

		rsm.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
	}
}

func (rsm *RSM) needsSnapshot() bool {
	return float64(rsm.rf.PersistBytes()) > float64(rsm.maxraftstate)*snapshotThreshold
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()

	id := rand.Int63() // generate random ID
	op := Op{Me: rsm.me, Id: id, Req: req}
	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader || rsm.channelClosed {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// Create the pending message and add to pending array
	pending := pendingOp{
		index:    index,
		term:     term,
		id:       id,
		submitCh: make(chan opResult),
	}
	rsm.pendings = append(rsm.pendings, pending)
	rsm.mu.Unlock()

	// Wait for submit result to come in
	submitMsg := <-pending.submitCh

	if submitMsg.ok {
		return rpc.OK, submitMsg.result
	}
	return rpc.ErrWrongLeader, nil
}
