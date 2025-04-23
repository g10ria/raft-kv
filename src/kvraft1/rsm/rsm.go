package rsm

import (
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
	Id  int64
	Req any // message value
}

type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// stores results of submitted messages
type SubmitMsg struct {
	ok     bool
	result any
}

// stores pending messages
type PendingSubmit struct {
	index    int
	term     int
	id       int64
	submitCh chan SubmitMsg
}

type RSM struct {
	mu            sync.Mutex
	me            int
	rf            raftapi.Raft
	applyCh       chan raftapi.ApplyMsg
	maxraftstate  int // snapshot if log grows this big
	sm            StateMachine
	pendings      []PendingSubmit
	channelClosed bool
	lastApplied   int
}

var snapshotThreshold = 0.8
var checkForSnapshotFreq = 10

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendings:     make([]PendingSubmit, 0),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	go rsm.applyChannelWatcher()
	go rsm.checkIfSnapshot()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Reads all applied messages and funnels them to the correct pending message
func (rsm *RSM) applyChannelWatcher() {
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
		if rsm.maxraftstate != -1 && count == checkForSnapshotFreq {
			count = 0
			if rsm.shouldSnapshot() {
				rsm.rf.Snapshot(applyMsg.CommandIndex, rsm.sm.Snapshot())
			}
		}

		if len(rsm.pendings) != 0 {
			mostRecentPending := rsm.pendings[0]

			if applyMsg.CommandIndex == mostRecentPending.index {
				if mostRecentPending.id == op.Id && rsm.me == op.Me {
					mostRecentPending.submitCh <- SubmitMsg{ok: true, result: result}
					rsm.pendings = rsm.pendings[1:]
				} else {
					rsm.killAllPendings()
				}
			} else {
				if applyMsg.CommandTerm > mostRecentPending.term {
					rsm.killAllPendings()
				}
			}
		}
		rsm.mu.Unlock()
	}

	rsm.channelClosed = true

	rsm.mu.Lock()
	rsm.killAllPendings()
	rsm.mu.Unlock()
}

func (rsm *RSM) killAllPendings() {
	for _, pending := range rsm.pendings {
		pending.submitCh <- SubmitMsg{ok: false, result: nil}
	}
	rsm.pendings = make([]PendingSubmit, 0)
}

func (rsm *RSM) checkIfSnapshot() {
	// Periodically checks for snapshots and prunes the pending array if our term is too high
	for {
		rsm.mu.Lock()

		if len(rsm.pendings) != 0 {
			pending := rsm.pendings[0]
			term, _ := rsm.rf.GetState()

			if term > pending.term {
				rsm.killAllPendings()
			}

			if rsm.maxraftstate != -1 {
				if rsm.shouldSnapshot() {
					rsm.rf.Snapshot(rsm.lastApplied, rsm.sm.Snapshot())
				}
			}
		}

		rsm.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
	}
}

func (rsm *RSM) shouldSnapshot() bool {
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
	pending := PendingSubmit{
		index:    index,
		term:     term,
		id:       id,
		submitCh: make(chan SubmitMsg),
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
