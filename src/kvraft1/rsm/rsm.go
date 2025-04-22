// rsm, changed broken

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

	pending_ch map[int]chan any
	idx_id_map map[int][]int
	id_idx_map map[int]int
	close_ch   chan int
	closeOnce  sync.Once
	term       int
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
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,

		pending_ch: make(map[int]chan any),
		idx_id_map: make(map[int][]int),
		id_idx_map: make(map[int]int),
		close_ch:   make(chan int),
		closeOnce:  sync.Once{},
	}

	go rsm.Submit_reader_helper()

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) cleanup(id int) {
	rsm.mu.Lock()
	delete(rsm.pending_ch, id)
	rsm.mu.Unlock()
}

type RaftState struct {
	term   int
	leader bool
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	id := rand.Int()
	op := Op{Me: rsm.me, Id: id, Req: req}

	index, term, isLeader := rsm.rf.Start(op)

	fmt.Printf("%d client submitting op %d\n", rsm.me, id)

	rsm.mu.Lock()
	rsm.term = term
	rsm.mu.Unlock()
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	pending_ch := make(chan any)

	rsm.mu.Lock()
	rsm.pending_ch[id] = pending_ch
	rsm.idx_id_map[index] = append(rsm.idx_id_map[index], id)
	rsm.id_idx_map[id] = index
	rsm.mu.Unlock()

	termCh := make(chan RaftState)
	for {
		go func() {
			curterm, thinks_leader := rsm.rf.GetState()
			termCh <- RaftState{term: curterm, leader: thinks_leader}
		}()

		select {
		case message := <-termCh:
			if message.term != term || !message.leader {
				rsm.cleanup(id)
				return rpc.ErrWrongLeader, nil
			}
		case result := <-pending_ch:
			if result == -1 {
				rsm.cleanup(id)
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result
		case <-rsm.close_ch:
			rsm.cleanup(id)
			return rpc.ErrWrongLeader, nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rsm *RSM) Submit_reader_helper() {
	for message := range rsm.applyCh {
		op := message.Command.(Op)
		result := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()

		// notify
		if pending_ch, ok := rsm.pending_ch[op.Id]; ok {
			pending_ch <- result
			delete(rsm.pending_ch, op.Id)
		}

		// reject other requests
		index := rsm.id_idx_map[op.Id]
		for _, other_pending_ids := range rsm.idx_id_map[index] {
			if other_pending_ids != op.Id {
				if pending_ch, ok := rsm.pending_ch[other_pending_ids]; ok {
					pending_ch <- -1
					delete(rsm.pending_ch, other_pending_ids)
				}
			}
		}

		// cleanup
		delete(rsm.id_idx_map, op.Id)
		delete(rsm.idx_id_map, index)

		rsm.mu.Unlock()
	}

	rsm.closeOnce.Do(func() {
		close(rsm.close_ch)
	})
}
