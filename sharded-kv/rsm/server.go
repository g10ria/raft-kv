package rsm

import (
	"bytes"
	"log"
	"sync"

	"encoding/gob"
	"raft-kv/simulated-rpc"
	"raft-kv/raft/raftapi"
	"raft-kv/testers/testkit"
)

type Inc struct {
}

type IncRep struct {
	N int
}

type Null struct {
}

type NullRep struct {
}

type Dec struct {
}

type rsmSrv struct {
	ts      *Test
	me      int
	rsm     *RSM
	mu      sync.Mutex
	counter int
}

func makeRsmSrv(ts *Test, srv int, ends []*simulatedrpc.ClientEnd, persister *tester.Persister, snapshot bool) *rsmSrv {
	//log.Printf("mksrv %d", srv)
	gob.Register(Op{})
	gob.Register(Inc{})
	gob.Register(IncRep{})
	gob.Register(Null{})
	gob.Register(NullRep{})
	gob.Register(Dec{})
	s := &rsmSrv{
		ts: ts,
		me: srv,
	}
	s.rsm = MakeRSM(ends, srv, persister, ts.maxraftstate, s)
	return s
}

func (rs *rsmSrv) DoOp(req any) any {
	//log.Printf("%d: DoOp: %T(%v)", rs.me, req, req)
	switch req.(type) {
	case Inc:
		rs.mu.Lock()
		rs.counter += 1
		rs.mu.Unlock()
		return &IncRep{rs.counter}
	case Null:
		return &NullRep{}
	default:
		// wrong type! expecting an Inc.
		log.Fatalf("DoOp should execute only Inc and not %T", req)
	}
	return nil
}

func (rs *rsmSrv) Snapshot() []byte {
	//log.Printf("%d: snapshot", rs.me)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rs.counter)
	return w.Bytes()
}

func (rs *rsmSrv) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if d.Decode(&rs.counter) != nil {
		log.Fatalf("%v couldn't decode counter", rs.me)
	}
	//log.Printf("%d: restore %d", rs.me, rs.counter)
}

func (rs *rsmSrv) Kill() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	//log.Printf("kill %d", rs.me)
	//rs.rsm.Kill()
	rs.rsm = nil
}

func (rs *rsmSrv) Raft() raftapi.Raft {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.rsm.Raft()
}
