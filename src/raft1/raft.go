package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"slices"

	//	"6.5840/labgob"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const DEBUG = true
func LogDebugln(str ...interface{}) {
	if DEBUG {
		fmt.Println(str...)
		return
	}
}
func LogDebugf(str string, params ...any) {
	if DEBUG {
		fmt.Printf(str, params...)
		return
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	votesNeededToWin int
	persister        *tester.Persister // Object to hold this peer's persisted state
	me               int               // this peer's index into peers[]
	dead             int32             // set by Kill()
	currentTerm      int               // last term the server saw (initialize at 0)
	votedFor         int               // index of last voted-for peer (-1 if none)
	logEntries       []interface{}     // log entry data
	logTermsReceived []int             // term when each log entry was receieved
	commitIndex      int               // index of highest log entry known to be committed
	lastApplied      int               // index of highest log entry known to be applied
	nextIndex        []int             // for each server, index of next log entry to send to that server
	matchIndex       []int             // for each server, index of highest log entry known to be replicated on server
	believesLeader   bool              // if the server believes it is the leader
	heartbeatTimer 	 time.Time
	numVotesReceived int
	applyCh 		 chan raftapi.ApplyMsg
	isCandidate		 bool
}

type RequestVoteArgs struct {
	Term           int
	CandidateIndex int
	LastLogIndex   int
	LastLogTerm    int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term  				int
	LeaderId 			int 
	PrevLogIndex 		int
	PrevLogTerm 		int 
	LogEntries 			[]interface{}
	LogTermsReceived	[]int
	LeaderCommit		int		// leader's commit index
}

type AppendEntriesReply struct {
	Term				int 
	Success				bool
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.believesLeader
	rf.mu.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) startElection() {
	// Staggered election start times
	ms := (rand.Int63() % 50) // 20 ms
	time.Sleep(time.Duration(ms) * time.Millisecond)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	LogDebugf("\n[%d STARTING ELECTION for term %d]\n", rf.me, rf.currentTerm)

	rf.votedFor = rf.me // vote for self
	rf.numVotesReceived = 1

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateIndex = rf.me

	// get last element from logs arrays
	args.LastLogIndex = len(rf.logEntries) - 1
	args.LastLogTerm = rf.logTermsReceived[len(rf.logTermsReceived)-1]

	for index, _ := range rf.peers {
		if index != rf.me {
			reply := RequestVoteReply{}
			// LogDebugf("\t%d requesting vote from %d\n", rf.me, index)
			go rf.sendRequestVote(index, &args, &reply)
		}
	}
}

func (rf *Raft) resetElectionTimeout() {
	ms := 600 + (rand.Int63() % 650)
	now := time.Now()
	candidateTimer := now.Add(time.Duration(ms) * time.Millisecond)

	if candidateTimer.After(rf.heartbeatTimer) {
		rf.heartbeatTimer = candidateTimer
	}
}

// Responds to request vote RPCs
// give vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Received message so update heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = args.Term
	reply.VoteGranted = false

	myLastLogIndex := len(rf.logEntries) - 1
	myLastLogTerm := rf.logTermsReceived[len(rf.logTermsReceived)-1]
	candidateLogUpToDate := args.LastLogIndex >= myLastLogIndex && args.LastLogTerm >= myLastLogTerm

	// Check if the candidate term is updated
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			LogDebugf("\t\t%d updating term from %d to %d\n", rf.me, rf.currentTerm, args.Term)
			rf.believesLeader = false
			rf.currentTerm = args.Term
			rf.isCandidate = false
			rf.votedFor = -1
		}
		LogDebugf("1")
		if rf.votedFor == -1 || rf.votedFor == args.CandidateIndex {
			LogDebugf("2")
			if candidateLogUpToDate {
				LogDebugf("3")
				// Then grant the vote
				reply.VoteGranted = true
				rf.votedFor = args.CandidateIndex
			}
		}
	}

	if reply.VoteGranted {
		rf.resetElectionTimeout()
		LogDebugf("\t\t%d (term %d) says YES to %d (term %d)\n", rf.me, rf.currentTerm, args.CandidateIndex, args.Term)
	} else {
		LogDebugf("\t\t%d (term %d) says NO to %d (term %d)\n", rf.me, rf.currentTerm, args.CandidateIndex, args.Term)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.believesLeader && reply.VoteGranted && args.Term == rf.currentTerm {
		rf.numVotesReceived += 1
		if rf.numVotesReceived >= rf.votesNeededToWin {
			rf.believesLeader = true
			LogDebugf("\t%d becomes leader for term %d\n", rf.me, reply.Term)
			
			for i:=0;i<len(rf.peers);i++ {
				if i != rf.me {
					rf.nextIndex[i] = max(len(rf.logEntries), 1)
					rf.matchIndex[i] = 0
				}
			}
		}
		// LogDebugf("\t%d received vote", rf.me)
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
	}
	return ok
}

func (rf *Raft) AppendToPeer(command interface{}, peer int) () {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	nextIndexToSend := rf.nextIndex[peer]
	lastLogIndex := len(rf.logEntries) - 1 // includes the newest command

	if nextIndexToSend < lastLogIndex {
		// send starting at nextIndex
		args.PrevLogIndex = nextIndexToSend-1
		args.PrevLogTerm = rf.logTermsReceived[nextIndexToSend-1]

		args.LogEntries = rf.logEntries[nextIndexToSend:len(rf.logEntries)]
		args.LogTermsReceived = rf.logTermsReceived[nextIndexToSend:len(rf.logEntries)]

		LogDebugf("%d sending APPEND from index %d-%d to %d\n", rf.me, nextIndexToSend, lastLogIndex, peer)
	} else {
		// just send the newest command
		args.PrevLogIndex = lastLogIndex-1
		args.PrevLogTerm = rf.logTermsReceived[lastLogIndex-1]

		args.LogEntries = make([]interface{}, 1)
		args.LogEntries[0] = command 
		args.LogTermsReceived = make([]int, 1)
		args.LogTermsReceived[0] = rf.currentTerm 

		LogDebugf("%d sending APPEND at index %d to %d\n", rf.me, lastLogIndex, peer)
	}

	args.LeaderCommit = rf.commitIndex

	reply := AppendEntriesReply{}
	go rf.sendAppendEntries(peer, &args, &reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.believesLeader
	// Issue append entries commands to every peer, if leader
	if isLeader {
		rf.logEntries = append(rf.logEntries, command)
		rf.logTermsReceived = append(rf.logTermsReceived, rf.currentTerm)
		for peer, _ := range rf.peers {
			if peer != rf.me {
				go rf.AppendToPeer(command, peer)
			}
		}
	}

	index := len(rf.logEntries)-1
	term := rf.currentTerm

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyCommands() {
	lastApplied := 0

	for !rf.killed() {
		time.Sleep(30 * time.Millisecond)

		rf.mu.Lock()
		
		if rf.commitIndex > lastApplied {
			lastApplied += 1

			// LogDebugf("\t\t\t%d applying %d\n", rf.me, lastApplied)
			
			applyMsg := raftapi.ApplyMsg{}
			applyMsg.CommandValid = true 
			applyMsg.Command = rf.logEntries[lastApplied]
			applyMsg.CommandIndex = lastApplied
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // always sleep

		rf.mu.Lock()

		electionTimerRanOut := time.Now().After(rf.heartbeatTimer)
		
		if !rf.believesLeader && electionTimerRanOut && rf.votedFor == -1 {
			rf.resetElectionTimeout()
			rf.isCandidate = true
			go rf.startElection()
		} else if electionTimerRanOut { // reset so that we can vote again
			rf.votedFor = -1
		}

		rf.mu.Unlock()
	}
}

// receiveappend
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isHeartbeat := len(args.LogEntries) == 0

	// if len(args.LogEntries) != 0 {
	// 	LogDebugf("\t%d received APPEND at index %d from %d\n", rf.me, args.PrevLogIndex+1, args.LeaderId)
	// }

	reply.Term = rf.currentTerm 

	if args.Term < rf.currentTerm {
		reply.Success = false
		LogDebugf("\t%d ignoring APPEND from %d, terms %d %d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		return
	} // Assume args.Term >= rf.currentTerm below this line

	rf.resetElectionTimeout()

	if rf.commitIndex < args.LeaderCommit && isHeartbeat {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logEntries)-1)
		if rf.commitIndex == args.LeaderCommit {
			LogDebugf("%d's commit index is now %d yay\n", rf.me, rf.commitIndex)
		}
	}

	// Demote self if it believed it was the leader
	if rf.believesLeader {
		rf.believesLeader = false
		rf.currentTerm = args.Term
		LogDebugf("\t%d demotes themselves for term %d\n", rf.me, rf.currentTerm)
	}

	// if it was a heartbeat, just return now :p
	if isHeartbeat {
		// LogDebugf("\t%d received heartbeat from %d\n", rf.me, args.LeaderId)
		return
	}

	// Check if log contains a matching entry at args.PrevLogIndex
	// If given index is 3, need at least an array of length 4 to acommodate
	if len(rf.logEntries) <= args.PrevLogIndex || rf.logTermsReceived[args.PrevLogIndex] != args.PrevLogTerm {
		// LogDebugf("\tfailed - has %d entries, needs %d\n", len(rf.logEntries), args.PrevLogIndex)
		// LogDebugf("\tfailed - non matching log terms %d %d at index %d\n", rf.logTermsReceived[args.PrevLogIndex], args.PrevLogTerm, args.PrevLogIndex)
		reply.Success = false 
		return
	}

	// Delete everything after prevlogindex, if existing
    if len(rf.logEntries) > args.PrevLogIndex + 1 {
		rf.logEntries = slices.Delete(rf.logEntries, args.PrevLogIndex+1, len(rf.logEntries))
		rf.logTermsReceived = slices.Delete(rf.logTermsReceived, args.PrevLogIndex+1, len(rf.logTermsReceived))
	}
	
	// Re-append all the sent entries
	rf.logEntries = append(rf.logEntries, args.LogEntries...)
	rf.logTermsReceived = append(rf.logTermsReceived, args.LogTermsReceived...)

	// set commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(min(args.LeaderCommit, args.PrevLogIndex + len(args.LogEntries)), len(rf.logEntries)-1)
	}
	
	reply.Success = true;

	return;
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.believesLeader {
			// Send heartbeats if rf believes they are the leader
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LogEntries = make([]interface{}, 0)
			args.LeaderCommit = rf.commitIndex
			args.LeaderId = rf.me
			for index, _ := range rf.peers {
				if index != rf.me {
					reply := AppendEntriesReply{}
					// LogDebugf("%d heartbeating %d\n", rf.me, index)
					go rf.sendAppendEntriesHeartbeat(index, &args, &reply)
				}
			}
		}
		rf.mu.Unlock()
		// Sleep for 100 milliseconds
		delay := 100 // 10 times per second
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntriesHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) attemptToUpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for start_n := len(rf.logEntries)-1 ; start_n > rf.commitIndex ; start_n-- {
		if rf.logTermsReceived[start_n] < rf.currentTerm {
			// can't commit "back into" a previous term, so just quit
			break
		}

		// otherwise attempt to set this as the new commit index
		number_matches := 1
		for i := 0; i<len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= start_n {
				number_matches++
			}
		}
		if number_matches >= rf.votesNeededToWin {
			// commit her!
			LogDebugf("%d's commit index is now %d\n", rf.me, start_n)
			rf.commitIndex = start_n 

			// go rf.sendApplyMsg(start_n)
			// and also quit
			break
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm && rf.believesLeader {
		// it's joever, demote from leader
		rf.believesLeader = false
		rf.currentTerm = reply.Term
		LogDebugf("\t%d demotes (from APPEND from %d at term %d) for term %d\n", rf.me, server, reply.Term, rf.currentTerm)
	} else if reply.Term <= rf.currentTerm && rf.believesLeader {
		// alright, process the append entries response
		if reply.Success {
			LogDebugf("\t%d APPENDED until index %d from %d\n", server, args.PrevLogIndex + len(args.LogEntries), rf.me)
			rf.nextIndex[server] = args.PrevLogIndex + len(args.LogEntries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)
			go rf.attemptToUpdateCommitIndex()
		} else {
			if rf.nextIndex[server] == 1 {
				// give up
				LogDebugf("\t%d could not append until index %d from %d, GIVING UP\n", server, args.PrevLogIndex + len(args.LogEntries), rf.me)
			} else {
				LogDebugf("\t%d could not append until index %d from %d, resending\n", server, args.PrevLogIndex + len(args.LogEntries), rf.me)
				rf.nextIndex[server] = rf.nextIndex[server] - 1
				go rf.AppendToPeer(args.LogEntries[0], server)
			}
		}
	}
	
	return ok
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votesNeededToWin = len(rf.peers)/2 + 1

	// Initialize raft server here
	// start with one (empty) entry in the log
	rf.logEntries = make([]interface{}, 1)
	rf.logTermsReceived = make([]int, 1)
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.applyCh = applyCh

	rf.isCandidate = false

	rf.commitIndex = 0
	rf.lastApplied = 0
	// volatile server state below
	rf.nextIndex = make([]int, len(rf.peers))
	for i:=0;i<len(rf.peers);i++ {
		rf.nextIndex[i] = 1 // initialize to last log index (0) + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	rf.believesLeader = false
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommands()
	go rf.sendHeartbeats()

	return rf
}
