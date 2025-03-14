package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

func (rf *Raft) DebugDisconnect() {
	DebugPrint(dLeader, "S%d DISCONNECTED", rf.me)
}

func (rf *Raft) DebugConnect() {
	DebugPrint(dLeader, "S%d CONNECTED", rf.me)
}

type Raft struct {
	mu               sync.Mutex            // lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd   // RPC end points of all peers
	votesNeededToWin int                   // shortcut to store what a majority of peers entails
	persister        *tester.Persister     // object to hold this peer's persisted state
	me               int                   // this peer's index into peers[]
	dead             int32                 // set by Kill()
	currentTerm      int                   // last term the server saw (initialize at 0)
	votedFor         int                   // index of last voted-for peer (-1 if none)
	logEntries       []interface{}         // log entry data
	logTermsReceived []int                 // term when each log entry was receieved
	commitIndex      int                   // index of highest log entry known to be committed
	lastApplied      int                   // index of highest log entry known to be applied
	nextIndex        []int                 // for each server, index of next log entry to send to that server
	matchIndex       []int                 // for each server, index of highest log entry known to be replicated on server
	believesLeader   bool                  // if the server believes it is the leader
	electionTimer    time.Time             // tracks when the current election timer is set to run out
	numVotesReceived int                   // number of votes received for current election so far
	applyCh          chan raftapi.ApplyMsg // channel to apply messages on
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
	Term             int
	LeaderId         int
	PrevLogIndex     int
	PrevLogTerm      int
	LogEntries       []interface{}
	LogTermsReceived []int
	LeaderCommit     int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Return currentTerm and whether this server believes it is the leader
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
	// rf.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.logTermsReceived)
	// rf.mu.Unlock()

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	DebugPrint(dPersist, "S%d persisted", rf.me)
	// str := fmt.Sprintf("%v", rf.logEntries)
	// DebugPrint(dPersist, "S%d saved %s", rf.me, str)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []interface{}
	var logTermsReceived []int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&logTermsReceived) != nil {
		// rf.mu.Lock()
		DebugPrint(dError, "S%d had an issue reading persist", rf.me)
		// rf.mu.Unlock()
	} else {
		// rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.logTermsReceived = logTermsReceived
		DebugPrint(dPersist, "S%d read persist", rf.me)
		// rf.mu.Unlock()
	}
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

// Resets the election timer to the current time + a random offset
func (rf *Raft) resetElectionTimer() {
	ms := 1500 + (rand.Int63() % 650)
	now := time.Now()
	candidateTimer := now.Add(time.Duration(ms) * time.Millisecond)

	if candidateTimer.After(rf.electionTimer) {
		rf.electionTimer = candidateTimer
	}

	// DebugPrint(dTime, "S%d reset election timer to %d", rf.me, rf.electionTimer.UnixMilli())
}

// Responds to request vote RPCs; either gives the vote or not
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	myOldTerm := rf.currentTerm

	reply.Term = args.Term
	reply.VoteGranted = false

	// Determine if the candidate's log is up to date
	myLastLogIndex := len(rf.logEntries) - 1
	myLastLogTerm := rf.logTermsReceived[len(rf.logTermsReceived)-1]
	candidateLogUpToDate := false
	if args.LastLogTerm > myLastLogTerm {
		candidateLogUpToDate = true
	} else if args.LastLogTerm == myLastLogTerm {
		candidateLogUpToDate = args.LastLogIndex >= myLastLogIndex
	}

	// If the candidate term is >= my current term, try to grant them the vote
	if args.Term >= rf.currentTerm {

		// Demote self if I believe I'm the leader currently
		if args.Term > rf.currentTerm {
			if rf.believesLeader {
				DebugPrint(dLeader, "S%d demotes for term %d", rf.me, rf.currentTerm)
			}
			rf.believesLeader = false
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		// Grant them the vote if I haven't voted already, or I voted already in this term for them previously
		// AND if their log is up to date
		if rf.votedFor == -1 || rf.votedFor == args.CandidateIndex { // TODO: remove that latter check...?
			if candidateLogUpToDate {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateIndex
			}
		}

		// DebugPrint(dPersist, "S%d 1", rf.me)
		rf.persist()
	} else { // Otherwise, tell them that their term is lagging behind, and don't grant them the vote
		reply.Term = rf.currentTerm
	}

	if reply.VoteGranted {
		// If we granted the vote, reset the election timer
		rf.resetElectionTimer()
		DebugPrint(dVote, "S%d (term %d) voted YES for %d (term %d)", rf.me, myOldTerm, args.CandidateIndex, args.Term)
	} else {
		DebugPrint(dVote, "S%d (term %d) voted NO for %d (term %d)", rf.me, myOldTerm, args.CandidateIndex, args.Term)
	}
}

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

// Sends a Request Vote RPC and handles the reply
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If we are a candidate, the vote was granted, and the vote was for the term we're currently in, increment numVotesReceived
	if !rf.believesLeader && reply.VoteGranted && args.Term == rf.currentTerm {
		rf.numVotesReceived += 1

		// If we have enough votes to win, become leader
		if rf.numVotesReceived >= rf.votesNeededToWin {
			rf.believesLeader = true
			DebugPrint(dLeader, "S%d becomes leader for term %d", rf.me, reply.Term)

			// Initialize nextIndex to log[-1] and matchIndex to 0
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.nextIndex[i] = max(len(rf.logEntries), 1)
					rf.matchIndex[i] = 0
				}
			}
		}
	} else if reply.Term > rf.currentTerm {
		// Otherwise, if the reply is telling us our term is behind, update our term
		// No need to reset numVotesReceived because that gets set to 0 when a new election starts
		DebugPrint(dTerm, "S%d updates term from %d to %d", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term

		// DebugPrint(dPersist, "S%d 2", rf.me)
		rf.persist()
	}
	return ok
}

func (rf *Raft) IssueAppendToPeer(peer int) { // move this into the big function?
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	nextIndexToSend := rf.nextIndex[peer]
	lastLogIndex := len(rf.logEntries) - 1

	args.LeaderCommit = min(rf.commitIndex, rf.matchIndex[peer])

	if nextIndexToSend <= lastLogIndex {
		args.PrevLogIndex = nextIndexToSend - 1
		args.PrevLogTerm = rf.logTermsReceived[args.PrevLogIndex]

		args.LogEntries = rf.logEntries[nextIndexToSend:len(rf.logEntries)]
		args.LogTermsReceived = rf.logTermsReceived[nextIndexToSend:len(rf.logEntries)]

		if nextIndexToSend != 1 {
			DebugPrint(dSendAppend, "S%d (term %d) sending append (from %d to %d) to %d", rf.me, rf.currentTerm, nextIndexToSend, lastLogIndex, peer)
		}
	} else {
		// This should never happen
		DebugPrint(dError, "S%d has nextIndex = %d for S%d where lastLogIndex = %d", rf.me, nextIndexToSend, peer, lastLogIndex)
	}

	reply := AppendEntriesReply{}
	go rf.sendAppendEntries(peer, &args, &reply)
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.believesLeader {
			// Send heartbeats if rf believes they are the leader
			for index, _ := range rf.peers {
				if index != rf.me {

					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LogEntries = make([]interface{}, 0)
					args.LeaderCommit = min(rf.commitIndex, rf.matchIndex[index])
					args.LeaderId = rf.me

					reply := AppendEntriesReply{}
					go rf.sendAppendEntriesHeartbeat(index, &args, &reply)

					// DebugPrint(dLeader, "S%d sending heartbeat to %d", rf.me, index)

				}
			}
		}

		rf.mu.Unlock()
		// Sleep for 100 milliseconds
		delay := 80 // 10 times per second
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
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
				rf.IssueAppendToPeer(peer)
			}
		}
		DebugPrint(dLeader, "S%d received index %d", rf.me, len(rf.logEntries)-1)
		rf.persist()
	}

	index := len(rf.logEntries) - 1
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
	DebugPrint(dLeader, "S%d KILLED", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyCommands() {
	lastApplied := 0

	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > lastApplied {
			// NEW CODE TO APPLY MULTIPLE AT ONCE (UP TO 5)
			num_to_apply := min(10, rf.commitIndex-lastApplied)
			messages := make([]raftapi.ApplyMsg, num_to_apply)
			for i := 0; i < num_to_apply; i++ {
				lastApplied += 1

				applyMsg := raftapi.ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.logEntries[lastApplied]
				applyMsg.CommandIndex = lastApplied

				DebugPrint(dCommit, "S%d APPLYING %d", rf.me, lastApplied)

				messages[i] = applyMsg
			}
			rf.mu.Unlock()
			for i := 0; i < num_to_apply; i++ {
				rf.applyCh <- messages[i]
			}

			// OLD CODE TO APPLY JUST ONE
			// lastApplied += 1

			// applyMsg := raftapi.ApplyMsg{}
			// applyMsg.CommandValid = true
			// applyMsg.Command = rf.logEntries[lastApplied]
			// applyMsg.CommandIndex = lastApplied

			// DebugPrint(dCommit, "S%d APPLYING %d", rf.me, lastApplied)
			// rf.mu.Unlock()

			// rf.applyCh <- applyMsg

		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // always sleep

		rf.mu.Lock()

		electionTimerRanOut := time.Now().After(rf.electionTimer)

		if !rf.believesLeader && electionTimerRanOut && rf.votedFor == -1 {
			rf.resetElectionTimer()
			// DebugPrint(dElection, "S%d starting election at %d", rf.me, time.Now().UnixMilli())

			// Increment term
			rf.currentTerm += 1
			DebugPrint(dElection, "S%d Starting election for term %d", rf.me, rf.currentTerm)

			// Vote for self
			rf.votedFor = rf.me
			rf.numVotesReceived = 1

			// Issue RequestVote RPCs
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateIndex = rf.me

			// Include last log index and last log term (for up-to-date checks)
			args.LastLogIndex = len(rf.logEntries) - 1
			args.LastLogTerm = rf.logTermsReceived[len(rf.logTermsReceived)-1]

			// Issue RequestVote RPCs to peers
			for index, _ := range rf.peers {
				if index != rf.me {
					reply := RequestVoteReply{}
					go rf.sendRequestVote(index, &args, &reply)
				}
			}

			// DebugPrint(dPersist, "S%d 5", rf.me)
			rf.persist()
		} else if electionTimerRanOut { // Reset so that we can vote again
			rf.votedFor = -1
			// DebugPrint(dPersist, "S%d 6", rf.me)
			rf.persist()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) ReceiveAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isHeartbeat := len(args.LogEntries) == 0

	// Always reply letting them know what my term is
	reply.Term = rf.currentTerm

	// Ignore all AppendEntries RPCs if the sender's term is lower than my term
	if args.Term < rf.currentTerm {
		reply.Success = false
		if isHeartbeat {
			DebugPrint(dReplyAppend, "S%d (term %d) ignoring HEARTBEAT from %d (term %d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		} else {
			DebugPrint(dReplyAppend, "S%d (term %d) ignoring APPEND from %d (term %d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		}

		return
	} // Below this line, args.Term >= rf.currentTerm

	// DebugPrint(dHeartbeat, "S%d heartbeated from %d", rf.me, args.LeaderId)
	rf.resetElectionTimer()

	if rf.commitIndex < args.LeaderCommit && isHeartbeat {
		prevCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.logEntries)-1)
		if rf.commitIndex == args.LeaderCommit {
			DebugPrint(dCommit, "S%d commit index %d -> %d (from %d)", rf.me, prevCommitIndex, rf.commitIndex, args.LeaderId)
		}
	}

	// Demote self if it believed it was the leader
	if rf.believesLeader {
		rf.believesLeader = false
		rf.currentTerm = args.Term
		DebugPrint(dLeader, "S%d demotes for term %d (via append)", rf.me, rf.currentTerm)

		// DebugPrint(dPersist, "S%d 7", rf.me)
		rf.persist()
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term // was i not doing this before??? ummm???
		// should always update their term right??
	}

	// if it was a heartbeat, just return now :p
	if isHeartbeat {
		return
	}

	rfLastLogIndex := len(rf.logEntries) - 1
	// Check if log contains a matching entry at args.PrevLogIndex
	if rfLastLogIndex < args.PrevLogIndex || rf.logTermsReceived[args.PrevLogIndex] != args.PrevLogTerm {
		reply.Success = false
		return
	}

	replaceElements := false

	if rfLastLogIndex >= args.PrevLogIndex+len(args.LogEntries) {
		for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+1+len(args.LogEntries); i++ {
			if rf.logTermsReceived[i] != args.LogTermsReceived[i-1-args.PrevLogIndex] {
				replaceElements = true
				break
			}
		}
	} else {
		replaceElements = true
	}

	if replaceElements {
		DebugPrint(dReplyAppend, "S%d (term %d) APPENDED %d-%d from %d (term %d)", rf.me, rf.currentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.LogEntries), args.LeaderId, args.Term)

		rf.logEntries = slices.Delete(rf.logEntries, args.PrevLogIndex+1, len(rf.logEntries))
		rf.logTermsReceived = slices.Delete(rf.logTermsReceived, args.PrevLogIndex+1, len(rf.logTermsReceived))
		rf.logEntries = append(rf.logEntries, args.LogEntries...)
		rf.logTermsReceived = append(rf.logTermsReceived, args.LogTermsReceived...)

		// DebugPrint(dPersist, "S%d 8", rf.me)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		// rf.commitIndex = min(min(args.LeaderCommit, args.PrevLogIndex+len(args.LogEntries)), len(rf.logEntries)-1)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntriesHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)
	return ok
}

func (rf *Raft) attemptToUpdateCommitIndex() {
	// DebugPrint(dLeader, "S%d updating commit index", rf.me)

	for start_n := len(rf.logEntries) - 1; start_n > rf.commitIndex; start_n-- {
		if rf.logTermsReceived[start_n] < rf.currentTerm {
			// can't commit "back into" a previous term, so just quit

			// TODO what is going on here
			break
		}
		number_matches := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= start_n {
				number_matches++
			}
		}
		if number_matches >= rf.votesNeededToWin {
			DebugPrint(dCommit, "S%d (LEADER) commit index -> %d", rf.me, start_n)
			rf.commitIndex = start_n

			// str := fmt.Sprintf("%v", rf.logEntries)
			// DebugPrint(dLeader, "S%d %s", rf.me, str)
			break
		}
	}
	// DebugPrint(dLeader, "S%d DONE updating commit index", rf.me)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm && rf.believesLeader {
			// We're an outdated leader :( demote!
			rf.believesLeader = false
			rf.currentTerm = reply.Term

			// DebugPrint(dPersist, "S%d 9", rf.me)
			rf.persist()

			DebugPrint(dReceiveAppend, "S%d (term %d) received APPEND from (%d term %d)", rf.me, rf.currentTerm, server, reply.Term)
			DebugPrint(dLeader, "S%d demotes for term %d", rf.me, rf.currentTerm)
		} else if reply.Term <= rf.currentTerm && rf.believesLeader {
			// Ok, otherwise process the append entries response

			if reply.Success {
				// The logs were appended successfully!
				DebugPrint(dReceiveAppend, "S%d (term %d) SUCCEEDED append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)

				// Update nextIndex and matchIndex for this server. Use max to avoid late-arriving reponses from overwriting faster ones
				rf.nextIndex[server] = max(args.PrevLogIndex+len(args.LogEntries)+1, rf.nextIndex[server])
				prevMatchIndex := rf.matchIndex[server]
				rf.matchIndex[server] = max(args.PrevLogIndex+len(args.LogEntries), rf.matchIndex[server])

				// Attempt to update the commit index, if server's match index was updated
				if prevMatchIndex < rf.matchIndex[server] {
					rf.attemptToUpdateCommitIndex()
				}
			} else {
				// The logs were not appended successfully; attempt to back up and try again

				if rf.nextIndex[server] == 1 {
					// If we've already backed up to the earliest log, just give up entirely
					DebugPrint(dReceiveAppend, "S%d (term %d) GIVING UP append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)
					rf.IssueAppendToPeer(server)
				} else {
					// Otherwise, decrement by 1 and resend the AppendEntries RPC
					// TODO: optimize backing up strategy here
					// DebugPrint(dReceiveAppend, "S%d (term %d) RESENDING append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)
					rf.nextIndex[server] = max(rf.nextIndex[server]-25, 1)
					rf.IssueAppendToPeer(server)
				}
			}
		}
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// something failed
		if rf.believesLeader && !rf.killed() {
			// basically - the disconnected server demoted but the recursive issueAppends are still going because of
			// this else clause!
			if rf.nextIndex[server] == 1 {
				// If we've already backed up to the earliest log, just give up entirely
				DebugPrint(dReceiveAppend, "S%d (term %d) GIVING UP append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)
				rf.IssueAppendToPeer(server)
			} else {
				// Otherwise, decrement by 1 and resend the AppendEntries RPC
				// TODO: optimize backing up strategy here
				// DebugPrint(dReceiveAppend, "S%d (term %d) RESENDING append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)
				rf.nextIndex[server] = max(rf.nextIndex[server]-25, 1)
				rf.IssueAppendToPeer(server)
			}
		}
	}

	return ok
}

// persister is a place for this server to
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
	// Start with one (empty) entry in the log
	rf.logEntries = make([]interface{}, 1)
	rf.logTermsReceived = make([]int, 1)
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0
	// volatile server state below
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1 // initialize to last log index (0) + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	rf.believesLeader = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommands()
	go rf.sendHeartbeats()

	rf.resetElectionTimer()

	// rf.persist()

	return rf
}
