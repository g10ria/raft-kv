package raft

import (
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

func (rf *Raft) DebugShutdown() {
	DebugPrint(dLeader, "S%d SHUTDOWN", rf.me)
}

func (rf *Raft) DebugRestart() {
	DebugPrint(dLeader, "S%d RESTART", rf.me)
}

func (rf *Raft) DebugConnect() {
	DebugPrint(dLeader, "S%d CONNECTED", rf.me)
}

type Raft struct {
	mu                sync.Mutex            // lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd   // RPC end points of all peers
	votesNeededToWin  int                   // shortcut to store what a majority of peers entails
	persister         *tester.Persister     // object to hold this peer's persisted state
	me                int                   // this peer's index into peers[]
	dead              int32                 // set by Kill()
	currentTerm       int                   // last term the server saw (initialize at 0)
	votedFor          int                   // index of last voted-for peer (-1 if none)
	logEntries        []interface{}         // log entry data
	logTermsReceived  []int                 // term when each log entry was receieved
	commitIndex       int                   // index of highest log entry known to be committed
	lastApplied       int                   // index of highest log entry known to be applied
	nextIndex         []int                 // for each server, index of next log entry to send to that server
	matchIndex        []int                 // for each server, index of highest log entry known to be replicated on server
	believesLeader    bool                  // if the server believes it is the leader
	electionTimer     time.Time             // tracks when the current election timer is set to run out
	numVotesReceived  int                   // number of votes received for current election so far
	applyCh           chan raftapi.ApplyMsg // channel to apply messages on
	logStart          int                   // Earliest log index that hasn't been snapshotted (default to 0)
	snapshot          []byte                // Most recently provided snapshot (default to nil)
	snapshotLastIndex int                   // Last included log index in the snapshot (set when saving snapshot)
	snapshotLastTerm  int                   // Last included term in the snapshot (set when saving snapshot)
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
	LogLength        int
	CommitIndexFr    int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int

	HeartbeatSuccess bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// Return currentTerm and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.believesLeader
	return term, isLeader
}

func (rf *Raft) persist() {
	// Set up encoder

	if !rf.Killed() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		// Encode necessary data fields
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.logEntries[rf.logStart:])       // Save compacted log
		e.Encode(rf.logTermsReceived[rf.logStart:]) // Save compacted log

		e.Encode(rf.snapshotLastIndex)
		e.Encode(rf.snapshotLastTerm)

		// Save with persister
		raftstate := w.Bytes()
		rf.persister.Save(raftstate, rf.snapshot)

		// DebugPrint(dPersist, "S%d persisted", rf.me)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if len(data) < 1 {
		// Ignore if no state
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []interface{}
	var logTermsReceived []int

	var snapshotLastIndex int
	var snapshotLastTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&logTermsReceived) != nil ||
		d.Decode(&snapshotLastIndex) != nil ||
		d.Decode(&snapshotLastTerm) != nil {
		DebugPrint(dError, "S%d had an issue reading persist", rf.me)
	} else {
		if snapshotLastIndex == 0 { // No snapshot was saved
			DebugPrint(dPersist, "S%d started up and did not install snapshot", rf.me)
			// No snapshots to install, just copy in everything
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.logEntries = logEntries
			rf.logTermsReceived = logTermsReceived
		} else { // A snapshot should have been saved
			DebugPrint(dPersist, "S%d started up and installed snapshot (up to %d)", rf.me, snapshotLastIndex)
			// Install the snapshot and set logStart accordingly
			rf.snapshot = snapshot
			rf.snapshotLastIndex = snapshotLastIndex
			rf.snapshotLastTerm = snapshotLastTerm

			rf.logStart = rf.snapshotLastIndex + 1

			rf.currentTerm = currentTerm
			rf.votedFor = votedFor

			dummyLogs := make([]interface{}, snapshotLastIndex+1)
			dummyTerms := make([]int, snapshotLastIndex+1)
			rf.logEntries = append(dummyLogs, logEntries...)
			rf.logTermsReceived = append(dummyTerms, logTermsReceived...)

			DebugPrint(dPersist, "S%d snapshot info: last index was %d, plus log entries %d-%d. read %d log entries from snapshot..",
				rf.me, snapshotLastIndex, snapshotLastIndex+1, len(rf.logEntries)-1, len(logEntries))

			// copy in the term, even though the corresponding entry is empty (saved in the snapshot)
			// since we use this for various checks
			rf.logTermsReceived[snapshotLastIndex] = rf.snapshotLastTerm
		}

		DebugPrint(dPersist, "S%d read persist", rf.me)
	}
}

// returns how many bytes in Raft's persisted log
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If we have enough entries to actually hit that index and
	// if we haven't already saved a more up-to-date snapshot
	if index <= len(rf.logEntries) && index >= rf.logStart {
		DebugPrint(dPersist, "S%d snapshotting up to index %d", rf.me, index)

		rf.logStart = index + 1
		// Save snapshot info
		rf.snapshot = snapshot
		rf.snapshotLastIndex = index
		rf.snapshotLastTerm = rf.logTermsReceived[index]

		DebugPrint(dPersist, "S%d old log length %d", rf.me, len(rf.logEntries))
		// update log
		rf.updateLogAfterSnapshot()

		DebugPrint(dPersist, "S%d new log length %d", rf.me, len(rf.logEntries))

		// And persist it
		rf.persist()
	}
}

// assumes snapshot, snapshotLastIndex, and snapshotLastTerm have all been set
func (rf *Raft) updateLogAfterSnapshot() {
	numEntriesInLog := len(rf.logEntries)
	numEntriesInSnapshot := rf.snapshotLastIndex + 1

	newLogEntries := make([]interface{}, numEntriesInSnapshot)
	newLogTermsReceived := make([]int, numEntriesInSnapshot)
	for i := range newLogTermsReceived {
		newLogTermsReceived[i] = -1
	}
	newLogTermsReceived[rf.snapshotLastIndex] = rf.snapshotLastTerm // set the second to last logTermsReceived value

	if numEntriesInLog > numEntriesInSnapshot { // check if it's actually a prefix...?
		// Only re-append the rest of the log to the newly created dummy log
		// if the snapshot was a prefix of the old log
		newLogEntries = append(newLogEntries, rf.logEntries[rf.snapshotLastIndex+1:]...)
		newLogTermsReceived = append(newLogTermsReceived, rf.logTermsReceived[rf.snapshotLastIndex+1:]...)
	}
	rf.logEntries = newLogEntries
	rf.logTermsReceived = newLogTermsReceived

	// DebugPrint(dPersist, "S%d terms are now %v", rf.me, rf.logTermsReceived)
}

// Resets the election timer to the current time + a random offset
func (rf *Raft) resetElectionTimer() {
	ms := 1400 + (rand.Int63() % 700)
	now := time.Now()
	candidateTimer := now.Add(time.Duration(ms) * time.Millisecond)

	if candidateTimer.After(rf.electionTimer) {
		// rf.mu.Lock()
		rf.electionTimer = candidateTimer
		// rf.mu.Unlock()
	}
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

	if ok {
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
	} else {
		// maybe re-issue the request...? idk if this will just break everything lol
	}
	return ok
}

func (rf *Raft) ReceiveInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	reply.Success = true

	// maybe more stuff to do here? idk
	if args.LastIncludedIndex > rf.snapshotLastIndex {
		// Install snapshot if it's more up to date than current snapshot

		rf.snapshotLastIndex = args.LastIncludedIndex
		rf.snapshotLastTerm = args.LastIncludedTerm
		rf.snapshot = args.Data

		rf.logStart = rf.snapshotLastIndex + 1

		rf.updateLogAfterSnapshot()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveInstallSnapshot", args, reply)

	if ok {
		// TODO: make the debug topics like dSnapshot or something lol
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm && rf.believesLeader {
			// We're an outdated leader :( demote!
			rf.believesLeader = false
			rf.currentTerm = reply.Term

			// DebugPrint(dPersist, "S%d 10", rf.me)
			rf.persist()

			DebugPrint(dReceiveAppend, "S%d (term %d) received INSTALL from (%d term %d)", rf.me, rf.currentTerm, server, reply.Term)
			DebugPrint(dLeader, "S%d demotes for term %d", rf.me, rf.currentTerm)
		} else if reply.Term <= rf.currentTerm && rf.believesLeader {
			// Ok, otherwise process the install snapshot response

			if reply.Success {
				// The snapshot was installed successfully!
				DebugPrint(dReceiveAppend, "S%d (term %d) SUCCEEDED install snapshot (until %d) to %d", rf.me, rf.currentTerm, args.LastIncludedIndex, server)

				// Update nextIndex and matchIndex for this server. Use max to avoid late-arriving reponses from overwriting faster ones
				prevMatchIndex := rf.matchIndex[server]
				rf.matchIndex[server] = max(args.LastIncludedIndex, rf.matchIndex[server])
				rf.nextIndex[server] = max(rf.matchIndex[server]+1, rf.nextIndex[server])

				DebugPrint(dReceiveAppend, "S%d (term %d) updated %d status to next=%d match=%d", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])

				// Attempt to update the commit index, if server's match index was updated
				if prevMatchIndex < rf.matchIndex[server] {
					rf.attemptToUpdateCommitIndex()
				}

				// continue issuing appends to the server, we might not be done!
				// rf.IssueAppendToPeer(server)
			} else {
				DebugPrint(dReceiveAppend, "S%d (term %d) FAILED install snapshot (until %d) to %d, trying again", rf.me, rf.currentTerm, args.LastIncludedIndex, server)
				// The snapshot was not successfully installed; just send it again
				rf.IssueAppendToPeer(server)
			}
		}
	} else { // didn't even receive a response
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// something failed
		if rf.believesLeader && !rf.Killed() {
			// The snapshot was not successfully installed; just send it again
			rf.IssueAppendToPeer(server)
		}
	}

	return ok
}

func (rf *Raft) IssueAppendToPeer(peer int) {
	nextIndexToSend := rf.nextIndex[peer]
	lastLogIndex := len(rf.logEntries) - 1

	if nextIndexToSend < rf.logStart {
		DebugPrint(dSendAppend, "S%d sending snapshot to %d up to index %d", rf.me, peer, rf.snapshotLastIndex)

		args := InstallSnapshotArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LastIncludedIndex = rf.snapshotLastIndex
		args.LastIncludedTerm = rf.snapshotLastTerm
		args.Data = rf.snapshot
		reply := InstallSnapshotReply{}

		go rf.sendInstallSnapshot(peer, &args, &reply)

	} else if nextIndexToSend <= lastLogIndex {
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me

		args.LeaderCommit = min(rf.commitIndex, rf.matchIndex[peer])

		args.PrevLogIndex = nextIndexToSend - 1
		args.PrevLogTerm = rf.logTermsReceived[args.PrevLogIndex]

		args.LogEntries = rf.logEntries[nextIndexToSend:len(rf.logEntries)]
		args.LogTermsReceived = rf.logTermsReceived[nextIndexToSend:len(rf.logEntries)]

		if nextIndexToSend != 1 {
			DebugPrint(dSendAppend, "S%d (term %d) sending append (from %d to %d) to %d", rf.me, rf.currentTerm, nextIndexToSend, lastLogIndex, peer)
		}

		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(peer, &args, &reply)
	} else {
		// This should never happen
		DebugPrint(dError, "S%d has nextIndex = %d for S%d where lastLogIndex = %d", rf.me, nextIndexToSend, peer, lastLogIndex)
	}
}

func (rf *Raft) sendHeartbeats() {
	for !rf.Killed() {
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

					args.LogLength = len(rf.logEntries)
					args.CommitIndexFr = rf.commitIndex

					reply := AppendEntriesReply{}
					go rf.sendAppendEntriesHeartbeat(index, &args, &reply)

					// DebugPrint(dLeader, "S%d sending heartbeat to %d", rf.me, index)
				}
			}
		}

		rf.mu.Unlock()
		delay := 80 // ~10 times per second
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
		DebugPrint(dLeader, "S%d received index %d", rf.me, len(rf.logEntries)-1)
		for peer, _ := range rf.peers {
			if peer != rf.me {
				rf.IssueAppendToPeer(peer)
			}
		}
		rf.persist()
	}

	index := len(rf.logEntries) - 1
	term := rf.currentTerm

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DebugPrint(dLeader, "S%d KILLED", rf.me)
	// rf.mu.Lock()
	// fmt.Printf("S%d KILLED\n", rf.me)

	time.Sleep(50 * time.Millisecond) // sleep before closing channel
	close(rf.applyCh)
	// rf.mu.Unlock()
}

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyCommands() {
	lastApplied := 0

	for !rf.Killed() {
		rf.mu.Lock()

		if rf.snapshotLastIndex > lastApplied {
			// send a snapshot on the channel
			applyMsg := raftapi.ApplyMsg{}
			applyMsg.SnapshotValid = true
			applyMsg.Snapshot = rf.snapshot
			applyMsg.SnapshotIndex = rf.snapshotLastIndex
			applyMsg.SnapshotTerm = rf.snapshotLastTerm

			lastApplied = rf.snapshotLastIndex

			rf.mu.Unlock()
			rf.applyCh <- applyMsg

		} else if rf.commitIndex > lastApplied {
			// Applies up to 10 commits at once
			num_to_apply := min(10, rf.commitIndex-lastApplied)
			messages := make([]raftapi.ApplyMsg, num_to_apply)

			for i := 0; i < num_to_apply; i++ {
				lastApplied += 1

				applyMsg := raftapi.ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.logEntries[lastApplied]
				applyMsg.CommandIndex = lastApplied
				applyMsg.CommandTerm = rf.logTermsReceived[lastApplied]

				DebugPrint(dCommit, "S%d APPLYING %d", rf.me, lastApplied)
				// fmt.Printf("S%d APPLYING %d with value %d\n", rf.me, lastApplied, applyMsg.Command)

				messages[i] = applyMsg
			}

			rf.mu.Unlock()

			keep_applying := false
			for i := 0; i < num_to_apply; i++ {
				rf.mu.Lock()
				keep_applying = !rf.Killed()
				// getting killed here?
				if keep_applying {
					// fmt.Printf("%d applying %d\n", rf.me, lastApplied-num_to_apply+i+1)
					rf.mu.Unlock()
					rf.applyCh <- messages[i]
				} else {
					// fmt.Printf("%d stop applying at %d\n", rf.me, messages[i].Command)
					rf.mu.Unlock()
					break
				}
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for !rf.Killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		electionTimerRanOut := time.Now().After(rf.electionTimer)

		if !rf.believesLeader && electionTimerRanOut && rf.votedFor == -1 {
			rf.resetElectionTimer()

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

	reply.HeartbeatSuccess = true

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

	// Update commit index from heartbeat, if relevant
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

	//  Update current term from append entries
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}

	// If it was a heartbeat, just return now
	if isHeartbeat {
		// ohh ok
		// DebugPrint(dCommit, "S%d (heartbeat) len %d vs received %d", rf.me, len(rf.logEntries), args.LogLength)
		// DebugPrint(dCommit, "S%d (heartbeat) commit index %d, received %d", rf.me, rf.commitIndex, args.LeaderCommit)
		if len(rf.logEntries) < args.LogLength || rf.commitIndex < args.CommitIndexFr {
			// basically we are unable to update our commit index to the leader's commit :(
			// DebugPrint(dCommit, "S%d (HEART) len %d, received %d. commit %d vs %d", rf.me, len(rf.logEntries), args.LogLength, rf.commitIndex, args.CommitIndexFr)
			reply.HeartbeatSuccess = false
			// Co-opt the x index and success flag here lol

			// Term    int
			// Success bool
			// XTerm   int
			// XIndex  int
			// XLen    int
		}
		return
	}

	// jellyfish
	// DebugPrint(dReplyAppend, "S%d (term %d) received APPEND %d-%d from %d (term %d)", rf.me, rf.currentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.LogEntries), args.LeaderId, args.Term)

	rfLastLogIndex := len(rf.logEntries) - 1
	// Check if log contains a matching entry at args.PrevLogIndex
	if rfLastLogIndex < args.PrevLogIndex || rf.logTermsReceived[args.PrevLogIndex] != args.PrevLogTerm {
		reply.Success = false

		// Include necessary info for backing-up optimization
		if rfLastLogIndex < args.PrevLogIndex {
			// Follower's log is too short
			reply.XLen = len(rf.logEntries)
		} else {
			reply.XLen = -1
			reply.XTerm = rf.logTermsReceived[args.PrevLogIndex]
			XIndex := args.PrevLogIndex
			for ; XIndex >= 0; XIndex-- {
				if rf.logTermsReceived[XIndex] != reply.XTerm {
					break
				}
			}
			reply.XIndex = XIndex + 1
		}
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
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.HeartbeatSuccess {
			// do nothing
		} else {
			// DebugPrint(dCommit, "S%d (LEADER) heartbeat was UNSUCCESSFUL to %d, resending", rf.me, server)
			rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
			rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)

			rf.IssueAppendToPeer(server)
		}
	}
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
	// DebugPrint(dLeader, "S9 args: %+v", args)
	// DebugPrint(dLeader, "S9 reply (before): %+v", reply)
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
				// DebugPrint(dReceiveAppend, "S%d (term %d) SUCCEEDED append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)

				// Update nextIndex and matchIndex for this server. Use max to avoid late-arriving reponses from overwriting faster ones
				// NOTE: need to fix this maybe? unsure? jellyfish
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
					// Otherwise, back up with optimization (same code replicated below)
					if reply.XLen != -1 {
						// Follower's log was too short
						rf.nextIndex[server] = reply.XLen
						DebugPrint(dReceiveAppend, "S%d 1", rf.me)
					} else {
						DebugPrint(dReceiveAppend, "S%d 2 %d", rf.me, reply.XTerm)
						// Follower's log was long enough, but terms didn't match
						leaderHasXTerm := false
						lastXTermEntryIndex := -1
						for i := 0; i < len(rf.logEntries); i++ {
							if rf.logTermsReceived[i] == reply.XTerm {
								leaderHasXTerm = true
								lastXTermEntryIndex = i
							}
						}
						if !leaderHasXTerm {
							rf.nextIndex[server] = reply.XIndex
						} else {
							rf.nextIndex[server] = lastXTermEntryIndex
						}
					}
					DebugPrint(dReceiveAppend, "S%d (term %d) RESENDING append (until %d) to %d", rf.me, rf.currentTerm, rf.nextIndex[server], server)
					rf.nextIndex[server] = max(rf.nextIndex[server], 1)
					// rf.nextIndex[server] = max(rf.nextIndex[server]-25, 1)
					rf.IssueAppendToPeer(server)
				}
			}
		}
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// something failed
		if rf.believesLeader && !rf.Killed() {

			// check the match index to see if this rpc is still relevant
			// lowkey add this check up above too?

			serverMatch := rf.matchIndex[server]
			attemptedMatch := args.PrevLogIndex + len(args.LogEntries)

			// DebugPrint(dReceiveAppend, "S%d (term %d) didn't get reply to append %d-%d to %d, match=%d attempt=%d",
			// 	rf.me, rf.currentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.LogEntries), server,
			// 	serverMatch, attemptedMatch)

			if serverMatch >= attemptedMatch {
				return ok
			}

			// DebugPrint(dReceiveAppend, "S%d (^ so we're trying again)", rf.me)

			if rf.nextIndex[server] == 1 {
				// If we've already backed up to the earliest log, just give up entirely
				DebugPrint(dReceiveAppend, "S%d (term %d) GIVING UP append (until %d) to %d", rf.me, rf.currentTerm, args.PrevLogIndex+len(args.LogEntries), server)
				rf.IssueAppendToPeer(server)
			} else {
				// NOTE: removing optimization here
				// Otherwise, back up with optimization (same code as above)
				// if reply.XLen != -1 {
				// 	// Follower's log was too short
				// 	rf.nextIndex[server] = reply.XLen
				// 	DebugPrint(dReceiveAppend, "1")
				// } else {
				// 	DebugPrint(dReceiveAppend, "2")
				// 	// Follower's log was long enough, but terms didn't match
				// 	leaderHasXTerm := false
				// 	lastXTermEntryIndex := -1
				// 	for i := 0; i < len(rf.logEntries); i++ {
				// 		if rf.logTermsReceived[i] == reply.XTerm {
				// 			leaderHasXTerm = true
				// 			lastXTermEntryIndex = i
				// 		}
				// 	}
				// 	if !leaderHasXTerm {
				// 		rf.nextIndex[server] = reply.XIndex
				// 	} else {
				// 		rf.nextIndex[server] = lastXTermEntryIndex
				// 	}
				// }
				// DebugPrint(dReceiveAppend, "S%d (term %d) RESENDING append (until %d) to %d", rf.me, rf.currentTerm, rf.nextIndex[server], server)
				// rf.nextIndex[server] = max(rf.nextIndex[server], 1)
				rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
				rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
				// DebugPrint(dReceiveAppend, "S%d (term %d) RESENDIN' append (from %d, match %d) to %d", rf.me, rf.currentTerm, rf.nextIndex[server], rf.matchIndex[server], server)
				rf.IssueAppendToPeer(server)
			}
		}
	}

	return ok
}

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
	rf.logEntries[0] = 420
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
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommands()
	go rf.sendHeartbeats()

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	return rf
}
