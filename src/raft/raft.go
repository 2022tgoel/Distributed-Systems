package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const DEBUG = false
func DebugPrintf(str string, args ...interface{}) {
	if DEBUG {
		fmt.Printf(str, args...)
		return
	}
}
	
	


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (  // iota is reset to 0
	Candidate = iota  // c0 == 0
	Follower = iota  // c1 == 1
	Leader = iota  // c2 == 2
)

type LogEntry struct {
	Term 	int
	Command interface{}
}


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]
	dead      		int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	position		int
	commitIndex		int
	lastApplied		int
	currentTerm		int
	votedFor		int
	electionTimer	int64

	// Logging
	snapshot      	[]byte
	snapshotTerm  	int
	snapshotIndex 	int
	nextIndex		[]int
	matchIndex		[]int
	log				map[int]LogEntry
	applyCh			chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.position == Leader
	return term, isleader
}

func (rf *Raft) resetElectionTimer() {
	// Have recieved communication
	atomic.StoreInt64(&rf.electionTimer, 1);
}

func (rf *Raft) becomeFollower(term int) { 
	// Assumes you are holding a lock!! important!!
	// if rf.position == Leader {
	// 	rf.resetElectionTimer() // you were just dethroned, give it some time
	// }
	rf.currentTerm = term
	rf.position = Follower
	rf.votedFor = -1 // Have not voted for anyone in this term	
}

func (rf *Raft) lastLogIndex() int { // hold the lock!!
	return rf.snapshotIndex + len(rf.log)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// This code assumes that the lock is held!!! This is very important.
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log map[int]LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
	//   error...
		panic("Could not decode persistent state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.commitIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.snapshot = snapshot
	if index > rf.commitIndex {
		panic("uncommited entry")
	}
	rf.snapshotTerm = rf.log[index].Term
	for i := rf.snapshotIndex + 1; i <= index; i++ {
		delete(rf.log, i)
	}
	rf.snapshotIndex = index
	
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data 				[]byte	
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	// Your data here (2A).
	Term		int
}

// example RequestVote RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} 
	reply.Term = rf.currentTerm
	if args.Term == rf.currentTerm {
		// apply the snapshot
		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
		rf.applyCh<-ApplyMsg{false, nil, 0, true, rf.snapshot, rf.snapshotTerm, rf.snapshotIndex}
		rf.log = make(map[int]LogEntry)
		rf.commitIndex = rf.snapshotIndex
		rf.persist()
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted	bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B)
	if args.Term > rf.currentTerm { 
		// Advance the term:
		// It is important to increment the term so that the algorithm can advance
		// Otherwise, a leader of a prevous term might be stuck trying to 
		// commit an entry from a pervious term. And a candidate might be stuck 
		// trying to get votes on the new term. Imagine the situation of 2 down servers,
		// 2 servers on a new term and 1 leader on an old term
		rf.becomeFollower(args.Term)
	} 
	reply.Term = rf.currentTerm
	// Check if you can grant the vote
	// Must be 1) not for a stale term 
	// 2) the server must be follower to be able to give a vote
	// 3) the server must have an available vote
	// 4) the server's log must be up-to-date (handled in a later part)
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.log[lastLogIndex].Term
	if args.Term == rf.currentTerm && rf.position == Follower && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		DebugPrintf("%d voted for %d %d %d %d %d\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		reply.VoteGranted = true;
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false;
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
	return ok
}


type AppendEntriesArgs struct {
	Term			int
	LeaderCommit	int
	PrevLogIndex	int
	PrevLogTerm		int
	CommandTerm		int
	Command 		interface{}
}

type AppendEntriesReply struct {
	Term 	int
	Success	bool
	XTerm	int
	XIndex	int
	XLen	int
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DebugPrintf("%d command 1\n", rf.me)
	rf.mu.Lock()
	// DebugPrintf("%d command 2\n", rf.me)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.XLen = -1 
	reply.XTerm = -1
	reply.XIndex = -1
	if args.Term == rf.currentTerm {
		// Reset the election timeout
		rf.resetElectionTimer()
		if args.Command != nil { // not a heartbeat
			if args.PrevLogIndex > rf.lastLogIndex() {
				reply.Success = false
				reply.XLen = rf.lastLogIndex()
			} else if entry, ok := rf.log[args.PrevLogIndex]; ok && entry.Term != args.PrevLogTerm {
				reply.Success = false
				// Temporarily disable this optimization
				// reply.XTerm = rf.log[args.PrevLogIndex].Term
				// reply.XIndex = args.PrevLogIndex
				// for rf.log[reply.XIndex - 1].Term == reply.XTerm {
				// 	reply.XIndex--
				// }
			} else {
				for i := args.PrevLogIndex + 1; i <= rf.lastLogIndex(); i++ {
					delete(rf.log, i)
				}
				rf.log[args.PrevLogIndex + 1] = LogEntry{args.CommandTerm, args.Command}
				rf.persist()
				reply.Success = true
				DebugPrintf("%d adding log entry %d\n", rf.me, args.PrevLogIndex + 1)
			}
		} else { // is a heartbeat
			reply.Success = true
		}
		if args.LeaderCommit > rf.commitIndex && args.LeaderCommit <= rf.lastLogIndex() && rf.log[args.LeaderCommit].Term == rf.currentTerm {
			start := rf.commitIndex + 1
			rf.commitIndex = args.LeaderCommit
			for i := start; i <= args.LeaderCommit; i += 1 {
				msg := ApplyMsg{true, rf.log[i].Command, i, false, nil, 0, 0}
				rf.mu.Unlock()
				// Cannot be holding the lock while sending the message, so that 
				// Snapshot() has an opportunity to grab the lock
				rf.applyCh<-msg
				rf.mu.Lock()
			}
		}
	} else {
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		term := rf.currentTerm
		committed := rf.commitIndex
		isleader := rf.position == Leader
		rf.mu.Unlock()
		if isleader {
			for i := 0; i < len(rf.peers); i+=1 {
				if i != rf.me {
					go func(i int) {
						reply := AppendEntriesReply{}
						ok := rf.sendAppendEntries(i, &AppendEntriesArgs{term, committed, 0, 0, 0, nil}, &reply)
						rf.mu.Lock()
						if ok && reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)	
						}
						rf.mu.Unlock()
					}(i)
				}
			}
		}
		// pause, the network is limited to only being able to send tens of heartbeats a second
		ms := 80
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// tells the other servers to add the log entry
func (rf *Raft) requestPeerStart(server int) {
	for rf.killed() == false && rf.nextIndex[server] > rf.lastLogIndex() || rf.position != Leader {
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	logIndex := rf.nextIndex[server]
	if logIndex > rf.lastLogIndex() || rf.position != Leader {
		rf.mu.Unlock()
		rf.requestPeerStart(server)
		return
	}
	// if true {
	if logIndex > rf.snapshotIndex {
		var prevLogTerm int
		if rf.snapshotIndex == logIndex - 1 {
			prevLogTerm = rf.snapshotTerm
		} else {
			prevLogTerm = rf.log[logIndex - 1].Term
		}
		DebugPrintf("%d sending log entry %d(data:%v) to %d\n", rf.me, logIndex, rf.log[logIndex].Command, server)
		args := AppendEntriesArgs{rf.currentTerm, rf.commitIndex, logIndex - 1, prevLogTerm, rf.log[logIndex].Term, rf.log[logIndex].Command}
		rf.mu.Unlock()
		// can try sending the message
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		DebugPrintf("%d received response from %d\n", rf.me, server)
		if ok {
			rf.mu.Lock()
			DebugPrintf("%d received response from %d\n", rf.me, server)
			if rf.currentTerm == args.Term && rf.currentTerm == reply.Term {
				if reply.Success {
					rf.matchIndex[server] = rf.nextIndex[server]
					if rf.matchIndex[server] > rf.commitIndex && rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
						matches := 1
						for i := 0; i < len(rf.peers); i+=1 {
							if i != rf.me && rf.matchIndex[i] >= rf.matchIndex[server] {
								matches++
							}
						}
						if matches > len(rf.peers) / 2 {
							start := rf.commitIndex + 1
							rf.commitIndex = rf.matchIndex[server]
							for i := start; i <= rf.matchIndex[server]; i += 1 {
								msg := ApplyMsg{true, rf.log[i].Command, i, false, nil, 0, 0}
								rf.mu.Unlock()
								rf.applyCh<-msg
								rf.mu.Lock()
							}
						}
					}
					rf.nextIndex[server]++
					
				} else {
					rf.nextIndex[server]--
					// also take the additional XInfo to reduce the # of RPCs
					if reply.XLen != -1 && reply.XLen < rf.nextIndex[server]  {
						rf.nextIndex[server] = reply.XLen
					}
					if reply.XIndex != -1 {
						lastXTermIndex := reply.XIndex
						for lastXTermIndex + 1 <= rf.lastLogIndex() && rf.log[lastXTermIndex + 1].Term == reply.XTerm {
							lastXTermIndex++
						}
						if lastXTermIndex < rf.nextIndex[server] {
							rf.nextIndex[server] = lastXTermIndex
						}
					}
				}
			} else if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	} else {
		// send the snapshot
		DebugPrintf("%d send snapshot to %d (index: %d, term: %d)\n", rf.me, server, rf.snapshotIndex, rf.snapshotTerm)
		args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot}
		rf.mu.Unlock()
		// can try sending the message
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if ok {
			rf.mu.Lock()
			if rf.currentTerm == args.Term && rf.currentTerm == reply.Term {
				rf.nextIndex[server] = args.LastIncludedIndex + 1
				rf.matchIndex[server] = args.LastIncludedIndex
			} else if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	}
	// time.Sleep(time.Duration(3) * time.Millisecond)
	rf.requestPeerStart(server)
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
	index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.position == Leader

	// Your code here (2B).
	if isLeader {
		// add the entry to the log
		rf.log[index] = LogEntry{term, command}
		rf.persist()
	}

	return index, term, isLeader
}

// Handles the logic of a candidate
func (rf *Raft) requestVotes() { 
	rf.mu.Lock()
	if rf.position == Leader { 
		// do not start a new term if you are already the leader
		rf.mu.Unlock()
		return
	}
	DebugPrintf("%d start election!", rf.me)
	rf.currentTerm += 1
	rf.position = Candidate
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	rf.mu.Unlock()
	// votes for itself
	rf.votedFor = rf.me
	var votes int32
	var completed int32
	votes = 1
	completed = 0
	// Asks every other server for a vote
	for i := 0; i < len(rf.peers); i+=1 {
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					if reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					}
				}
				atomic.AddInt32(&completed, 1)
			}(i)
		}
	}

	for rf.currentTerm == args.Term && votes <= int32(len(rf.peers) / 2) && completed < int32(len(rf.peers) - 1) { 
		// polling approach to checking if you have won he election
		// wait to either have enough votes or to have gotten everyone's response
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
	rf.mu.Lock()
	if rf.currentTerm != args.Term {
		// the term changed in some other goroutine while we were collecting votes
		rf.mu.Unlock()
		return
	}
	// If it gets a majority of votes, it becomes the leader
	if votes > int32(len(rf.peers) / 2) {
		rf.position = Leader
		// reset nextIndex and matchIndex
		for i := 0; i < len(rf.peers); i += 1 {
			rf.nextIndex[i] = rf.lastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
	} else {
		rf.position = Follower // failed to get elected
	}
	rf.mu.Unlock()
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		atomic.StoreInt64(&rf.electionTimer, 0)
		ms := 750 + (rand.Int63() % 1000)
		// DebugPrintf("%d start timer %d\n", rf.me, ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if rf.killed() == false && atomic.LoadInt64(&rf.electionTimer) == 0 {
			go rf.requestVotes()
		}
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.position = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	// The actual log entries should start with positive indices.
	// Put a dummy entry in the first position so that this is easy. 
	rf.log = make(map[int]LogEntry)
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0
	rf.snapshot = nil
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	// State finished intialization. 
	// start AppendEntries goroutine
	go rf.heartbeat()
	for i := 0; i < len(rf.peers); i+=1 {
		if i != rf.me {
			go rf.requestPeerStart(i)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
