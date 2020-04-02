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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type raftState string

const (
	follower  raftState = "follower"
	candidate raftState = "candidate"
	leader    raftState = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currentTerm               int // latest term server has seen
	votedFor                  int // candidateId that received vote in current term (or -1 if none)
	votes                     int // number of votes received
	state                     raftState
	lastRecvHeartBeartTime    time.Time
	lastSendAppendEntriesTime []time.Time
	timeBeCandidate           time.Time
	// 2B
	log         []logEntry
	commitIndex int
	// lastApplied int
	// only for leader, reinitialized after election:
	nextIndex  []int
	matchIndex []int // for checking if one entry can commit
	applyCh    chan ApplyMsg
}

type logEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	// 2A
	Term     int
	LeaderID int
	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// 2A
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int // candidate's term
	CandidateID int // candidate requesting vote
	// 2B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// election timeouts in the range of 450 to 900 milliseconds
func electionTimeout() time.Duration {
	return time.Duration(rand.Intn(450)+450) * time.Millisecond
}

func sendHBInterval() time.Duration {
	return time.Duration(200 * time.Millisecond)
}

// becomeFollower must be called after rf.mu.Lock()
func (rf *Raft) becomeFollower(term int) {
	if rf.state != follower || term == 0 {
		rf.state = follower
		DPrintf("[%d] %v -> %v\n", rf.me, rf.state, follower)
		go rf.followerLoop()
	}
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votes = 0
}

// becomeLeader must be called after rf.mu.Lock()
func (rf *Raft) becomeLeader() {
	if rf.state == leader {
		DPrintf("[%d] already being %v", rf.me, leader)
		return
	}
	DPrintf("[%d] %v -> %v\n", rf.me, rf.state, leader)
	rf.state = leader
	rf.votedFor = -1
	rf.votes = 0

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex()+1) // log's index start from 1
		rf.matchIndex = append(rf.matchIndex, 0)
		if i == rf.me {
			continue
		}
		go rf.leaderLoopForPeers(i, rf.currentTerm)
	}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log)
}

func (rf *Raft) leaderProcessAppendEntries(server int, req *AppendEntriesArgs) {
	var reply AppendEntriesReply
	DPrintf("[%d] send AppendEntries %+v\n", rf.me, *req)

	rf.lastSendAppendEntriesTime[server] = time.Now()
	ok := rf.peers[server].Call("Raft.AppendEntries", req, &reply)
	if !ok {
		DPrintf("[%d] AppendEntries failed\n", rf.me)
		return
	}
	DPrintf("[%d] recv AppendEntriesReply %+v\n", rf.me, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		rf.nextIndex[server] = req.PrevLogIndex + len(req.Entries)
		rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
		rf.checkCommit()
	} else {
		if req.Entries == nil {
			// just a heartbeat
			return
		}
		if rf.nextIndex[server] > 0 {
			rf.nextIndex[server]--
			DPrintf("[%d] server[%d] nextIndex--, now is %d\n", rf.me, server, rf.nextIndex[server])
		}
	}
}

func (rf *Raft) indexLog(index int) logEntry {
	return rf.log[index-1]
}

// Lock first
func (rf *Raft) checkCommit() {
	DPrintf("[%d] before checkcommit : %d\n", rf.me, rf.commitIndex)
	for index := rf.commitIndex; index <= len(rf.log); index++ {
		majority := 0
		for server := 0; server < len(rf.peers); server++ {
			if rf.matchIndex[server] >= index && rf.indexLog(index).Term == rf.currentTerm {
				majority++
				if majority > len(rf.peers)/2+1 {
					rf.commitIndex = index
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.indexLog(index),
						CommandIndex: index,
					}
					rf.applyCh <- msg
					break
				}
			}
		}
	}
	DPrintf("[%d] after checkcommit : %d\n", rf.me, rf.commitIndex)
}

// Lock first
func (rf *Raft) entreisToSend(peer int) (entries []logEntry, prevLogIndex int, prevLogTerm int) {
	if rf.nextIndex[peer] > rf.lastLogIndex() {
		// no entries to send
		return
	}

	prevLogIndex = rf.nextIndex[peer] - 1
	entries = rf.log[prevLogIndex:]

	if prevLogIndex == 0 {
		// no prevLog, entries contains all rf.log
		return
	}

	prevLogTerm = rf.indexLog(prevLogIndex).Term
	return
}

// loop for send AppendEntries to peer server
func (rf *Raft) leaderLoopForPeers(peer int, term int) {
	// send a heartbeat first when it become leader(start leader loop)
	aeargs := AppendEntriesArgs{
		Term:     term,
		LeaderID: rf.me,
	}
	go rf.leaderProcessAppendEntries(peer, &aeargs)

	for {
		time.Sleep(10 * time.Millisecond)

		if rf.killed() || rf.state != leader {
			return
		}

		if time.Now().Sub(rf.lastSendAppendEntriesTime[peer]) < sendHBInterval() {
			continue
		}
		
		req := func() *AppendEntriesArgs {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			entries, prevLogIndex, prevLogTerm := rf.entreisToSend(peer)
			if len(entries) == 0 {
				return &AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
				}
			}
			return &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
		}()

		go rf.leaderProcessAppendEntries(peer, req)
	}
}

// should lock first
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = -1
	rf.votes = 0
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.votedFor = i
			rf.votes++
			continue
		}
		go rf.processRequestVote(i, rf.currentTerm)
	}
}

func (rf *Raft) becomeCandidate() {
	DPrintf("[%d] %v -> %v\n", rf.me, rf.state, candidate)
	rf.state = candidate
	rf.startElection()
	go rf.candidateLoop()
}

func (rf *Raft) processVoteReply(req RequestVoteArgs, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate {
		DPrintf("[%d] current state : %v is not candidate, no processing vote reply\n", rf.me, rf.state)
		return
	}

	if reply.Term > rf.currentTerm {
		if reply.VoteGranted {
			panic("reply.Term > rf.currentTerm but granted true")
		}
		DPrintf("[%d] some peer's term(%d) > my.term(%d)\n", rf.me, reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Term < rf.currentTerm && rf.currentTerm > req.Term {
		DPrintf("[%d] old RPC reply, do nothing\n", rf.me)
		return
	}

	if !reply.VoteGranted {
		DPrintf("[%d] request not granted\n", rf.me)
		return
	}

	rf.votes++
	if rf.votes >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	}

}

func (rf *Raft) processRequestVote(server int, term int) {
	req := RequestVoteArgs{Term: term, CandidateID: rf.me}
	var reply RequestVoteReply
	DPrintf("[%d] send request vote %+v\n", rf.me, req)
	ok := rf.sendRequestVote(server, &req, &reply)
	if !ok {
		DPrintf("[%d] sendRequestVote failed\n", rf.me)
		return
	}
	DPrintf("[%d] receive request vote reply %+v", rf.me, reply)
	rf.processVoteReply(req, reply)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	rf.lastRecvHeartBeartTime = time.Now()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] recv an AppendEntries, %+v\n", rf.me, *args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeFollower(args.Term)
	}

	rf.lastRecvHeartBeartTime = time.Now()
	if args.Entries == nil {
		// it's a heartbeat packet
		reply.Success = false
		return
	}

	if args.PrevLogIndex > 0 {
		log := rf.indexLog(args.PrevLogIndex)
		if log.Term != args.Term {
			reply.Success = false
			return
		}
	}
	reply.Success = true
	rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		for index := rf.commitIndex + 1; index < args.LeaderCommit; index++ {
			log := rf.indexLog(index)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: index,
			}
		}
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
	}
}

// func (rf *Raft) leaderSendHB(server int, term int) {
// 	req := AppendEntriesArgs{Term: term, LeaderID: rf.me}
// 	var reply AppendEntriesReply

// 	DPrintf("[%d] send AppendEntries %+v\n", rf.me, req)

// 	ok := rf.peers[server].Call("Raft.AppendEntries", &req, &reply)
// 	if !ok {
// 		DPrintf("[%d] AppendEntries failed\n", rf.me)
// 		return
// 	}
// 	DPrintf("[%d] recv AppendEntriesReply %+v\n", rf.me, reply)
// }

//
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
//
func (rf *Raft) Start(cmd interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).

	rf.mu.Lock()
	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, logEntry{Term: rf.currentTerm, Command: cmd})
	// rf.lastApplied++
	rf.mu.Unlock()

	for {
		time.Sleep(10 * time.Millisecond)
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.state != leader {
			isLeader = false
			rf.mu.Unlock()
			return
		}

		isLeader = true
		if rf.commitIndex >= index {
			DPrintf("[%d] log index %d at term %d has been commited\n", rf.me, index, term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	return
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) followerLoop() {
	DPrintf("[%d] start %v loop\n", rf.me, follower)
	for {
		time.Sleep(10 * time.Millisecond)
		if rf.killed() {
			return
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != follower {
				return
			}
			if time.Now().Sub(rf.lastRecvHeartBeartTime) < electionTimeout() {
				return
			}
			rf.becomeCandidate()
		}()
	}
}

func (rf *Raft) candidateLoop() {
	DPrintf("[%d] start %v loop\n", rf.me, candidate)
	for {
		time.Sleep(10 * time.Millisecond)
		if rf.killed() {
			return
		}
		continueLoop := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != candidate {
				return false
			}
			if time.Now().Sub(rf.timeBeCandidate) < electionTimeout() {
				return true
			}
			if rf.votes < len(rf.peers)/2+1 {
				rf.startElection()
			}
			return true
		}()

		if !continueLoop {
			DPrintf("[%d] exit candidate loop\n", rf.me)
			return
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	now := time.Now()
	rf.lastRecvHeartBeartTime = now
	for i := 0; i < len(rf.peers); i++ {
		rf.lastSendAppendEntriesTime = append(rf.lastSendAppendEntriesTime, now)
	}
	rf.timeBeCandidate = now
	rf.applyCh = applyCh
	rf.becomeFollower(0)
	go rf.followerLoop()

	// 2B
	rf.commitIndex = 0
	// rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
