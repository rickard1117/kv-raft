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
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
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
type CommandType int

const (
	CommandLog      = 1
	CommandSnapshot = 2
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandType  CommandType
}

type raftState string

const (
	follower  raftState = "follower"
	candidate raftState = "candidate"
	leader    raftState = "leader"
)

func init() {
	file := "./raft-" + strconv.FormatInt(time.Now().Unix(), 10) + ".log"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
	// log.SetOutput(ioutil.Discard)
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	return
}

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
	currentTerm               int    // latest term server has seen
	votedFor                  int    // candidateId that received vote in current term (or -1 if none)
	votes                     []bool // whther received vote from each peer
	state                     raftState
	lastRecvHeartBeartTime    time.Time
	lastSendAppendEntriesTime []time.Time
	startElectionTime         time.Time
	log                       []logEntry
	commitIndex               int
	lastApplied               int
	snapshotLastIncludeTerm   int
	nextIndex                 []int
	matchIndex                []int // for checking if one entry can commit
	applyCh                   chan ApplyMsg
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
	// faster roll-back
	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderID int
	Data     []byte
}

type InstallSnapshotReply struct {
	Term int
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

func (rf *Raft) GetIdx() int {
	return rf.me
}

// log after idx will be saved
func (rf *Raft) SaveSnapshot(kvstore interface{}, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if idx < rf.lastApplied {
	// 	log.Fatalf("[%d] idx %d < rf.lastApplied %d", rf.me, idx, rf.lastApplied)
	// }

	if idx <= rf.lastApplied {
		log.Printf("[%d] idx[%d] == rf.lastApplied[%d],  ignore\n", rf.me, idx, idx)
		return
	}

	log.Printf("[%d] going to save snapshot before idx %d\n", rf.me, idx)

	// rf.snapshotLastIncludeTerm = rf.log[len(rf.log)].Term
	rf.snapshotLastIncludeTerm = rf.indexLogTerm(rf.lastLogIndex())
	rf.log = rf.logAfterIdx(idx)
	rf.lastApplied = idx

	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.lastApplied)
	e.Encode(rf.snapshotLastIncludeTerm)
	e.Encode(kvstore)

	rf.persister.SaveStateAndSnapshot(rf.logstate(), buf.Bytes())
}

func (rf *Raft) logstate() []byte {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return buf.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.logstate())
}

func (rf *Raft) StateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	d := labgob.NewDecoder(bytes.NewBuffer(data))
	var currentTerm int
	var votedFor int
	var logs []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		panic("labgob decode error")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logs
	log.Printf("[%d] read log : %+v\n", rf.me, rf.log)
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	rf.applySnapshot(snapshot)
}

// election timeouts in the range of 450 to 900 milliseconds
func electionTimeout() time.Duration {
	return time.Duration(rand.Intn(450)+450) * time.Millisecond
}

func sendHBInterval() time.Duration {
	return time.Duration(100) * time.Millisecond
}

// becomeFollower must be called after rf.mu.Lock()
func (rf *Raft) becomeFollower(term int) {
	if rf.state != follower || term == 0 {
		log.Printf("[%d] %v -> %v\n", rf.me, rf.state, follower)
		rf.state = follower
		rf.lastRecvHeartBeartTime = time.Now()
		go rf.followerLoop()
	}

	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
}

// becomeLeader must be called after rf.mu.Lock()
func (rf *Raft) becomeLeader() {
	if rf.state == leader {
		log.Printf("[%d] already being %v", rf.me, leader)
		return
	}
	log.Printf("[%d] %v -> %v\n", rf.me, rf.state, leader)
	rf.state = leader
	rf.nextIndex = nil
	rf.matchIndex = nil
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex()+1)
		rf.matchIndex = append(rf.matchIndex, 0)

		if i == rf.me {
			continue
		}

		entries, prevLogIndex, prevLogTerm, sendSnapshot := rf.entreisToSend(i)
		if sendSnapshot {
			panic("should not send snapshot when first become leader")
		}
		req := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		go rf.leaderProcessAppendEntries(i, req)    // must send hb to every peer first
		go rf.leaderLoopForPeers(i, rf.currentTerm) // then start peer's loop
	}
}

func (rf *Raft) getLog(index int) (log logEntry, ok bool) {
	if index > rf.lastApplied && index < rf.lastLogIndex() {
		log = rf.log[index-rf.lastApplied-1]
		ok = true
		return
	}
	return
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) + rf.lastApplied
}

func (rf *Raft) indexLogCmd(index int) interface{} {
	logidx := index - rf.lastApplied - 1
	if logidx < 0 {
		log.Printf("[%d] index-rf.lastApplied-1 < 0 index = %d, rf.lastApplied = %d\n", rf.me, index, rf.lastApplied)
	}
	return rf.log[index-rf.lastApplied-1].Command
}

func (rf *Raft) indexLogTerm(index int) int {
	logidx := index - rf.lastApplied - 1
	if logidx == -1 {
		return rf.snapshotLastIncludeTerm
	}
	if logidx < 0 {
		log.Printf("[%d] index-rf.lastApplied-1 < 0 index = %d, rf.lastApplied = %d\n", rf.me, index, rf.lastApplied)
	}
	return rf.log[index-rf.lastApplied-1].Term
}

func (rf *Raft) indexInLog(index int) bool {
	return index > rf.lastApplied
}

func (rf *Raft) logAfterIdx(idx int) []logEntry {
	return rf.log[idx-rf.lastApplied:]
}

func (rf *Raft) logBeforeIdx(idx int) []logEntry {
	// actualIdx := idx - rf.lastApplied
	return rf.log[:idx-rf.lastApplied-1]
}

func (rf *Raft) leaderProcessAppendEntries(server int, req *AppendEntriesArgs) {
	var reply AppendEntriesReply
	log.Printf("[%d] send AppendEntries to %d : %+v\n", rf.me, server, *req)

	rf.mu.Lock()
	rf.lastSendAppendEntriesTime[server] = time.Now()
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", req, &reply)
	if !ok {
		log.Printf("[%d] AppendEntries failed\n", rf.me)
		return
	}
	log.Printf("[%d] recv AppendEntriesReply from %d : %+v\n", rf.me, server, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if rf.currentTerm > req.Term {
		return
	}

	if reply.Success {
		if len(req.Entries) == 0 {
			// heartbeat
			return
		}
		rf.nextIndex[server] = req.PrevLogIndex + len(req.Entries) + 1
		rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
		rf.checkCommit()
		return
	}

	if rf.nextIndex[server] < 1 {
		log.Fatalf("[%d] rf.nextIndex[server] should larger than 1\n", rf.me)
	}

	if reply.XLen < req.PrevLogIndex {
		// Case 3 (follower's log is too short)
		rf.nextIndex[server] = max(reply.XLen, 1)
		log.Printf("[%d] server[%d] nextIndex = %d\n", rf.me, server, rf.nextIndex[server])
		return
	}

	// if reply.XTerm == 0 {
	// 	panic("reply.Term == 0")
	// }

	find := false
	for i := rf.lastLogIndex(); i > rf.lastApplied; i-- {
		if rf.indexLogTerm(i) == reply.XTerm {
			rf.nextIndex[server] = i
			find = true
			break
		}
	}
	if !find {
		rf.nextIndex[server] = max(reply.XIndex, 1)
		// if rf.nextIndex[server] == 0 {
		// 	rf.nextIndex[server] = 1
		// 	log.Printf("[%d] rf.nextIndex[%d] == 0\n", rf.me, server)
		// }
	}
	log.Printf("[%d] server[%d] nextIndex = %d\n", rf.me, server, rf.nextIndex[server])
}

// Lock first
func (rf *Raft) checkCommit() {
	log.Printf("[%d] before checkcommit : %d\n", rf.me, rf.commitIndex)
	beforeCommitIdx := rf.commitIndex
	for index := rf.commitIndex + 1; index <= rf.lastLogIndex(); index++ {
		majority := 0
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= index && rf.indexLogTerm(index) == rf.currentTerm {
				majority++
				if majority >= len(rf.peers)/2 {
					rf.commitIndex = index
					break
				}
			}
		}
	}

	if rf.commitIndex > beforeCommitIdx {
		for i := beforeCommitIdx + 1; i <= rf.commitIndex; i++ {
			if i == 0 {
				continue
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.indexLogCmd(i),
				CommandIndex: i,
			}
			log.Printf("[%d] applyCh <- %+v\n", rf.me, msg)
			rf.applyCh <- msg
		}
	}

	log.Printf("[%d] after checkcommit : %d\n", rf.me, rf.commitIndex)
}

// Lock first
func (rf *Raft) entreisToSend(peer int) (entries []logEntry, prevLogIndex int, prevLogTerm int, sendSnapshot bool) {
	if rf.lastLogIndex() == 0 {
		return
	}
	if rf.nextIndex[peer] <= rf.lastApplied {
		sendSnapshot = true
		return
	}
	if rf.nextIndex[peer] > rf.lastLogIndex() {
		prevLogIndex = rf.lastLogIndex()
		if prevLogIndex == 0 {
			return
		}
		prevLogTerm = rf.indexLogTerm(prevLogIndex)
		return
	}
	prevLogIndex = rf.nextIndex[peer] - 1
	prevLogTerm = rf.indexLogTerm(prevLogIndex)
	entries = rf.logAfterIdx(prevLogIndex)
	return
}

// loop for send AppendEntries to peer server
func (rf *Raft) leaderLoopForPeers(peer int, term int) {
	log.Printf("[%d] start leaderLoopForPeers for %d\n", rf.me, peer)

	time.Sleep(50 * time.Millisecond)
	for {

		time.Sleep(10 * time.Millisecond)

		if rf.killed() {
			log.Printf("[%d] exit leaderLoopForPeers %d\n", rf.me, peer)
			return
		}

		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}

		now := time.Now()
		entries, prevLogIndex, prevLogTerm, sendSnapshot := rf.entreisToSend(peer)
		if len(entries) == 0 && now.Sub(rf.lastSendAppendEntriesTime[peer]) < sendHBInterval() {
			rf.mu.Unlock()
			continue
		}

		if now.Sub(rf.lastSendAppendEntriesTime[peer]) < time.Duration(30)*time.Millisecond {
			rf.mu.Unlock()
			continue
		}
		if !sendSnapshot {
			req := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			go rf.leaderProcessAppendEntries(peer, req)
		} else {
			req := &InstallSnapshotArgs{
				Term:     rf.currentTerm,
				LeaderID: rf.me,
				Data:     rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()
			go rf.processInstallSnapshot(peer, req)
		}
	}
}

func (rf *Raft) processInstallSnapshot(server int, args *InstallSnapshotArgs) {
	log.Printf("[%d] send InstallSnapshot to %d\n", rf.me, server)
	rf.mu.Lock()
	rf.lastSendAppendEntriesTime[server] = time.Now()
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok {
		log.Printf("[%d] send InstallSnapshot failed\n", rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	rf.nextIndex[server] = rf.lastApplied + 1

	log.Printf("[%d] server[%d] InstallSnapshot success", rf.me, server)
}

func (rf *Raft) decodeSnapshot(snapshot []byte) (lastApplied int, lastTerm int, kv []byte) {
	d := labgob.NewDecoder(bytes.NewBuffer(snapshot))

	if err := d.Decode(&lastApplied); err != nil {
		log.Printf("[%d] %+v\n", rf.me, err)
		panic("labgob decode error")
	}

	if err := d.Decode(&lastTerm); err != nil {
		log.Printf("[%d] %+v\n", rf.me, err)
		panic("labgob decode error")
	}

	if err := d.Decode(&kv); err != nil {
		log.Printf("[%d] %+v\n", rf.me, err)
		panic("labgob decode error")
	}
	return
}

func (rf *Raft) replayCommand(cmd interface{}, index int) {
	rf.applyCh <- ApplyMsg{CommandValid: false, Command: cmd, CommandIndex: index, CommandType: CommandLog}
}

func (rf *Raft) replaySnapshot(kv interface{}, lastApplied int) {
	rf.applyCh <- ApplyMsg{CommandValid: false, Command: kv, CommandIndex: lastApplied, CommandType: CommandSnapshot}
}

func (rf *Raft) applySnapshot(snapshot []byte) {
	lastApplied, lastTerm, kv := rf.decodeSnapshot(snapshot)
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.lastApplied = lastApplied
		rf.commitIndex = rf.lastApplied
		rf.snapshotLastIncludeTerm = lastTerm
		log.Printf("[%d] apply snapshot succeed. idx = %d, term = %d\n", rf.me, rf.lastApplied, rf.snapshotLastIncludeTerm)
	}()

	rf.replaySnapshot(kv, lastApplied)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	log.Printf("[%d] recv InstallSnapshot request from %d at term %d\n", rf.me, args.LeaderID, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastRecvHeartBeartTime = time.Now()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		// rf.mu.Unlock()
		log.Printf("[%d] rf.currentTerm[%d] > args.Term[%d]", rf.me, rf.currentTerm, args.Term)
		return
	}
	// oldLastApplied := rf.lastApplied
	// oldLastTerm := rf.snapshotLastIncludeTerm
	// rf.mu.Unlock()

	lastApplied, lastTerm, kv := rf.decodeSnapshot(args.Data)
	// rf.mu.Lock()
	entry, ok := rf.getLog(lastApplied)
	if ok && entry.Term == lastTerm || !ok && lastApplied == rf.lastApplied && lastTerm == rf.snapshotLastIncludeTerm {
		rf.log = rf.logAfterIdx(lastApplied)
		rf.persister.SaveStateAndSnapshot(rf.logstate(), args.Data)
	} else {
		rf.log = nil
		rf.replaySnapshot(kv, lastApplied)
		log.Printf("[%d] replaySnapshot from %d to %d\n", rf.me, rf.lastApplied, lastApplied)
	}

	rf.lastApplied = lastApplied
	rf.commitIndex = rf.lastApplied
	rf.snapshotLastIncludeTerm = lastTerm
}

// should lock first
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = -1
	rf.votes = nil
	for i := 0; i < len(rf.peers); i++ {
		rf.votes = append(rf.votes, false)
	}
	rf.startElectionTime = time.Now()

	term := 0
	if rf.lastLogIndex() > 0 {
		term = rf.indexLogTerm(rf.lastLogIndex())
	}

	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  term,
	}

	rf.votedFor = rf.me
	rf.votes[rf.me] = true
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.processRequestVote(i, req)
	}
}

func (rf *Raft) becomeCandidate() {
	log.Printf("[%d] %v -> %v\n", rf.me, rf.state, candidate)
	rf.state = candidate
	rf.startElection()
	go rf.candidateLoop()
}

func (rf *Raft) votesNum() (n int) {
	for _, b := range rf.votes {
		if b {
			n++
		}
	}
	return
}

func (rf *Raft) processVoteReply(server int, req *RequestVoteArgs, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate {
		log.Printf("[%d] current state : %v is not candidate, no processing vote reply\n", rf.me, rf.state)
		return
	}

	if reply.Term > rf.currentTerm {
		if reply.VoteGranted {
			panic("reply.Term > rf.currentTerm but granted true")
		}
		log.Printf("[%d] some peer's term(%d) > my.term(%d)\n", rf.me, reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Term < rf.currentTerm && rf.currentTerm > req.Term {
		log.Printf("[%d] old RPC reply, do nothing\n", rf.me)
		return
	}

	if !reply.VoteGranted {
		log.Printf("[%d] request not granted\n", rf.me)
		return
	}

	rf.votes[server] = true
	if rf.votesNum() >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	}

}

func (rf *Raft) processRequestVote(server int, req *RequestVoteArgs) {

	var reply RequestVoteReply
	log.Printf("[%d] send request vote to %d : %+v\n", rf.me, server, *req)
	ok := rf.sendRequestVote(server, req, &reply)
	if !ok {
		log.Printf("[%d] sendRequestVote failed\n", rf.me)
		return
	}
	log.Printf("[%d] receive request vote reply from %d : %+v", rf.me, server, reply)
	rf.processVoteReply(server, req, reply)
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

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && !rf.logMoreUpTodate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) logMoreUpTodate(index int, term int) bool {
	if rf.lastLogIndex() == 0 {
		return false
	}

	lastLogTerm := rf.indexLogTerm(rf.lastLogIndex())
	if lastLogTerm > term {
		return true
	} else if lastLogTerm < term {
		return false
	}
	return rf.lastLogIndex() > index
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

func (rf *Raft) matchLog(index int, term int) (result bool, xterm int, xindex int) {
	if index == 0 {
		if term != 0 {
			panic("0 index but not term 0")
		}
		result = true
		return
	}

	if index > rf.lastLogIndex() {
		// Case 3 (follower's log is too short)
		result = false
		return
	}

	if index-rf.lastApplied < 0 {
		result = false
		return
	}

	if rf.indexLogTerm(index) == term {
		result = true
		return
	}

	result = false
	xterm = rf.indexLogTerm(index) // not zero
	xindex = index
	for i := index - 1; i >= 1; i-- {
		if rf.indexLogTerm(i) == xterm {
			xindex = i
		}
	}
	return
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[%d] recv an AppendEntries, %+v\n", rf.me, *args)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		log.Printf("[%d] args.Term=%d < rf.currentTerm=%d\n", rf.me, args.Term, rf.currentTerm)
		return
	}

	rf.lastRecvHeartBeartTime = time.Now()
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == candidate) {
		rf.currentTerm = args.Term
		rf.becomeFollower(args.Term)
	}

	reply.Success, reply.XTerm, reply.XIndex = rf.matchLog(args.PrevLogIndex, args.PrevLogTerm)
	if !reply.Success {
		reply.XLen = rf.lastLogIndex()
		// log.Printf("[%d] prev log [%d, %d] doesn't match any log %+v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log)
		return
	}

	for i := 1; i <= len(args.Entries); i++ {
		idx := args.PrevLogIndex + i
		if idx > rf.lastLogIndex() || rf.indexLogTerm(idx) != args.Entries[i-1].Term {
			rf.log = append(rf.logBeforeIdx(idx), args.Entries[i-1:]...)
			rf.persist()
			// log.Printf("[%d] log append %+v , now is %+v\n", rf.me, args.Entries[i-1:], rf.log)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		for index := rf.commitIndex + 1; index <= args.LeaderCommit; index++ {
			if index > rf.lastLogIndex() {
				break
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.indexLogCmd(index),
				CommandIndex: index,
			}
			log.Printf("[%d] applyCh <- %+v\n", rf.me, msg)
			rf.applyCh <- msg
		}
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}
	log.Printf("[%d] rf.commitIndex = %d\n", rf.me, rf.commitIndex)
}

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
	defer rf.mu.Unlock()

	if rf.state != leader {
		isLeader = false
		return
	}
	isLeader = true
	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, logEntry{Term: rf.currentTerm, Command: cmd})
	rf.persist()
	log.Printf("[%d] leader append cmd %+v at index %d in term %d\n", rf.me, cmd, index, term)
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
	log.Printf("[%d] start %v loop\n", rf.me, follower)
	for {
		time.Sleep(10 * time.Millisecond)
		if rf.killed() {
			log.Printf("[%d] exit followerLoop\n", rf.me)
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
	log.Printf("[%d] start %v loop\n", rf.me, candidate)
	for {
		time.Sleep(10 * time.Millisecond)
		if rf.killed() {
			log.Printf("[%d] exit candidateLoop\n", rf.me)
			return
		}
		continueLoop := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != candidate {
				return false
			}
			if time.Now().Sub(rf.startElectionTime) < electionTimeout() {
				return true
			}
			if rf.votesNum() < len(rf.peers)/2+1 {
				rf.startElection()
			}
			return true
		}()

		if !continueLoop {
			log.Printf("[%d] exit candidate loop\n", rf.me)
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
	rf.startElectionTime = now
	rf.applyCh = applyCh
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.becomeFollower(0)

	// 2B
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	return rf
}
