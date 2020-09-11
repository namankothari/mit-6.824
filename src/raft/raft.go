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
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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

	state State
	lastHeartBeat time.Time

	// persistant
	currentTerm int
	votedFor int
	log []LogEntry

	// volatile
	commitIndex int
	lastApplied int

	// volatile for leaders
	nextIndex []int
	matchIndex []int

	// chan
	applyCh chan ApplyMsg
}

// Log interface
type LogEntry struct {
	Command interface{}
	Term int
	Index int
}

type State string

const (
	Follower State = "follower"
	Candidate = "candidate"
	Leader = "leader"
)

const NotVoted = -1
const ElectionInterval = 1000


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
	}

	if rf.votedFor == NotVoted || rf.votedFor == args.CandidateId {

		lastLogEntry := rf.GetLastLogEntry()
		if args.LastLogTerm > lastLogEntry.Term {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		} else if args.LastLogTerm == lastLogEntry.Term {
			if args.LastLogIndex >= lastLogEntry.Index {
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				return
			}
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.ConvertToFollower(args.Term)
	reply.Term = rf.currentTerm

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		logEntry := rf.log[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command: logEntry.Command,
			CommandIndex: logEntry.Index,
		}
		rf.applyCh <- applyMsg
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	} else{

		prevLogEntry := rf.GetLogEntry(args.PrevLogIndex)
		if prevLogEntry.Index == args.PrevLogIndex && prevLogEntry.Term == args.PrevLogTerm {

			currentLogIndex := prevLogEntry.Index + 1
			newEntryLogIndex := 0

			for currentLogIndex <= rf.GetLastLogEntry().Index && newEntryLogIndex < len(args.Entries) {
				if rf.GetLogEntry(currentLogIndex).Index == args.Entries[newEntryLogIndex].Index &&
					rf.GetLogEntry(currentLogIndex).Term == args.Entries[newEntryLogIndex].Term {
					currentLogIndex++
					newEntryLogIndex++
				} else {
					break
				}
			}

			if currentLogIndex > rf.GetLastLogEntry().Index {
				rf.log = append(rf.log, args.Entries[newEntryLogIndex:]...)
			} else if currentLogIndex <= rf.GetLastLogEntry().Index && newEntryLogIndex < len(args.Entries) {
				rf.log = append(rf.log[:currentLogIndex], args.Entries[newEntryLogIndex:]...)
			}
			reply.Success = true
		} else {
			rf.log = rf.log[:prevLogEntry.Index]
			reply.Success = false
			return
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.GetLastLogEntry().Index)))
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader

	if !isLeader{
		return -1, -1, false
	}

	lastLogEntry := rf.GetLastLogEntry()
	index := lastLogEntry.Index
	newLogEntry := LogEntry{
		Command: command,
		Term: rf.currentTerm,
		Index: index +1,
	}

	rf.log = append(rf.log, newLogEntry)

	go rf.startAgreement(command, index + 1)

	return index + 1, rf.currentTerm, true
}

func (rf *Raft) startAgreement(command interface{}, commandIndex int) {

	hasAppended := make([]bool, len(rf.peers))
	votes := 1
	for {
		rf.mu.Lock()

		if rf.state != Leader || rf.killed() || votes == len(rf.peers){
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for peer, _ := range rf.peers {
			if peer != rf.me && !hasAppended[peer]{
				go func(server int, cmd interface{}) {
					rf.mu.Lock()

					nextIndex := rf.nextIndex[server]
					prevLogEntry := rf.GetLogEntry(nextIndex - 1)

					lastLogIndex := rf.GetLastLogEntry().Index

					args := AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: prevLogEntry.Index,
						PrevLogTerm: prevLogEntry.Term,
						Entries: rf.log[nextIndex:],
						LeaderCommit: rf.commitIndex,
					}

					reply := AppendEntriesReply{}

					rf.mu.Unlock()

					ok := rf.sendAppendEntries(server, &args, &reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if !ok {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.ConvertToFollower(reply.Term)
						return
					}

					if reply.Success  {
						if !hasAppended[server] {

							hasAppended[server] = true
							rf.nextIndex[server] = int(math.Max(float64(rf.nextIndex[server]), float64(lastLogIndex + 1)))
							rf.matchIndex[server] = int(math.Max(float64(rf.matchIndex[server]), float64(lastLogIndex)))
							votes++
							if votes > len(rf.peers) / 2 {
								rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(commandIndex)))
							}
						}
					} else {
						rf.nextIndex[server]--
					}
					return
				}(peer, command)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NotVoted
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, LogEntry{Index:0, Term:0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.lastHeartBeat = time.Now()

	go rf.LeaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}


func (rf *Raft) LeaderElection() {

	for {
		if rf.killed(){
			return
		}

		electionTimeOut := ElectionInterval - rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)

		rf.mu.Lock()

		if rf.lastHeartBeat.Before(startTime) {
			if rf.state != Leader {
				go rf.StartElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartElection() {

	rf.mu.Lock()
	rf.ConvertToCandidate()

	lastLogEntry := rf.GetLastLogEntry()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm: lastLogEntry.Term,
	}
	votes := 1
	rf.mu.Unlock()
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func (server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if !ok{
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.ConvertToFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					DPrintf("Vote got from %d and majority is %d", server, len(rf.peers) / 2)
					votes++
					if votes > len(rf.peers) / 2 && rf.state == Candidate {
						DPrintf(" **** LEADER ELECTED server %d***", rf.me)
						rf.ConvertToLeader()
						go rf.OperateLeader()
						return
					}
				}
			} (peer)
		}
	}
}

func (rf *Raft) OperateLeader() {

	for {
		rf.mu.Lock()
		lastLogEntry := rf.GetLastLogEntry()
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: lastLogEntry.Index,
			PrevLogTerm: lastLogEntry.Term,
			Entries:[]LogEntry{},
		}
		if rf.state != Leader || rf.killed(){
			rf.mu.Unlock()
			return
		}

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			logEntry := rf.log[rf.lastApplied]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command: logEntry.Command,
				CommandIndex: logEntry.Index,
			}
			rf.applyCh <- applyMsg
		}

		rf.mu.Unlock()
		for peer, _ := range rf.peers {
			if peer != rf.me {
				go func(server int) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.ConvertToFollower(reply.Term)
						return
					}
				}(peer)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) GetLastLogEntry() LogEntry{
	return rf.log[len(rf.log) - 1]
}

func (rf *Raft) GetLogEntry(index int) LogEntry{
	return rf.log[index]
}

func (rf *Raft) ConvertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = NotVoted
	rf.lastHeartBeat = time.Now()
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartBeat = time.Now()
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader
	lastLogEntry := rf.GetLastLogEntry()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogEntry.Index + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	rf.lastHeartBeat = time.Now()
}
