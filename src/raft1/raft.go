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

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type logEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// nv states
	currentTerm int
	votedFor    int
	log         []logEntry // first index is 1, so log[0] is a dummy entry
	// volatile states
	commitIndex int
	lastApplied int
	// volatile states on leaders
	nextIndex  []int
	matchIndex []int

	state int
	// timer reset channel
	ElectionRst   chan struct{}
	HeartRst      chan struct{}
	ElectionAbort chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	// if isleader {
	// 	DPrintf("Server %d is leader for term %d\n", rf.me, rf.currentTerm)
	// }
	return term, isleader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	//TODO: 3bcd
	switch args.Term {
	case rf.currentTerm:
		reply.Term = rf.currentTerm
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			select {
			case rf.ElectionRst <- struct{}{}:
			default:
			}
		}
	default:
		// first new term candidate
		reply.VoteGranted = true
		reply.Term = args.Term
		select {
		case rf.ElectionRst <- struct{}{}:
		default:
		}
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		return
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// TODO: 3bcd args
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.state = Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	select {
	case rf.ElectionRst <- struct{}{}:
	default:
	}
	// TODO: 3bcd
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

type Ticker struct {
	rst   chan struct{}
	timer *time.Timer
}

func (rf *Raft) GetVote(peer int, replyChan chan RequestVoteReply, abortChan chan struct{}) {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	result := RequestVoteReply{}
	result.VoteGranted = false
	ok := rf.peers[peer].Call("Raft.RequestVote", &args, &reply)
	if ok {
		result = reply
	}
	select {
	case <-abortChan:
		return
	default:
	}
	replyChan <- result

}

func (rf *Raft) StartElection(abortElectionChan chan struct{}) {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.mu.Unlock()
	// fmt.Printf("Server %d starting election for term %d\n", rf.me, rf.currentTerm)
	abortChan := make(chan struct{})
	replyChan := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.GetVote(i, replyChan, abortChan)
	}
	voteCount := 1
OUTER:
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case reply := <-replyChan:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.mu.Unlock()
				DPrintf("[demote] Server %d: term %d\n", rf.me, rf.currentTerm)
				close(abortChan)
				break OUTER
			} else if reply.VoteGranted {
				voteCount++
				if voteCount >= len(rf.peers)/2+1 {
					rf.state = Leader
					rf.mu.Unlock()
					close(abortChan)
					DPrintf("[ElectionWon] Server %d: term %d\n", rf.me, rf.currentTerm)
					break OUTER
				}
			}
			rf.mu.Unlock()
		case <-abortElectionChan:
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
			close(abortChan)
			return
		}
	}
	// broadcast heartbeat to all
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			select {
			case <-abortElectionChan:
				return
			default:
			}
			go rf.BoardcastHeartbeat(i)
		}
	} else {
		rf.state = Follower
	}
}

// func (rf *Raft) ElectionWorker() {
// 	abortChan := make(chan struct{}, 1)
// 	for rf.killed() == false {
// 		select {
// 		case <-rf.ElectionAbort:
// 			// election aborted, do nothing
// 			select {
// 			case abortChan <- struct{}{}:
// 			default:
// 			}
// 		default:
// 			rf.StartElection(rf.ElectionAbort)
// 		}
// 	}
// }

func (rf *Raft) BoardcastHeartbeat(i int) {
	rf.mu.Lock()
	state := rf.state
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if state != Leader {
		return
	}
	reply := AppendEntriesReply{}
	ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		if reply.Term > currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.mu.Unlock()
			DPrintf("[demote] Server %d: term %d\n", rf.me, reply.Term)
		}
	}
}

func (rf *Raft) ticker(rst chan struct{}) {
	ticker := Ticker{
		rst:   rst,
		timer: time.NewTimer(time.Duration(300+rand.Int63()%200) * time.Millisecond),
	}
	defer ticker.timer.Stop()
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 800 and 2500
		// milliseconds.
		select {
		case <-ticker.rst:
			if !ticker.timer.Stop() {
				select {
				case <-ticker.timer.C:
				default:
				}
			}
			ticker.timer.Reset(time.Duration(300+rand.Int63()%200) * time.Millisecond)
			// fmt.Printf("Server %d received heartbeat, reset timer\n", rf.me)
		case <-ticker.timer.C:
			if !ticker.timer.Stop() {
				select {
				case <-ticker.timer.C:
				default:
				}
			}
			ticker.timer.Reset(time.Duration(300+rand.Int63()%200) * time.Millisecond)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state != Leader {
				if state == Candidate {
					// a election is already in progress, abort it
					// DPrintf("Server %d aborting election for term %d\n", rf.me, rf.currentTerm)
					select {
					case rf.ElectionAbort <- struct{}{}:
					default:
					}
				}

				go rf.StartElection(rf.ElectionAbort)
			}
		}
	}
}

func (rf *Raft) HeartbeatMaintain(heartRst chan struct{}) {
	ticker := Ticker{
		rst:   heartRst,
		timer: time.NewTimer(time.Duration(100) * time.Millisecond),
	}
	defer ticker.timer.Stop()
	for rf.killed() == false {
		select {
		case <-ticker.rst:
			if !ticker.timer.Stop() {
				select {
				case <-ticker.timer.C:
				default:
				}
			}
			ticker.timer.Reset(time.Duration(100) * time.Millisecond)
		case <-ticker.timer.C:
			if !ticker.timer.Stop() {
				select {
				case <-ticker.timer.C:
				default:
				}
			}
			ticker.timer.Reset(time.Duration(100) * time.Millisecond)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state == Leader {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.BoardcastHeartbeat(i)
				}
			}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 1) // log[0] is a dummy entry

	rf.commitIndex = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ElectionRst = make(chan struct{}, 1)
	rf.HeartRst = make(chan struct{}, 1)
	rf.ElectionAbort = make(chan struct{}, 1)

	// start ticker goroutine to start elections
	go rf.ticker(rf.ElectionRst)

	// start heartbeat maintenance goroutine
	go rf.HeartbeatMaintain(rf.HeartRst)

	return rf
}
