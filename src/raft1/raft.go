package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"math/rand"
	"sort"
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
	// ticker for server to maintain heartbeat
	// one for self for election timeout
	tickers []*Ticker
	// cancel channnel
	ElectionAbort chan struct{}
	// 记录是否正在重试
	retry []bool
	// 通知channnel
	notify []chan struct{}
	// for leader to stop routine when demoted
	ctx    context.Context
	cancel context.CancelFunc
	// closed by Kill() to stop long-running goroutines
	killCh   chan struct{}
	killOnce sync.Once

	// apply channel
	applyCh chan raftapi.ApplyMsg
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
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
			return
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			select {
			case rf.tickers[rf.me].rst <- struct{}{}:
			default:
			}
		}
	default:
		// first new term candidate
		rf.currentTerm = args.Term
		if rf.state == Leader {
			rf.state = Follower
			rf.cancel()
		}
		rf.state = Follower
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
			return
		}
		reply.VoteGranted = true
		reply.Term = args.Term
		select {
		case rf.tickers[rf.me].rst <- struct{}{}:
		default:
		}
		rf.votedFor = args.CandidateId
		return
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// TODO: 3bcd args
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
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
	if rf.state == Leader {
		rf.cancel()
	}
	rf.state = Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	select {
	case rf.tickers[rf.me].rst <- struct{}{}:
	default:
	}
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		return
	}
	// append new entries
	if len(args.Entries) > 0 {
		DPrintf("[AppendEntries] Server %d from %d: prevLogIndex=%d, self commitIndex=%d", rf.me, args.LeaderId, args.PrevLogIndex, rf.commitIndex)
		if rf.commitIndex > args.PrevLogIndex {
			if rf.commitIndex-args.PrevLogIndex < len(args.Entries) {
				rf.log = rf.log[:rf.commitIndex+1]
				entries := args.Entries[rf.commitIndex-args.PrevLogIndex:]
				rf.log = append(rf.log, entries...)
			}
		} else {
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyLogEntries()
	}

	// TODO: 3bcd
}

// check commitIndex and apply to state machine
func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		// just check self
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
	} else {
		// leader check all followers
		tmpMatchIndex := make([]int, len(rf.peers))
		copy(tmpMatchIndex, rf.matchIndex)
		tmpMatchIndex[rf.me] = len(rf.log) - 1
		sort.Ints(tmpMatchIndex)
		minMatchIndex := tmpMatchIndex[len(tmpMatchIndex)/2]

		if minMatchIndex > rf.commitIndex {
			if rf.log[minMatchIndex].Term != rf.currentTerm {
				return
			}
			rf.commitIndex = minMatchIndex
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			}
		}
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

// send AppendEntries RPC to a server. automatically retry if failed
func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	workerTerm := rf.currentTerm
	workerCtx := rf.ctx
	workerCancel := rf.cancel
	rf.mu.Unlock()
	for rf.killed() == false {
		rf.mu.Lock()
		isRetry := rf.retry[server]
		if rf.currentTerm != workerTerm {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader {
			rf.mu.Unlock()
			workerCancel()
			return
		}
		rf.mu.Unlock()
		select {
		case <-workerCtx.Done():
			return
		default:
		}
		if !isRetry {
			//等待通知
			select {
			case <-rf.notify[server]:
			case <-workerCtx.Done():
				return
			case <-rf.killCh:
				return
			}
			DPrintf("[sendAppendEntries] Server %d to %d\n", rf.me, server)
		} else {
			// 将要重试，清空notify channel
			select {
			case <-rf.notify[server]:
			default:
			}
		}
		select {
		case <-workerCtx.Done():
			return
		default:
		}
		rf.mu.Lock()
		// 检查term是否发生变化
		if rf.currentTerm != workerTerm {
			rf.mu.Unlock()
			return
		}
		if rf.nextIndex[server] > len(rf.log) {
			// illegal state, reset nextIndex
			rf.nextIndex[server] = len(rf.log)
		}
		logs := make([]logEntry, len(rf.log[rf.nextIndex[server]:]))
		copy(logs, rf.log[rf.nextIndex[server]:])
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		LeaderCommit := rf.commitIndex
		term := rf.currentTerm
		state := rf.state
		rf.mu.Unlock()
		if state != Leader {
			workerCancel()
			return
		}
		if isRetry {
			// 只在重试时重置计时器
			select {
			case rf.tickers[server].rst <- struct{}{}:
			default:
			}
		}
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      logs,
			LeaderCommit: LeaderCommit,
		}
		// send with a timeout: labrpc Call to an unreachable/disabled peer
		// can block up to LONGDELAY (7s). Don't let it stall this sender,
		// otherwise a reconnected peer can't be reached in time.
		type aeResult struct {
			ok    bool
			reply AppendEntriesReply
		}
		ch := make(chan aeResult, 1)
		go func() {
			reply := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			ch <- aeResult{ok, reply}
		}()
		var ok bool
		var reply AppendEntriesReply
		select {
		case res := <-ch:
			ok, reply = res.ok, res.reply
		case <-time.After(100 * time.Millisecond):
			ok = false
		case <-workerCtx.Done():
			return
		case <-rf.killCh:
			return
		}
		if ok {
			// DPrintf("[AppendEntries] Server %d to %d: success=%v, term=%d\n", rf.me, server, reply.Success, reply.Term)
			success := reply.Success
			severTerm := reply.Term
			switch success {
			case true:
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					workerCancel()
					return
				}
				if rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[server] = max(rf.nextIndex[server], prevLogIndex+len(logs)+1)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.retry[server] = false
				rf.mu.Unlock()
			case false:
				if severTerm > term {
					rf.mu.Lock()
					if rf.currentTerm == term && rf.state == Leader {
						rf.currentTerm = severTerm
						rf.votedFor = -1
						rf.state = Follower
						workerCancel()
						rf.mu.Unlock()
						// DPrintf("[demote] Server %d: term %d\n", rf.me, severTerm)
						return
					}
					// 超期worker，直接返回
					rf.mu.Unlock()
					// DPrintf("[demote] Server %d: term %d\n", rf.me, severTerm)
					return
				}
				rf.mu.Lock()
				//TODO: better decrement way
				rf.nextIndex[server] = prevLogIndex
				rf.retry[server] = true
				rf.mu.Unlock()
			}
		} else {
			//retry
			rf.mu.Lock()
			rf.retry[server] = true
			if rf.state != Leader {
				workerCancel()
				rf.mu.Unlock()
				return
			}
			if rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}
}

// Start() call boardcastLogEntries() to send new log entries to all followers
// before sending, check commitIndex and apply to state machine
func (rf *Raft) boardcastLogEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.cancel()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.applyLogEntries()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		select {
		case <-rf.ctx.Done():
			return
		default:
		}
		rf.mu.Lock()
		select {
		case rf.notify[i] <- struct{}{}:
		default:
		}
		select {
		case rf.tickers[i].rst <- struct{}{}:
		default:
		}
		rf.mu.Unlock()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, logEntry{
		Term:    term,
		Command: command,
	})
	rf.mu.Unlock()
	go rf.boardcastLogEntries()
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
	rf.killOnce.Do(func() {
		close(rf.killCh)
	})
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
	ElectionTerm := rf.currentTerm
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
	for i := 0; i < len(rf.peers)-1; i++ {
		// 检查存活
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if ElectionTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		select {
		case reply := <-replyChan:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.mu.Unlock()
				// DPrintf("[demote] Server %d: term %d\n", rf.me, rf.currentTerm)
				close(abortChan)
				return
			} else if reply.VoteGranted {
				voteCount++
				if voteCount >= len(rf.peers)/2+1 {
					rf.state = Leader
					// 排空abortElectionChan
					close(abortChan)
					select {
					case <-abortElectionChan:
					default:
					}
					// 重置ticker
					select {
					case rf.tickers[rf.me].rst <- struct{}{}:
					default:
					}
					// get new ctx
					ctx, cancel := context.WithCancel(context.Background())
					rf.ctx = ctx
					rf.cancel = cancel
					// init nextIndex and matchIndex
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						select {
						case <-rf.ctx.Done():
							return
						default:
						}
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
						rf.retry[i] = false
						go rf.sendAppendEntries(i)
						select {
						case <-rf.notify[i]:
						default:
						}
					}
					rf.mu.Unlock()
					DPrintf("[ElectionWon] Server %d: term %d\n", rf.me, rf.currentTerm)
					return
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
	rf.mu.Lock()
	rf.state = Follower
	rf.mu.Unlock()
	close(abortChan)
}

func (rf *Raft) ticker() {
	rst := make(chan struct{}, 1)
	rf.mu.Lock()
	rf.tickers[rf.me] = &Ticker{
		rst:   rst,
		timer: time.NewTimer(time.Duration(300+rand.Int63()%200) * time.Millisecond),
	}
	rf.mu.Unlock()
	ticker := rf.tickers[rf.me]
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

func (rf *Raft) HeartbeatMaintain(sever int) {
	rst := make(chan struct{}, 1)
	rf.tickers[sever] = &Ticker{
		rst:   rst,
		timer: time.NewTimer(time.Duration(100) * time.Millisecond),
	}
	ticker := rf.tickers[sever]
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
				rf.applyLogEntries()
				DPrintf("sending hb to %d", sever)
				select {
				case rf.notify[sever] <- struct{}{}:
				default:
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
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ElectionAbort = make(chan struct{}, 1)
	rf.tickers = make([]*Ticker, len(peers))
	rf.retry = make([]bool, len(peers))
	rf.killCh = make(chan struct{})
	for i := 0; i < len(peers); i++ {
		rf.notify = append(rf.notify, make(chan struct{}, 1))
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	for i := 0; i < len(peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.HeartbeatMaintain(i)
	}

	return rf
}
