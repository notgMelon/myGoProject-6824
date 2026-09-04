package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	CurrentTerm int
	VotedFor    int
	Log         []logEntry // first index is 1, so log[0] is a dummy entry
	// volatile states
	commitIndex int
	lastApplied int
	// noopIndex   int
	// 通知applyWorker
	notifyApply chan struct{}
	// volatile states on leaders
	nextIndex  []int
	matchIndex []int

	state int
	// states for snapshot
	SnapshotIndex int
	SnapshotTerm  int
	SnapshotByte  []byte
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
	term = rf.CurrentTerm
	isleader = rf.state == Leader
	// if isleader {
	// 	DPrintf("Server %d is leader for term %d\n", rf.me, rf.CurrentTerm)
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
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.SnapshotIndex)
	raftstate := buf.Bytes()
	var snapshot []byte
	if rf.SnapshotByte != nil {
		snapshot = rf.SnapshotByte
	}
	rf.persister.Save(raftstate, snapshot)
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
	var log []logEntry
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil {
		DPrintf("[readPersist] Server %d: decode error\n", rf.me)
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.SnapshotIndex = snapshotIndex
	}
	snapshot := rf.persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rf.SnapshotByte = snapshot
		rf.SnapshotTerm = rf.Log[0].Term
		// notify applyWorker to apply snapshot
		select {
		case rf.notifyApply <- struct{}{}:
		default:
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.SnapshotIndex >= index || index > rf.Loglen()-1 {
		return
	}
	trimIndex := index - rf.SnapshotIndex
	// Trim the log up to the snapshot index
	rf.Log = rf.Log[trimIndex:]
	rf.SnapshotTerm = rf.Log[0].Term
	rf.SnapshotIndex = index
	rf.SnapshotByte = snapshot
	rf.persist()
	// 从上层调用的，不需要再传输到上层
	rf.lastApplied = max(rf.lastApplied, index)
	rf.commitIndex = max(rf.commitIndex, index)
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
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		return
	}
	//TODO: 3bcd
	switch args.Term {
	case rf.CurrentTerm:
		if len(rf.Log) == 0 {
			// 根据snapshot的index和term判断是否可以投票
			if args.LastLogTerm < rf.SnapshotTerm ||
				(args.LastLogTerm == rf.SnapshotTerm && args.LastLogIndex < rf.SnapshotIndex) {
				return
			}
		} else {
			if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
				(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < rf.Loglen()-1) {
				return
			}
		}
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.persist()
			select {
			case rf.tickers[rf.me].rst <- struct{}{}:
			default:
			}
		}
	default:
		// first new term candidate
		rf.CurrentTerm = args.Term
		rf.persist()
		if rf.state == Leader {
			rf.state = Follower
			rf.cancel()
		}
		rf.state = Follower
		if len(rf.Log) == 0 {
			// 根据snapshot的index和term判断是否可以投票
			if (args.LastLogIndex < rf.SnapshotIndex && args.LastLogTerm == rf.SnapshotTerm) ||
				args.LastLogTerm < rf.SnapshotTerm {
				return
			}
		} else {
			if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
				(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < rf.Loglen()-1) {
				return
			}
		}
		reply.VoteGranted = true
		reply.Term = args.Term
		select {
		case rf.tickers[rf.me].rst <- struct{}{}:
		default:
		}
		rf.VotedFor = args.CandidateId
		rf.persist()
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
	XTerm   int
	XIndex  int
	XLength int
}

// 找到log中某个term的第一个index
func (rf *Raft) findFirstIndex(term int, index int) int {
	if rf.SnapshotTerm >= term {
		return -1
	}
	low, high := 0, index
	for low < high {
		mid := (low + high) / 2
		if rf.Log[mid].Term < term {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == 0 && rf.SnapshotIndex == 0 {
		low = 1
	}
	return low
}

// 找到log中某个term的最后一个index
// 如果没有这个term的log，返回小于该term的最大index
func (rf *Raft) findLastIndex(term int) int {
	if rf.SnapshotTerm > term || len(rf.Log) == 0 {
		return -1
	}
	low, index := 0, len(rf.Log)-1
	for {
		tmp := (low + index) / 2
		if rf.Log[tmp].Term <= term {
			low = tmp
		} else {
			index = tmp
		}
		if index-low <= 10 {
			for i := index; i >= low; i-- {
				if rf.Log[i].Term <= term {
					return i
				}
			}
			return low
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	if rf.state == Leader {
		rf.cancel()
	}
	rf.state = Follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	reply.Term = rf.CurrentTerm
	reply.Success = true
	select {
	case rf.tickers[rf.me].rst <- struct{}{}:
	default:
	}
	if args.PrevLogIndex >= rf.Loglen() {
		reply.Success = false
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XLength = rf.Loglen()
		return
	}
	if len(rf.Log) > rf.idxInLog(args.PrevLogIndex) && rf.Log[rf.idxInLog(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.Log[rf.idxInLog(args.PrevLogIndex)].Term
		reply.XIndex = rf.findFirstIndex(reply.XTerm, rf.idxInLog(args.PrevLogIndex)) + rf.SnapshotIndex
		reply.XLength = 0
		return
	}
	// append new entries
	if len(args.Entries) > 0 {
		rf.mu.Unlock()
		DPrintf("[AE] Server %d from %d: lastone = %v, %d", rf.me, args.LeaderId, args.Entries[len(args.Entries)-1].Command, len(args.Entries)+args.PrevLogIndex)
		rf.mu.Lock()
		if rf.commitIndex > args.PrevLogIndex {
			if len(args.Entries) <= rf.commitIndex-args.PrevLogIndex {
				// 不能覆盖已经提交的日志
				return
			}
			rf.Log = rf.Log[:rf.commitIndex+1-rf.SnapshotIndex]
			entries := args.Entries[rf.commitIndex-args.PrevLogIndex:]
			rf.Log = append(rf.Log, entries...)
		} else {
			rf.Log = rf.Log[:args.PrevLogIndex+1-rf.SnapshotIndex]
			rf.Log = append(rf.Log, args.Entries...)
		}
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.Loglen()-1)
		select {
		case rf.notifyApply <- struct{}{}:
		default:
		}
		// rf.mu.Unlock()
		// DPrintf("[AE] Server %d start apply logs", rf.me)
		// rf.mu.Lock()
	}

	// TODO: 3bcd
}

// check commitIndex and apply to state machine
func (rf *Raft) applyLogEntries() {
	for rf.killed() == false {
		select {
		case <-rf.killCh:
			return
		case <-rf.notifyApply:
			rf.mu.Lock()
			if rf.SnapshotByte != nil && rf.lastApplied < rf.SnapshotIndex {
				applymsg := raftapi.ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      rf.SnapshotByte,
					SnapshotTerm:  rf.SnapshotTerm,
					SnapshotIndex: rf.SnapshotIndex,
				}
				rf.mu.Unlock()
				rf.applyCh <- applymsg
				rf.mu.Lock()
				rf.lastApplied = rf.SnapshotIndex
			}
			if rf.state != Leader {
				// just check self
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					// if rf.Log[rf.lastApplied].Command == nil {
					// 	continue
					// }
					// rf.applyIndex++
					applyMsg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.Log[rf.idxInLog(rf.lastApplied)].Command,
						// CommandIndex: rf.applyIndex,
						CommandIndex: rf.lastApplied,
					}
					rf.mu.Unlock()
					DPrintf("[APPLY] server %d -> %v, %d", rf.me, applyMsg.Command, applyMsg.CommandIndex)
					rf.applyCh <- applyMsg
					rf.mu.Lock()
				}
			} else {
				// leader check all followers
				tmpMatchIndex := make([]int, len(rf.peers))
				copy(tmpMatchIndex, rf.matchIndex)
				tmpMatchIndex[rf.me] = rf.Loglen() - 1
				sort.Ints(tmpMatchIndex)
				minMatchIndex := tmpMatchIndex[len(tmpMatchIndex)/2]
				if minMatchIndex > rf.commitIndex {
					if rf.Log[rf.idxInLog(minMatchIndex)].Term != rf.CurrentTerm {
						rf.mu.Unlock()
						continue
					}
					rf.commitIndex = minMatchIndex
					for rf.lastApplied < rf.commitIndex {
						rf.lastApplied++
						// if rf.Log[rf.lastApplied].Command == nil {
						// 	continue
						// }
						// rf.applyIndex++
						applyMsg := raftapi.ApplyMsg{
							CommandValid: true,
							Command:      rf.Log[rf.idxInLog(rf.lastApplied)].Command,
							// CommandIndex: rf.applyIndex,
							CommandIndex: rf.lastApplied,
						}
						rf.mu.Unlock()
						DPrintf("[APPLY] server %d -> %v, %d", rf.me, applyMsg.Command, applyMsg.CommandIndex)
						rf.applyCh <- applyMsg
						rf.mu.Lock()
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// 处理InstallSnapshot RPC请求
// 截断后的log至少保留一条日志，作为快照的最后一条日志
// 如果日志太短，则清空log,然后log[0]作为dummy entry
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[IS] %d: lastone %d, %d, from %d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	select {
	case rf.notifyApply <- struct{}{}:
	default:
	}
	reply.Term = args.Term
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	if rf.state == Leader {
		rf.cancel()
	}
	rf.state = Follower
	rf.tickers[rf.me].rst <- struct{}{}
	if (rf.SnapshotByte != nil && args.LastIncludedIndex <= rf.SnapshotIndex) ||
		(args.LastIncludedIndex == rf.SnapshotIndex && args.LastIncludedTerm == rf.SnapshotTerm) {
		// 忽略相同或更旧的快照
		return
	}
	if args.LastIncludedIndex == rf.SnapshotIndex && args.LastIncludedTerm != rf.SnapshotTerm {
		// 快照冲突，重置lastapplied
		rf.lastApplied = args.LastIncludedIndex - 1
	}
	rf.SnapshotByte = args.Data
	rf.SnapshotTerm = args.LastIncludedTerm
	var isConfilict bool
	isConfilict = false
	if rf.idxInLog(rf.SnapshotIndex) >= 0 && rf.idxInLog(args.LastIncludedIndex) >= 0 &&
		rf.Log[rf.idxInLog(rf.SnapshotIndex)].Term != args.LastIncludedTerm {
		isConfilict = true
	}
	// 出现冲突时follower丢弃所有log条目
	if len(rf.Log)+rf.SnapshotIndex <= args.LastIncludedIndex || isConfilict {
		rf.Log = make([]logEntry, 1)
		// dummy copy of last included entry
		rf.Log[0].Term = args.LastIncludedTerm
		rf.Log[0].Command = nil
	} else {
		trimIndex := args.LastIncludedIndex - rf.SnapshotIndex
		rf.Log = rf.Log[trimIndex:]
	}
	rf.SnapshotIndex = args.LastIncludedIndex
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.persist()
}

// get the length of the log, including the snapshot index
func (rf *Raft) Loglen() int {
	return len(rf.Log) + rf.SnapshotIndex
}

// 计算log中某个index对应的log 在 log切片中的位置
func (rf *Raft) idxInLog(index int) int {
	return index - rf.SnapshotIndex
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
	workerTerm := rf.CurrentTerm
	workerCtx := rf.ctx
	workerCancel := rf.cancel
	rf.mu.Unlock()
	for rf.killed() == false {
		rf.mu.Lock()
		isRetry := rf.retry[server]
		if rf.CurrentTerm != workerTerm {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader {
			rf.mu.Unlock()
			workerCancel()
			// DPrintf("[sendAE] Server %d to %d demoted", rf.me, server)
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
			// DPrintf("[sendAE] Server %d to %d waiting\n", rf.me, server)
			select {
			case <-rf.notify[server]:
			case <-workerCtx.Done():
				return
			case <-rf.killCh:
				return
			}
		} else {
			// 将要重试，清空notify channel
			select {
			case <-rf.notify[server]:
			default:
			}
		}
		// DPrintf("[sendAE] Server %d to %d\n", rf.me, server)
		select {
		case <-workerCtx.Done():
			return
		default:
		}
		// DPrintf("[sendAE] Server %d to %d, getting lock\n", rf.me, server)
		rf.mu.Lock()
		// 检查term是否发生变化
		if rf.CurrentTerm != workerTerm {
			rf.mu.Unlock()
			return
		}
		if rf.nextIndex[server] > rf.Loglen() {
			// illegal state, reset nextIndex
			rf.nextIndex[server] = rf.Loglen()
		}
		switch {
		case rf.nextIndex[server] <= rf.SnapshotIndex:
			term := rf.CurrentTerm
			snapshot := rf.SnapshotByte
			lastIncludedIndex := rf.SnapshotIndex
			lastIncludedTerm := rf.SnapshotTerm
			args := InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Data:              snapshot,
			}
			rf.mu.Unlock()
			DPrintf("[sendIS] Server %d to %d, lastone: %d, %d\n", rf.me, server, lastIncludedIndex, lastIncludedTerm)
			type isResult struct {
				ok    bool
				reply InstallSnapshotReply
			}
			ch := make(chan isResult, 1)
			go func() {
				reply := InstallSnapshotReply{}
				ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
				ch <- isResult{ok, reply}
			}()
			var ok bool
			var reply InstallSnapshotReply
			select {
			case res := <-ch:
				ok, reply = res.ok, res.reply
			case <-time.After(100 * time.Millisecond):
				ok = false
			case <-workerCtx.Done():
				DPrintf("[sAE] server %d to %d shutdown", rf.me, server)
				return
			case <-rf.killCh:
				DPrintf("[sAE] server %d to %d shutdown", rf.me, server)
				return
			}
			if ok {
				serverTerm := reply.Term
				if serverTerm > term {
					rf.mu.Lock()
					if rf.CurrentTerm == term && rf.state == Leader {
						rf.CurrentTerm = serverTerm
						rf.VotedFor = -1
						rf.state = Follower
						rf.persist()
						workerCancel()
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					workerCancel()
					return
				}
				if rf.CurrentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[server] = rf.SnapshotIndex + 1
				rf.matchIndex[server] = max(rf.matchIndex[server], rf.SnapshotIndex)
				rf.retry[server] = false
				nextIdx := rf.nextIndex[server]
				matchIdx := rf.matchIndex[server]
				rf.mu.Unlock()
				DPrintf("[sendIS] Server %d to %d done, lastone: %d, %d,done\n", rf.me, server, lastIncludedIndex, lastIncludedTerm)
				DPrintf("[sendIS] %d: set nextIndex[%d] = %d, matchIndex[%d] = %d\n", rf.me, server, nextIdx, server, matchIdx)
				select {
				case rf.notifyApply <- struct{}{}:
				default:
				}
				if rf.retry[server] {
					// 重置计时器
					select {
					case rf.tickers[server].rst <- struct{}{}:
					default:
					}
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
				if rf.CurrentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
			break
		default:
			nextIdx := rf.idxInLog(rf.nextIndex[server])
			prevIdx := rf.idxInLog(rf.nextIndex[server] - 1)
			logs := make([]logEntry, len(rf.Log[nextIdx:]))
			copy(logs, rf.Log[nextIdx:])
			prevLogIndex := rf.nextIndex[server] - 1
			if prevLogIndex < 0 {
				prevLogIndex = 0
			}
			prevLogTerm := rf.Log[prevIdx].Term
			LeaderCommit := rf.commitIndex
			term := rf.CurrentTerm
			state := rf.state
			rf.mu.Unlock()
			if len(logs) > 0 {
				DPrintf("[sendAE] Server %d to %d: %v, %d\n", rf.me, server, logs[len(logs)-1], prevLogIndex)
			}
			if state != Leader {
				workerCancel()
				// DPrintf("[sendAE] Server %d to %d demoted", rf.me, server)
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
				DPrintf("[sAE] server %d to %d shutdown", rf.me, server)
				return
			case <-rf.killCh:
				DPrintf("[sAE] server %d to %d shutdown", rf.me, server)
				return
			}
			if ok {
				// DPrintf("[AE] Server %d to %d: success=%v, term=%d\n", rf.me, server, reply.Success, reply.Term)
				success := reply.Success
				serverTerm := reply.Term
				switch success {
				case true:
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						workerCancel()
						// DPrintf("[sendAE] Server %d to %d demoted", rf.me, server)
						return
					}
					if rf.CurrentTerm != term {
						rf.mu.Unlock()
						return
					}
					rf.nextIndex[server] = max(rf.nextIndex[server], prevLogIndex+len(logs)+1)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					rf.retry[server] = false
					rf.mu.Unlock()
					select {
					case rf.notifyApply <- struct{}{}:
					default:
					}
				case false:
					if serverTerm > term {
						rf.mu.Lock()
						if rf.CurrentTerm == term && rf.state == Leader {
							rf.CurrentTerm = serverTerm
							rf.VotedFor = -1
							rf.state = Follower
							rf.persist()
							workerCancel()
							rf.mu.Unlock()
							// DPrintf("[demote] Server %d: term %d\n", rf.me, serverTerm)
							return
						}
						// 超期worker，直接返回
						rf.mu.Unlock()
						DPrintf("[sendAE] [demote] Server %d: term %d\n", rf.me, serverTerm)
						return
					}
					rf.mu.Lock()
					rf.retry[server] = true
					if reply.XTerm == 0 {
						rf.nextIndex[server] = max(reply.XLength, rf.SnapshotIndex)
					} else {
						lastIndex := rf.findLastIndex(reply.XTerm)
						switch {
						case lastIndex == -1:
							rf.nextIndex[server] = rf.SnapshotIndex

						case rf.Log[lastIndex].Term == reply.XTerm:
							rf.nextIndex[server] = lastIndex + 1

						default:
							rf.nextIndex[server] = max(reply.XIndex, rf.SnapshotIndex)
						}
					}
					nextIdx := rf.nextIndex[server]
					rf.mu.Unlock()
					if nextIdx <= rf.SnapshotIndex {
						DPrintf("[sendAE] Server %d to %d: small next, try send snapshot", rf.me, server)
					}
				}
			} else {
				//retry
				rf.mu.Lock()
				rf.retry[server] = true
				if rf.state != Leader {
					workerCancel()
					rf.mu.Unlock()
					// DPrintf("[sendAE] Server %d to %d demoted", rf.me, server)
					return
				}
				if rf.CurrentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
			// DPrintf("[sendAE] seveer %d to %d done", rf.me, server)
			break
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
	select {
	case rf.notifyApply <- struct{}{}:
	default:
	}
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
	index = rf.Loglen()
	term = rf.CurrentTerm
	rf.Log = append(rf.Log, logEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	rf.mu.Unlock()
	DPrintf("[Start] Server %d: %v, %d", rf.me, command, index)
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
	rf.mu.Lock()
	DPrintf("[Kill]Server %d\n", rf.me)
	rf.mu.Unlock()
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
	var args RequestVoteArgs
	args = RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.Loglen() - 1,
		LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
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

func (rf *Raft) ElectionWorker(abortElectionChan chan struct{}, start chan struct{}) {
	for rf.killed() == false {
	WORK:
		select {
		case <-abortElectionChan:
			return
		case <-rf.killCh:
			return
		case <-start:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock()
				break WORK
			}
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.state = Candidate
			ElectionTerm := rf.CurrentTerm
			rf.persist()
			me, term := rf.me, rf.CurrentTerm
			rf.mu.Unlock()
			DPrintf("[StartElection]Server %d term %d\n", me, term)
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
				if ElectionTerm != rf.CurrentTerm {
					rf.mu.Unlock()
					break WORK
				}
				rf.mu.Unlock()
				select {
				case reply := <-replyChan:
					rf.mu.Lock()
					if rf.CurrentTerm != ElectionTerm {
						rf.mu.Unlock()
						close(abortChan)
						break WORK
					}
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.state = Follower
						rf.persist()
						rf.mu.Unlock()
						// DPrintf("[demote] Server %d: term %d\n", rf.me, reply.Term)
						close(abortChan)
						break WORK
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
							// 提交no op log entry
							// 仅在有需要间接提交的情况下提交no op log entry
							// noop := false
							// if rf.commitIndex < len(rf.Log)-1 {
							// 	rf.commitIndex = len(rf.Log) - 1
							// 	rf.noopIndex = len(rf.Log) - 1
							// 	noop = true
							// }
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								select {
								case <-rf.ctx.Done():
									return
								default:
								}
								rf.nextIndex[i] = rf.Loglen()
								rf.matchIndex[i] = 0
								rf.retry[i] = false
								go rf.sendAppendEntries(i)
								select {
								case <-rf.notify[i]:
								default:
								}
							}
							Term := rf.CurrentTerm
							rf.mu.Unlock()
							DPrintf("[ElectionWon] Server %d: term %d\n", rf.me, Term)
							// if noop {
							// 	DPrintf("[NOOP] Server %d: term %d\n", rf.me, Term)
							// }
							break WORK
						}
					}
					rf.mu.Unlock()
				case <-abortElectionChan:
					rf.mu.Lock()
					rf.state = Follower
					rf.mu.Unlock()
					close(abortChan)
					break WORK
				case <-rf.killCh:
					return
				}
			}
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
			close(abortChan)

		}
	}
}

func (rf *Raft) ticker() {
	ticker := rf.tickers[rf.me]
	defer ticker.timer.Stop()
	start := make(chan struct{}, 1)
	go rf.ElectionWorker(rf.ElectionAbort, start)
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
				// start a new election
				select {
				case start <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (rf *Raft) HeartbeatMaintain(sever int) {
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
				select {
				case rf.notifyApply <- struct{}{}:
				default:
				}
				// DPrintf("sending hb to %d", sever)
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]logEntry, 1) // log[0] is a dummy entry

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.SnapshotIndex = 0
	rf.SnapshotTerm = 0
	rf.SnapshotByte = nil
	rf.applyCh = applyCh
	// print nv states
	// DPrintf("[Make] Server %d: term=%d, votedFor=%d, loglen=%d, lastLogTerm=%d\n", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log)-1, rf.Log[len(rf.Log)-1].Term)
	rf.ElectionAbort = make(chan struct{}, 1)
	rf.notifyApply = make(chan struct{}, 1)
	rf.tickers = make([]*Ticker, len(peers))
	rf.retry = make([]bool, len(peers))
	rf.killCh = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(peers); i++ {
		rf.notify = append(rf.notify, make(chan struct{}, 1))
		rf.tickers[i] = &Ticker{
			rst:   make(chan struct{}, 1),
			timer: time.NewTimer(time.Duration(300+rand.Int63()%200) * time.Millisecond),
		}
	}
	// start applyWorker
	go rf.applyLogEntries()
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
