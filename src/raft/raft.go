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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        //当前任期号
	votedFor    int        //candidateId在当前任期内收到的选票（如果没有，则为null）
	logs        []LogEntry //日志条目;每个条目包含状态机的命令，以及leader接收条目的时间（第一个索引为1）。

	// Volatile state on all servers
	commitIndex int //已知提交的最高日志项的索引（初始化为0，单调递增）
	lastApplied int //应用于状态机的最高日志项的索引（初始化为0，单调递增）

	// Volatile state on leaders(选举后重新初始化)
	nextIndex  []int //对于每个服务器，发送到该服务器的下一个日志条目的索引（初始化为leader最后一个日志索引+ 1）
	matchIndex []int //对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为0，单调递增）

	//
	state          NodeState     // current state of the server
	electionTimer  *time.Timer   //选举超时计时器
	heartbeatTimer *time.Timer   //心跳超时计时器
	applych        chan ApplyMsg //向服务发送应用消息的通道
	applyCond      *sync.Cond    //应用程序的条件变量
	replicatorCond []*sync.Cond  //复制程序程序的条件变量
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == Leader
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// isLogUpToDate 判断传入的日志是否比当前的日志“新”
func (rf *Raft) isLogUpToDate(index int, term int) bool {
	//如果候选人请求的日志比当前日志“新”，且任期合理，服务器可以投票给该候选人。
	// 否则，服务器拒绝投票或继续等待。
	lastlog := rf.getLastlog()
	if term > lastlog.Term || (term == lastlog.Term && index >= lastlog.Index) {
		return true
	} else {
		return false
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

// ChangeState 更改状态并重置超时器
func (rf *Raft) ChangeState(NewState NodeState) {
	if rf.state == NewState {
		return
	}
	DPrintf("{Node %v} changes state from %v to %v", rf.me, rf.state, NewState)
	rf.state = NewState
	switch rf.state {
	case Follower:
		// Follower 节点定期检查是否需要发起选举，这个定时器用于触发选举逻辑
		rf.electionTimer.Reset(RandomElectionTimeout())
		//Follower 状态下，节点会被 Leader 定期发送心跳信号，不需要自己发送心跳
		rf.heartbeatTimer.Stop()
	case Candidate:
	case Leader:
		//节点会定期发送心跳信号给 Follower，以保持其领导地位
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
		//因为作为 Leader，节点无需再发起选举
		rf.electionTimer.Stop()
	}
}

// StartElection raft开始选举
func (rf *Raft) StartElection() {
	//这里应该不加锁
	rf.votedFor = rf.me //为自己投票
	args := rf.genRequestVoteArgs()
	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply) // 返回一个指向类型 T 的指针
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.currentTerm && rf.state == Candidate { //这里再次检查是避免rpc在调用过程中超时导致，当前Term和state发生了变化
					if reply.VoteGranted { //该peer 投票给了这个候选人
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat()
						}
					} else if reply.Term > rf.currentTerm { //没有投票给候选人，说明选人要么日志落后 要么周期落后
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
					}
				}
			}
		}(peer)
	}
}

// BroadcastHeartbeat Leader发送心跳
func (rf *Raft) BroadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		SugarLogger.Infof("%v Server 发送心跳", rf.me)
		go func(peer int) { //向每个领导者发送心跳，所以要开go协整
			//发送心跳
			rf.mu.RLock()
			if rf.state != Leader { //检查状态，因为可能在发送心跳中途已经有新的领导人出现了，自己的状态已经被修改了
				rf.mu.RUnlock()
				return
			}
			//
			args := rf.genAppendEntriesArgs()
			rf.mu.RUnlock()
			reply := new(AppendEntriesReply)
			if rf.sendAppendEntries(peer, args, reply) {
				rf.mu.Lock()
				if rf.currentTerm == args.Term && rf.state == Leader { //检查经过RPC后，当前leader的状态有没有改变
					if !reply.Success {
						//说明集群中可能有新的Leader出现了或者更新的Leader
						if reply.Term > rf.currentTerm {
							rf.ChangeState(Follower)
							rf.currentTerm, rf.votedFor = reply.Term, -1
						}
					}
				}
				rf.mu.Unlock()
			}
		}(peer)
	}
}

// ticker 选举超时与心跳超时逻辑
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			//选举超时逻辑
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			// rf.persist()
			// start election
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout()) //在分裂投票的情况下，重置选举计时器
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			//心跳超时逻辑
			rf.mu.Lock()
			if rf.state == Leader {
				//重新发送心跳
				rf.BroadcastHeartbeat()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf := &Raft{
		mu:        sync.RWMutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]LogEntry, 1), //索引0处的虚拟条目

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		state: Follower,
		//心跳超时时间<<小于选举超时(follower需要成为candidate所需要等待的时间)<<平均故障时间
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),

		applych: applyCh,
		// replicatorCond: make([]*sync.Cond, len(peers)),
	}

	SugarLogger.Infof("%v raft Server Initialization", me)
	// Your initialization code here (3A, 3B, 3C).
	// rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initialize nextIndex and matchIndex, and start replicator goroutine
	// for peer := range peers {
	// 	rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
	// 	if peer != rf.me {
	// 		rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})

	// 		go rf.replicator(peer)
	// 	}
	// }
	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply goroutine to apply log entries to state machine
	// go rf.applier()

	return rf
}
