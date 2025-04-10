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

	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	CommandTerm  int

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
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// persist 持久化
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	DPrintf("{Node %v}begin decode persisted state", rf.me)
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		DPrintf("{Node %v} fails to decode persisted state", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, voteFor, logs
	rf.lastApplied, rf.commitIndex = rf.getFirstlog().Index, rf.getFirstlog().Index
	DPrintf("{Node %v} decode persisted success", rf.me)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstlog().Index
	if index <= snapshotIndex || index > rf.getLastlog().Index {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}

	rf.logs = shrinkEntries(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)

	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot with index %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstlog(), rf.getLastlog(), index)

}

// 将config.go  中err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)  修改为:
//
//	if rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
//					err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
//				}
//
// // CondInstallSnapshot 用来peer判断leader发过来的快照是否满足条件，如果满足，则安装快照
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//过期快照
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects outdated snapshot with lastIncludeIndex %v as current commitIndex %v is larger in term %v", rf.me, lastIncludedIndex, rf.commitIndex, rf.currentTerm)
		return false
	}

	if lastIncludedIndex > rf.getLastlog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = shrinkEntries(rf.logs[lastIncludedIndex-rf.getFirstlog().Index:])
	}

	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex

	rf.persister.Save(rf.encodeState(), snapshot)

	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstlog(), rf.getLastlog(), lastIncludedTerm, lastIncludedIndex)
	return true
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
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// IsLogMatch 用来检查 Leader 的 PrevLogIndex 和 PrevLogTerm 是否与 Follower 的日志匹配
func (rf *Raft) IsLogMatch(index int, term int) bool {
	return index <= rf.getLastlog().Index && term == rf.logs[index-rf.getFirstlog().Index].Term
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
	if rf.state != Leader {
		return -1, -1, false
	}
	//每个raft服务器的第一条日志是对于他自己的
	newLogIndex := rf.getLastlog().Index + 1
	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Index:   newLogIndex,
		Command: command,
	})
	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLogIndex, newLogIndex+1
	DPrintf("{Node %v} starts agreement on a new log entry with command %v in term %v", rf.me, command, rf.currentTerm)

	// 为所有服务器广播日志添加条目
	rf.BroadcastHeartbeat(false)

	// Your code here (3B).

	return newLogIndex, rf.currentTerm, true
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

// GetId 用于lab4获取id
func (rf *Raft) GetId() int {
	return rf.me
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
	rf.persist()
	args := rf.genRequestVoteArgs()
	grantedVotes := 1
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)
	for peer := range rf.peers {
		// fmt.Println(peer)
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply) // 返回一个指向类型 T 的指针
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteReply %v from {Node %v} after sending RequestVoteArgs %v", rf.me, reply, peer, args)
				if args.Term == rf.currentTerm && rf.state == Candidate { //这里再次检查是避免rpc在调用过程中超时导致，当前Term和state发生了变化
					if reply.VoteGranted { //该peer 投票给了这个候选人
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives over half of the votes", rf.me)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm { //没有投票给候选人，说明选人要么日志落后 要么周期落后
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

// BroadcastHeartbeat Leader发送心跳  新的心跳机制，负责日志复制，和日志回退重发
func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			//应该立即将心跳发送给所有的对等体
			go rf.replicateOnceRound(peer)
		} else {
			//只需要向复制器发送信号，将日志条目发送给对等体
			rf.replicatorCond[peer].Signal()
		}
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
			rf.persist()
			// start election
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout()) //在分裂投票的情况下，重置选举计时器
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			//心跳超时逻辑
			rf.mu.Lock()
			if rf.state == Leader {
				//重新发送心跳
				DPrintf("{Node %v} BroadcastHeartbeat", rf.me)
				rf.BroadcastHeartbeat(true)

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

// advanceCommitIndexForLeader 通过检查大多数节点的日志复制状态，安全地推进 Leader 的提交索引
func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	//获取已知在大多数服务器上复制的索引最高的日志条目的索引
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.IsLogMatch(newCommitIndex, rf.currentTerm) { //只能提交本周期内的日志
			DPrintf("{Node %v} advances commitIndex from %v to %v in term %v", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

// applier 应用日志条目
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// 检查commitIndex是否可用
		if rf.commitIndex <= rf.lastApplied {
			//需要等待commitIndex被推进
			rf.applyCond.Wait()
		}
		// 应用日志条目到状态机
		firstLogIndex, commitIndex, lastApplied := rf.getFirstlog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()

		//将申请消息发送到applyCh以获取服务/状态机副本
		for _, entry := range entries {
			rf.applych <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies log entries from index %v to %v in term %v", rf.me, lastApplied+1, commitIndex, rf.currentTerm)
		// 使用commitIndex与rf.commitIndex最大值而不是commitIndex，因为rf.commitIndex可能在Unlock（）和Lock（）期间发生变化。
		//	如果直接赋值 rf.lastApplied = commitIndex，
		// 而 rf.commitIndex 在解锁期间减小（例如由于某种异常或竞争条件）
		// f.lastApplied 可能会被设置为一个较小的值，导致状态回退。
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// needReplicating 判断peer是否需要日志条目复制
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastlog().Index
}

// replicateOnceRound
func (rf *Raft) replicateOnceRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstlog().Index { //如果Follow落后太多直接发送快照给他
		//only send InstallSnapshot RPC
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.ChangeState(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
					// rf.electionTimer.Reset(RandomElectionTimeout())
				} else {
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
					rf.matchIndex[peer] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends InstallSnapshotArgs %v to {Node %v} and receives InstallSnapshotReply %v", rf.me, args, peer, reply)
		}
	} else {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			//如果rpc 后 还是leader 且周期没变化
			if args.Term == rf.currentTerm && rf.state == Leader {
				if !reply.Success { //日志一致性检查失败
					if reply.Term > rf.currentTerm { //脑裂，或者已经宕机的leader突然又活过来  可能发生了网络分区，或者这个 Leader 是一个刚刚恢复但任期落后的旧 Leader
						//标签当前服务器 已经过时了，重新变成follower
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					} else if reply.Term == rf.currentTerm { //说明follow的周期和leader周期一致，说明是条目出了问题
						// //减少nextIndex并重试
						// rf.nextIndex[peer] = reply.ConfictIndex
						// // TODO: optimize the nextIndex finding, maybe use binary search
						// if reply.ConfictTerm != -1 {
						// 	firstLogIndex := rf.getFirstlog().Index
						// 	for index := args.PrevLogIndex; index >= firstLogIndex; index-- {
						// 		if rf.logs[index-firstLogIndex].Term == reply.ConfictTerm {
						// 			rf.nextIndex[peer] = index
						// 			break
						// 		}
						// 	}
						// }
						firstLogIndex := rf.getFirstlog().Index
						if reply.ConfictTerm != -1 {
							lastIndex := -1
							for index := args.PrevLogIndex; index >= firstLogIndex; index-- {
								if rf.logs[index-firstLogIndex].Term == reply.ConfictTerm {
									lastIndex = index
									break
								}
							}
							if lastIndex != -1 {
								rf.nextIndex[peer] = lastIndex + 1 // Case 2  Leader 有 XTerm，nextIndex = XTerm 最后一个条目 + 1。
							} else {
								rf.nextIndex[peer] = reply.ConfictIndex // Case 1 Leader 无 XTerm，nextIndex = ConfictIndex。
							}
						} else {
							rf.nextIndex[peer] = reply.ConfictIndex // Case 3 Follower 日志太短，nextIndex = XLen。
						}
					}
				} else { //日志匹配成功 提交
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					//advance commitIndex if possible
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends AppendEntriesArgs %v to {Node %v} and receives AppendEntriesReply %v", rf.me, args, peer, reply)
		}
	}
}

// replicator 日志复制
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			// send log entries to peer
			SugarLogger.Infof("%v raft wait ", peer)
			rf.replicatorCond[peer].Wait()
		}
		// send log entries to peer
		SugarLogger.Infof("%v raft send Log entries ", rf.me)
		rf.replicateOnceRound(peer)
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

// time go test --run 3B
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

		applych:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// SugarLogger.Infof("%v raft Server Initialization", me)
	// Your initialization code here (3A, 3B, 3C).
	InitLogger()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// should use mu to protect applyCond, avoid other goroutine to change the critical section
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize nextIndex and matchIndex, and start replicator goroutine
	// 应该使用mu来保护应用程序，避免其他例程更改临界区
	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastlog().Index+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			// 启动复制程序例程以向对等节点发送日志项
			go rf.replicator(peer)
		}
	}

	SugarLogger.Infof("%v raft Server Initialization replicatorCond Finish", me)
	// start ticker goroutine to start elections
	go rf.ticker()

	SugarLogger.Infof("%v raft Server Initialization select and heartbeat Finish ", me)
	// start apply goroutine to apply log entries to state machine
	go rf.applier()
	SugarLogger.Infof("%v raft Server Initialization Log Applier Finish ", me)

	return rf
}
