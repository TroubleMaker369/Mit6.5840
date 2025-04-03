package raft

// ///////////////////////////////////////////请求投票RPC处理/////////////////////////////////////////////
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

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastlog().Index,
		LastLogTerm:  rf.getLastlog().Term,
	}
	return args
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//根据 Raft 协议，服务器在投票时会依据以下规则判断是否可以投票给某个候选人

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	//如果候选人请求的任期比当前服务器的任期小，拒绝投票。
	//如果请求的任期和当前服务器的任期相同，但当前服务器已经投票给了其他候选人，则拒绝投票。
	// 如果rf.votedFor!=-1是不是每次变成follow都要给他的投票设置为-1

	//1.任期比较：如果候选人的任期 (args.Term) 小于当前服务器的任期 (rf.currentTerm)，则拒绝投票。
	//2.已经投过票的候选人：如果当前服务器已经在同一任期内投过票，且投给了另一个候选人，那么拒绝投票给当前请求的候选人。
	//3.投票限制：每个服务器在同一任期内只能投一次票。如果在当前任期内已经投票，不能再投票给其他候选人，除非当前的任期被更新（当选举成功或者新任期开始时）
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//日志更新的判断标准是：候选人的日志的任期大于当前服务器的日志，或者在同一任期下，候选人的日志索引更大或相等。
	// 如果当前周期小于候选人周期，则说明当前周期较旧，需要转为Follow状态，是否投票还是要进一步看日志的新旧
	if rf.currentTerm < args.Term {
		rf.ChangeState(Follower)
		//如果候选人的任期大于当前服务器的任期，服务器会更新自己的任期并投票给该候选人
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	//4.日志一致性:如果候选人的日志比当前服务器的日志更新（通过 isLogUpToDate 函数判断），则投票给该候选人。
	// 如果候选人的log不是最新的，拒绝投票
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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

/////////////////////////////////////////////请求投票RPC处理结束/////////////////////////////////////////////

/////////////////////////////////////////////心跳日志RPC处理/////////////////////////////////////////////

// AppendEntriesArgs 发送心跳参数
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 最后一条日志的索引
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

// AppendEntriesReply 心跳消息结构
type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConfictIndex int
	ConfictTerm  int
}

// genAppendEntriesArgs  构造请求投票参数
func (rf *Raft) genAppendEntriesArgs(PrevLogIndex int) *AppendEntriesArgs {
	// Leader 认为的 Follower 最后一条日志的绝对索引
	firstLogIndex := rf.getFirstlog().Index
	entries := make([]LogEntry, len(rf.logs[PrevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[PrevLogIndex-firstLogIndex+1:])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,                           // Leader 当前任期
		LeaderId:     rf.me,                                    // Leader ID
		PrevLogIndex: PrevLogIndex,                             // 期望的 Follower 最后日志索引
		PrevLogTerm:  rf.logs[PrevLogIndex-firstLogIndex].Term, // 对应日志的任期
		LeaderCommit: rf.commitIndex,                           // Leader 的提交索引
		Entries:      entries,                                  // 需要复制的日志条目
	}

	return args
}

// sendAppendEntries 发送心跳RPC
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries 处理心跳日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	//如果当前周期大于 发送周期，说明发送周期已经落后。设置返回消息，立刻返回
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	//如果当前周期落后发送周期，说明当前Server未更新leader,首先设置该Raft的周期
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	// 改为Follow
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	//如果在当前peer的 prevLogIndex中没有一个词条匹配prevLogTerm的条目，则返回false
	//args.PrevLogIndex "我认为你的日志在索引 PrevLogIndex 处的条目应该和我的一致"
	if args.PrevLogIndex < rf.getFirstlog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	//检查日志是否匹配，如果不匹配，返回冲突索引和词条
	//如果现有项与新项冲突(相同的索引，但不同周期）删除现有条目及其后面的所有条目
	if !rf.IsLogMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastlog().Index
		//找出冲突项的第一个索引
		if lastLogIndex < args.PrevLogIndex { //如果follower的最后一个索引比leader期望的索引小，则最后一个的前一个位置为矛盾位置
			reply.ConfictIndex, reply.ConfictTerm = lastLogIndex+1, -1
		} else { //如果follower的最后一个索引比leader期望的索引大，且第一个索引比leader期望的索引小，则说明能follow存在该索引，是该索引的周期不匹配
			firtstLogIndex := rf.getFirstlog().Index
			//找出冲突项的第一个索引
			index := args.PrevLogIndex
			//这里index需不需要减1
			for index >= firtstLogIndex && rf.logs[index-firtstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			//告诉 Leader："我有一段连续的、任期为 args.PrevLogTerm 的日志，从索引 ConflictIndex 开始"
			// reply.ConfictIndex, reply.ConfictTerm = index+1, args.PrevLogTerm

			//更标准的 Raft 优化实现有时会返回 Follower 在 args.PrevLogIndex 处实际的任期 rf.logs[args.PrevLogIndex - firtstLogIndex].Term 作为 ConfictTerm，
			//让 Leader 可以更快地在其自身日志中查找匹配的任期。
			reply.ConfictIndex, reply.ConfictTerm = index+1, rf.logs[args.PrevLogIndex-firtstLogIndex].Term
		}
		return
	}
	//追加日志中没有的任何新条目
	firstLogIndex := rf.getFirstlog().Index
	for index, entry := range args.Entries {
		// 找到现有日志和附加日志的连接点

		// 对每个条目检查两种情况：
		// 超出范围：entry.Index-firstLogIndex >= len(rf.logs)
		// 任期冲突：rf.logs[entry.Index-firstLogIndex].Term != entry.Term
		// 当发现首个不匹配点，执行日志截断和追加，然后退出循环
		if entry.Index-firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstLogIndex], args.Entries[index:]...)
			break
		}
	}

	//如果leaderCommit > commitIndex，设置commitIndex = min（leaderCommit，最后一个新条目的索引）（论文）
	newCommitIndex := Min(args.LeaderCommit, rf.getLastlog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %v} advances commitIndex from %v to %v with leaderCommit %v in term %v", rf.me, rf.commitIndex, newCommitIndex, args.LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		//通知有新的已提交日志需要应用到状态机
		rf.applyCond.Signal()
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

/////////////////////////////////////////////心跳日志心跳日志RPC处理结束/////////////////////////////////////////////
