package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type NodeState uint8

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// shrinkEntriesArray 会丢弃 entries 切片底层使用的数组，
// 如果这个数组的大部分空间没有被使用的话。这样可以避免保留对一大堆可能很大的、不再需要的条目的引用。
// 简单地清空 entries 是不安全的，因为客户端可能仍在使用这些条目。
func shrinkEntries(entries []LogEntry) []LogEntry {
	const lenMultiple = 2
	if cap(entries) > len(entries)*lenMultiple {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

func (state NodeState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	panic("unexpected NodeState")
}

type LogEntry struct { //日志结构体
	Command interface{}
	Term    int
	Index   int
}

// ===========================================定时器设置
// 超时设置
const ElectionTimeout = 1000
const HeartbeatTimer = 125

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

// 初始化全局随机器
var GlobalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())), // 通过当前的 Unix 时间戳（单位是纳秒）作为种子来初始化
}

// RandomElectionTimeout 设置随机选举超时时间
func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond //1000-1999之间
}

// StableHeartbeatTimeout 设置心跳超时时间
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimer) * time.Millisecond //0-124
}

// 												定时器设置===========================================

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		SugarLogger.Infof(format, a...)
		// log.Printf(format, a...)
	}
}

// getLastlog 获取最后一条log日志
func (rf *Raft) getLastlog() LogEntry {
	return rf.logs[len(rf.logs)-1]

}

// getLastlog 获取第一条log日志
func (rf *Raft) getFirstlog() LogEntry {
	return rf.logs[0]
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v, CandidateId:%v, LastLogIndex:%v, LastLogTerm:%v}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v, VoteGranted:%v}", reply.Term, reply.VoteGranted)
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v, LeaderId:%v, PrevLogIndex:%v, PrevLogTerm:%v, LeaderCommit:%v, Entries:%v}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v, Success:%v, ConflictIndex:%v, ConflictTerm:%v}", reply.Term, reply.Success, reply.ConfictIndex, reply.ConfictTerm)
}
