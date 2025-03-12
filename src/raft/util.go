package raft

import (
	"log"
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

type LogEntry struct { //日志结构体
	Command interface{}
	Term    int
	Index   int
}

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
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Microsecond //1000-1999之间
}

// StableHeartbeatTimeout 设置心跳超时时间
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimer) * time.Millisecond //0-124
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		SugarLogger.Infof(format, a...)
		log.Printf(format, a...)
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
