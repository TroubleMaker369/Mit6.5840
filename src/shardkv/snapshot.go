package shardkv

import (
	"bytes"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

// restoreSnapShot 从快照中恢复 shardkv状态
func (kv *ShardKV) restoreSnapShot(snapshot []byte) {
	// 从快照中存储状态机。如果快照为nil或空，则初始化状态机
	if len(snapshot) < 1 {
		kv.initStateMachines()
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine map[int]*Shard
	var lastOperations map[int64]OperationContext
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&lastConfig) != nil || d.Decode(&currentConfig) != nil || d.Decode(&stateMachine) != nil || d.Decode(&lastOperations) != nil {
		DPrintf("{Node %v}{Group %v} fails to restore state machine from snapshot", kv.rf.GetId(), kv.gid)
	}
	kv.lastConfig, kv.currentConfig, kv.lastOperations, kv.stateMachine = lastConfig, currentConfig, lastOperations, stateMachine
}

// needSnapShot 查看是否需要快照
func (kv *ShardKV) needSnapShot() bool {
	return kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState
}

// takeSnapshot 快照当前状态
func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}
