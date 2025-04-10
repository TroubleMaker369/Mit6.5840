package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// KVServer 负责协调 Raft 协议和上层键值存储逻辑，确保线性一致性
type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int //避免重复应用，记录最后的应用索引

	// Your definitions here.
	stateMachine   KVStateMachine             //表示键值存储的状态机
	lastOperations map[int64]OperationContext //记录每个客户端的最新操作上下文，用于去重和结果缓存
	notifyChs      map[int]chan *CommandReply //用于通知等待命令执行结果的客户端
}

// isDuplicatedCommand 检查是否是重复命令
func (kv *KVServer) isDuplicatedCommand(clientId, commandId int64) bool {
	OperationContext, ok := kv.lastOperations[clientId]
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

// ExecuteCommand 处理客户端发送的命令请求（例如 Get、Put 或 Append 操作），并确保命令在分布式系统中正确执行。
func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	//如果不是Get命令，且是重复命令， 则直接返回旧值
	if args.Op != OpGet && kv.isDuplicatedCommand(args.ClientId, args.CommandId) {
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	//是Get命令，不确定是不是以及执行过的

	// 如果是Get命令，重复执行也没事
	// 如果不是Get命令，则说明该命令不是重复命令需要执行
	//提交命令到 Raft
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//创建通知通道并等待结果
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	// 使用 select 等待：
	// 如果通道 ch 返回结果（result），说明命令已成功应用，填充 reply。
	// 如果超时（ExecuteTimeout），返回 ErrTimeout。
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	//清理通知通道
	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()
}

// getNotifyCh 检查 notifyChs 中是否已有 index 对应的通道。
func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

// deleteNotifyCh 从 notifyChs 中删除指定索引的通道
func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

// applyLogToStateMachine 将内容应用到状态机
func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine.Get(command.Key)
	case OpPut:
		reply.Err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		reply.Err = kv.stateMachine.Append(command.Key, command.Value)
	}
	return reply
}

// needSnapshot 检查是否需要触发KV快照
func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

// takeSnapshot 生成kv快照
func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// restoreStateFromSnapshot 从快照中恢复kvserver状态
func (kv *KVServer) restoreStateFromSnapshot(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var statMachine MemoryKV
	var lastOperations map[int64]OperationContext
	if d.Decode(&statMachine) != nil || d.Decode(&lastOperations) != nil {
		panic("Failed to restore state from snapshot")
	}
	kv.stateMachine = &statMachine
	kv.lastOperations = lastOperations

}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.rf.GetId(), message)
			// 检查 message.CommandValid 是否为 true，表示这是一个命令消息（而不是快照消息）
			if message.CommandValid {
				kv.mu.Lock()
				//当前条目，已经被raft应用到集群  （可能是快照恢复导致的重复消息）
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.GetId(), message, kv.lastApplied)
					kv.mu.Unlock()
					continue //为什么continue？
				}
				kv.lastApplied = message.CommandIndex

				reply := new(CommandReply)
				command := message.Command.(Command) // type assertion
				//如果不是Get命令，且是重复命令， 则直接返回旧值
				if command.Op != OpGet && kv.isDuplicatedCommand(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.GetId(), message, kv.lastOperations[command.ClientId], command.ClientId)
					reply = kv.lastOperations[command.ClientId].LastReply
				} else {
					//要么是get命令，要么是新命令; Get 是只读操作，无副作用，可重复执行，不需去重。
					//应用到状态机
					reply = kv.applyLogToStateMachine(command)
					if command.Op != OpGet { //该命令是put且为新命令
						//保存最后应用
						kv.lastOperations[command.ClientId] = OperationContext{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}

				//回应客户端
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <- reply
				}
				if kv.needSnapshot() {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid { //如果消息种携带快照
				kv.mu.Lock()
				//将快照应用到raft上，且从快照中恢复kvserver的状态
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreStateFromSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("Invalid ApplyMsg %v", message))
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		mu:             sync.RWMutex{},
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		dead:           0,
		maxraftstate:   maxraftstate,
		stateMachine:   &MemoryKV{KV: make(map[string]string)},
		lastOperations: make(map[int64]OperationContext),
		notifyChs:      make(map[int]chan *CommandReply),
	}
	// You may need initialization code here.
	kv.restoreStateFromSnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
