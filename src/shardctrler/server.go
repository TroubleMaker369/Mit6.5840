package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex       //确保并发安全，保护共享状态。
	me      int                //标识服务器，辅助 Raft 或调试
	rf      *raft.Raft         //  Raft 实例，管理一致性协议
	applyCh chan raft.ApplyMsg //Raft 日志提交通道，连接状态机

	// Your data here.
	dead           int32                      //控制服务器终止，优雅退出
	stateMachine   ConfigStateMachine         //核心状态机，管理配置历史和操作
	LastOperations map[int64]OperationContext //去重客户端请求，防止重复执行。
	notifyChans    map[int]chan *CommandReply //异步通知 Raft 提交结果，提升并发性
}

// Command处理传入的命令
//如果该命令不是查询，而是重复请求，
//它返回之前的回复
//否则，它将尝试在raft层启动命令。
//如果当前服务器不是leader，它会返回一个错误
//如果命令启动成功，它将等待结果
//或者在一定时间后超时。

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.RLock()
	//检查命令是否为重复请求（仅适用于非查询命令）
	if args.Op != Query && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := sc.LastOperations[args.ClientId].LastReply
		reply.Config, reply.Err = lastReply.Config, lastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	//命令需要执行
	//尝试在raft层启动命令
	index, _, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	//获取通知通道等待结果
	notifyChan := sc.getNotifyChan(index)
	sc.mu.Unlock()

	DPrintf("执行%v", args.Op)
	select {
	case result := <-notifyChan:
		DPrintf("客户端收到编号%v答复", result.Config.Num)
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		//删除过时的通知通道以减少内存占用
		sc.removeOutdateNotifyChan(index)
		sc.mu.Unlock()
	}()
}

// getNotifyChan获取给定索引的通知通道,如果通道不存在，则创建一个
func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	notifyChans, ok := sc.notifyChans[index]
	if !ok {
		notifyChans = make(chan *CommandReply, 1)
		sc.notifyChans[index] = notifyChans
	}
	return notifyChans
}

// removeOutdateNotifyChan 移除通道
func (sc *ShardCtrler) removeOutdateNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

// isDuplicateRequest 检查命令是否是重复命令
func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	OperationContext, ok := sc.LastOperations[clientId]
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1

}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// applier 应用程序是将日志应用到状态机的程序
func (sc *ShardCtrler) applier() {
	for !sc.Killed() {
		select {
		case message := <-sc.applyCh:
			// 处理 Raft 的日志条目
			if message.CommandValid {
				reply := new(CommandReply)
				command := message.Command.(Command)
				sc.mu.Lock()

				if command.Op != Query && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = sc.LastOperations[command.ClientId].LastReply
				} else {
					reply = sc.applyLogToStateMachine(command)
					if command.Op != Query {
						sc.LastOperations[command.ClientId] = OperationContext{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}
				//当节点为leader时，通知相关通道currentTerm的日志
				//通知 Leader 的客户端：确保客户端请求（如 Join、Leave）在日志提交并应用后收到响应
				DPrintf("执行%s成功", command.Op)
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					notifyChan := sc.getNotifyChan(message.CommandIndex)
					notifyChan <- reply
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case Join:
		reply.Err = sc.stateMachine.Join(command.Servers)
	case Leave:
		reply.Err = sc.stateMachine.Leave(command.GIDs)
	case Move:
		reply.Err = sc.stateMachine.Move(command.Shard, command.GID)
	case Query:
		reply.Config, reply.Err = sc.stateMachine.Query(command.Num)
	}
	DPrintf("应用操作%s到状态机", command.Op)
	return reply
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {

	// Your code here.
	InitLogger()
	labgob.Register(Command{})
	apply := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		rf:             raft.Make(servers, me, persister, apply),
		applyCh:        apply,
		stateMachine:   NewMemoryConfigStateMachine(),
		LastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
		dead:           0,
	}
	go sc.applier()
	return sc
}
