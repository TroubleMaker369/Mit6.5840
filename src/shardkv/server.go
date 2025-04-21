package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type ShardKV struct {
	mu      sync.RWMutex       //互斥锁，用于保护 ShardKV 结构体的共享数据
	dead    int32              //指示节点是否被杀死
	rf      *raft.Raft         //指向 Raft 实例的指针
	applyCh chan raft.ApplyMsg //接收 Raft 提交的日志条目

	make_end func(string) *labrpc.ClientEnd //将服务器地址（string）转换为 RPC 客户端端点（*labrpc.ClientEnd）
	gid      int                            //副本组的唯一标识
	sc       *shardctrler.Clerk             //分片控制器的客户端，用于查询和更新分片配置

	maxRaftState int //Raft 日志的最大字节数，触发快照的阈值。
	lastApplied  int //最后应用的日志条目的索引，以防止statemmachine回滚
	// Your definitions here.

	lastConfig    shardctrler.Config // 上一次的分片配置（shardctrler.Config），由分片控制器提供。
	currentConfig shardctrler.Config //当前生效的分片配置

	stateMachine   map[int]*Shard             //键值存储的状态机，按分片（shard）组织，映射分片编号（int）到分片数据（*Shard）。
	lastOperations map[int64]OperationContext //记录每个客户端的最新操作，用于去重
	notifyChans    map[int]chan *CommandReply //映射 Raft 日志索引（int）到通知通道（*chan *CommandReply），用于异步通知客户端请求结果
}

// Command 处理来自客户端的RPC
func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	//如果命令是重复的，直接返回结果，没有raft层的参与
	if args.Op != Get && kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		kv.mu.RUnlock()
		return
	}
	//检查服务器是否可以提供请求的分片
	if !kv.canServe(key2shard(args.Key)) {
		DPrintf("服务器不能提供请求分片")
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	kv.mu.RUnlock()
	kv.Execute(NewOperationCommand(args), reply)
}

// Command handles client command requests.

// Execute 处理命令并通过reply参数返回结果
func (kv *ShardKV) Execute(command Command, reply *CommandReply) {
	// 不持有锁以提高吞吐量
	// 当KVServer持有锁以获取快照时，底层raft仍然可以提交raft日志
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("{Node %v}{Group %v} Execute Command %v with CommandReply %v", kv.rf.GetId(), kv.gid, command, reply)

	kv.mu.Lock()
	notifyChan := kv.getNotifyChan(index)
	kv.mu.Unlock()
	// 等待结果返回

	select {
	case result := <-notifyChan:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	//释放notifyChan以减少内存占用
	//为什么异步？为了提高吞吐量，这里不需要阻止客户机请求
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// 应用程序不断地将Raft日志中的命令应用到状态机。
func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		//在apply通道中等待新消息
		case message := <-kv.applyCh:
			DPrintf("{Node %v}{Group %v} tries to applfy message %v", kv.rf.GetId(), kv.gid, message)
			if message.CommandValid {
				kv.mu.Lock()
				//检查命令是否被应用
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v}{Group %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been applied", kv.rf.GetId(), kv.gid, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				//更新最新的日志提交索引
				kv.lastApplied = message.CommandIndex

				reply := new(CommandReply)
				//断言命令
				command := message.Command.(Command)
				switch command.CommandType {
				case Operation:
					//提取操作数据并将操作应用于状态机
					operation := command.Data.(CommandArgs)
					reply = kv.applyOperation(&operation)
				case Configuration:
					nextConfig := command.Data.(shardctrler.Config)
					reply = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationReply)
					reply = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					// time.Sleep(10*time.Millisecond)
					shardsInfo := command.Data.(ShardOperationArgs)
					reply = kv.applyDeleteShards(&shardsInfo)
				case EmptyShards:
					DPrintf("{Node %d}{Group %v} 应用空分片信息%v", kv.rf.GetId(), kv.gid, message)
					reply = kv.applyEmptyShards()
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					notifyChan := kv.getNotifyChan(message.CommandIndex)
					notifyChan <- reply
				}

				if kv.needSnapShot() {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				//从快照恢复状态机
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapShot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("{Node %v}{Group %v} invalid apply message %v", kv.rf.GetId(), kv.gid, message))
			}
		}
	}
}

// removeOutdatedNotifyChan 删除给定索引的通知通道。
func (kv *ShardKV) removeOutdatedNotifyChan(index int) {
	// delete(kv.notifyChans, index)
	if ch, ok := kv.notifyChans[index]; ok {
		close(ch) // ✅ 先关闭通道
		delete(kv.notifyChans, index)
	}
}

// getNotifyChan 返回给定索引的通知通道
func (kv *ShardKV) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[index]
}

// canServe 检查指定分片是否由当前副本组（GID）负责，并且分片状态是否允许服务（Serving 或 GCing）
func (kv *ShardKV) canServe(shardID int) bool {
	//GCing：分片已迁移到其他组但尚未清理，数据仍可读
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.stateMachine[shardID].Status == Serving || kv.stateMachine[shardID].Status == GCing)
}

// Command 处理来自客户端的RPC
func (kv *ShardKV) isDuplicateRequest(clientId int64, commandId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	return ok && commandId <= operationContext.MaxAppliedCommandId
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	DPrintf("{Node %v}{Group %v} is killed", kv.rf.GetId(), kv.gid)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// initStateMachines 用于初始化ShardKV状态机
func (kv *ShardKV) initStateMachines() {
	for shardID := 0; shardID < shardctrler.NShards; shardID++ {
		if _, ok := kv.stateMachine[shardID]; !ok {
			kv.stateMachine[shardID] = NewShard()
		}
	}
}

// Monitor 如果服务器是领导者，则监视特定的操作并在固定的时间间隔内重复执行该操作。
func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

// 返回指定状态的shard的GID到shard id的映射
func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shardID, shard := range kv.stateMachine {
		//过滤给定状态的分片
		if shard.Status == status {
			//找到负责该分片的最后一个gid，并从该gid中提取数据
			gid := kv.lastConfig.Shards[shardID]
			if gid != 0 {
				//按GID对分片id进行分组
				if _, ok := gid2shardIDs[gid]; !ok {
					gid2shardIDs[gid] = make([]int, 0)
				}
				gid2shardIDs[gid] = append(gid2shardIDs[gid], shardID)
			}
		}
	}
	return gid2shardIDs
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// InitLogger()
	labgob.Register(Command{})
	labgob.Register(CommandArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})

	//创建一个通道来接收Raft应用的消息
	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		make_end:       make_end,
		gid:            gid,
		sc:             shardctrler.MakeClerk(ctrlers),
		maxRaftState:   maxraftstate,
		lastApplied:    0,
		lastConfig:     shardctrler.DefaultConfig(),
		currentConfig:  shardctrler.DefaultConfig(),
		stateMachine:   make(map[int]*Shard),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
	}
	kv.restoreSnapShot(persister.ReadSnapshot())
	// Your initialization code here.
	go kv.applier()

	go kv.Monitor(kv.configurationAction, ConfigurationMonitorTimeout)
	go kv.Monitor(kv.migrationAction, MigrationMonitorTimeout)
	go kv.Monitor(kv.gcAction, GCMonitorTimeout)
	go kv.Monitor(kv.checkEntryInCurrentTermAction, EmptyEntryDetectorTimeout)

	DPrintf("{Node %v}{Group %v} started", kv.rf.GetId(), kv.gid)
	return kv
}
