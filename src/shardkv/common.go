package shardkv

import (
	"fmt"
	"time"

	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ExecuteTimeout              = 500 * time.Millisecond
	ConfigurationMonitorTimeout = 100 * time.Millisecond
	MigrationMonitorTimeout     = 50 * time.Millisecond
	GCMonitorTimeout            = 50 * time.Millisecond
	EmptyEntryDetectorTimeout   = 200 * time.Millisecond
)

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrOutDated
	ErrTimeout
	ErrNotReady
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrOutDated:
		return "ErrOutDated"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrNotReady:
		return "ErrNotReady"
	default:
		panic(fmt.Sprintf("Unknown error: %d", err))
	}
}

type CommandType uint8

const (
	Operation     CommandType = iota //处理客户端请求，修改 stateMachine 的键值数据
	Configuration                    //更新分片配置（currentConfig），触发分片迁移
	InsertShards                     //将新分片数据插入 stateMachine，对应 Pulling 状态
	DeleteShards                     //移除分片数据，完成迁移后的 GCing 清理
	EmptyShards                      //清空分片数据，处理特殊情况
)

func (commandType CommandType) String() string {
	switch commandType {
	case Operation:
		return "Operation"
	case Configuration:
		return "Configuration"
	case InsertShards:
		return "InsertShards"
	case DeleteShards:
		return "DeleteShards"
	case EmptyShards:
		return "EmptyShards"
	default:
		panic(fmt.Sprintf("Unknown CommandType: %d", commandType))
	}
}

type OperationType uint8

const (
	Get OperationType = iota
	Put
	Append
)

func (op OperationType) String() string {
	switch op {
	case Get:
		return "Get"
	case Put:
		return "Put"
	case Append:
		return "Append"
	default:
		panic(fmt.Sprintf("Unknown OperationType: %d", op))
	}
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        OperationType
	ClientId  int64
	CommandId int64
}

func (args *CommandArgs) String() string {
	return fmt.Sprintf("CommandArgs{Key: %s, Value: %s, Op: %s, ClientId: %d, CommandId: %d}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply *CommandReply) String() string {
	return fmt.Sprintf("CommandReply{Err: %s, Value: %s}", reply.Err, reply.Value)
}

// OperationContext 存储有关操作执行上下文的信息
type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

func (operationContext OperationContext) String() string {
	return fmt.Sprintf("OperationContext{MaxAppliedCommandId: %d, LastReply: %v}", operationContext.MaxAppliedCommandId, operationContext.LastReply)
}

// deepCopy
func (operationContext OperationContext) deepCopy() OperationContext {
	return OperationContext{
		MaxAppliedCommandId: operationContext.MaxAppliedCommandId,
		LastReply: &CommandReply{
			Err:   operationContext.LastReply.Err,
			Value: operationContext.LastReply.Value,
		},
	}
}

// ShardOperationArgs结构，用于与分片操作相关的参数
type ShardOperationArgs struct {
	ConfigNum int
	ShardIDs  []int
}

func (args *ShardOperationArgs) String() string {
	return fmt.Sprintf("ShardOperationArgs{ConfigNum: %d, ShardIDs: %v}", args.ConfigNum, args.ShardIDs)
}

// ShardOperationReply 用于来自分片操作的回复
type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]OperationContext
}

func (reply *ShardOperationReply) String() string {
	return fmt.Sprintf("ShardOperationReply{Err: %s, ConfigNum: %d, Shards: %v, LastOperations: %v}", reply.Err, reply.ConfigNum, reply.Shards, reply.LastOperations)
}

// 表示要执行的命令的命令结构
type Command struct {
	CommandType CommandType
	Data        interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("Command{commandType: %s, Data: %v}", command.CommandType, command.Data)
}

//在 ShardKV 系统中，Raft 日志包含了以下几种不同类型的操作：

// 客户端命令 (Command): 包含对键值存储的Put、Append、Get等操作，这些操作会通过 Raft 日志提交来保证多副本的一致性。
// 配置变更 (Config): 当从 shardctrler 获取到新的分片配置时，会通过 Raft 日志来记录配置的变化。所有副本组通过 Raft 日志共享同一配置，确保分片的一致分配。
// 分片操作 (ShardOperation): 当进行分片迁移时，涉及到的操作也会记录在 Raft 日志中，保证分片迁移过程的顺序和一致性。
// 空日志条目: Raft 有时会生成空日志条目以保持领导者的状态和活动性，避免在某些情况下集群处于无操作状态。

// NewOperationCommand 从CommandArgs中创建一个新的操作命令
func NewOperationCommand(args *CommandArgs) Command {
	// 客户端命令 (Command): 包含对键值存储的Put、Append、Get等操作，这些操作会通过 Raft 日志提交来保证多副本的一致性
	return Command{Operation, *args}
}

// NewConfigurationCommand创建一个新的配置命令
func NewConfigurationCommand(config *shardctrler.Config) Command {
	//// 配置变更 (Config): 当从 shardctrler 获取到新的分片配置时，会通过 Raft 日志来记录配置的变化。所有副本组通过 Raft 日志共享同一配置，确保分片的一致分配
	return Command{Configuration, *config}
}

// NewInsertShardsCommand创建一个新命令来插入分片
func NewInsertShardsCommand(reply *ShardOperationReply) Command {
	// 分片操作 (ShardOperation): 当进行分片迁移时，涉及到的操作也会记录在 Raft 日志中，保证分片迁移过程的顺序和一致性
	//创建一个 Command 类型的命令，用于将分片数据插入到当前 ShardKV 节点的 Raft 日志中
	return Command{InsertShards, *reply}
}

// NewDeleteShardsCommand创建一个删除分片的新命令
func NewDeleteShardsCommand(args *ShardOperationArgs) Command {

	//创建一个 Command 类型的命令，用于从当前 ShardKV 节点的 Raft 日志中删除分片数据
	return Command{DeleteShards, *args}
}

// NewEmptyShardsCommand创建一个新的命令，表示没有shardscommand
func NewEmptyShardsCommand() Command {
	//// 空日志条目: Raft 有时会生成空日志条目以保持领导者的状态和活动性，避免在某些情况下集群处于无操作状态
	return Command{EmptyShards, nil}
}
