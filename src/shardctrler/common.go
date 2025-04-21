package shardctrler

import (
	"fmt"
	"time"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const ExecuteTimeout = 500 * time.Millisecond

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number 初始配置的编号为 0，每次通过 Join、Leave 或 Move 操作创建新配置时，编号加 1
	Shards [NShards]int     // shard -> gid  表示分片 i（从 0 到 NShards-1）当前由哪个副本组（通过 GID 标识）负责。
	Groups map[int][]string // gid -> servers[] 记录系统中活跃的副本组及其服务器地址列表。
	//值（[]string） 该副本组中所有服务器的地址列表（例如 ["server1", "server2"]）。
}

func DefaultConfig() Config {
	return Config{
		Groups: make(map[int][]string),
	}
}

func (cf Config) String() string {
	return fmt.Sprintf("{Num:%v,Shards:%v,Groups:%v}", cf.Num, cf.Shards, cf.Groups)
}

type OpType uint8

const (
	Join OpType = iota
	Leave
	Move
	Query
)

func (op OpType) String() string {
	switch op {
	case Join:
		return "Join"
	case Leave:
		return "Leave"
	case Move:
		return "Move"
	case Query:
		return "Query"
	default:
		panic(fmt.Sprintf("unknown operation type %d", op))
	}
}

type Err uint8

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeout
)

func (e Err) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	default:
		panic(fmt.Sprintf("unknown error type %d", e))
	}
}

type CommandReply struct {
	Err    Err
	Config Config
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v,Config:%v}", reply.Err, reply.Config)
}

type CommandArgs struct {
	//
	Servers map[int][]string // for Join 用于将一组服务器加入到分布式系统中某个组或集群中。
	GIDs    []int            //for Leave  用于从分布式系统中移除某些组或服务器
	Shard   int              //for Move Shard 可能代表一个特定的分片 ID（shard ID)
	//Shards=[1,1,1,2,2,2,3,3,3,3]：
	// 表示 10 个分片分配给 3 个副本组：
	// 分片 0-2：GID 1。
	// 分片 3-5：GID 2。
	// 分片 6-9：GID 3。
	GID       int //for Move 用于在分布式系统中移动数据或责任（如将某个组的数据迁移到另一个组）
	Num       int //for Query 用于查询分布式系统的状态或信息
	Op        OpType
	ClientId  int64
	CommandId int64
}

func (args CommandArgs) String() string {
	switch args.Op {
	case Join:
		return fmt.Sprintf("{Servers:%v,Op:%v,ClientId:%v,CommandId:%v}", args.Servers, args.Op, args.ClientId, args.CommandId)
	case Leave:
		return fmt.Sprintf("{GIDs:%v,Op:%v,ClientId:%v,CommandId:%v}", args.GIDs, args.Op, args.ClientId, args.CommandId)
	case Move:
		return fmt.Sprintf("{Shard:%v,GID:%v,Op:%v,ClientId:%v,CommandId:%v}", args.Shard, args.GID, args.Op, args.ClientId, args.CommandId)
	case Query:
		return fmt.Sprintf("{Num:%v,Op:%v,ClientId:%v,CommandId:%v}", args.Num, args.Op, args.ClientId, args.CommandId)
	default:
		panic(fmt.Sprintf("unknown operation type %d", args.Op))
	}
}

type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}
type Command struct {
	*CommandArgs
}
