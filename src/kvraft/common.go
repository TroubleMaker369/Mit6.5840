package kvraft

import (
	"fmt"
	"time"
)

// 用来设置超时时间
const ExecuteTimeout = 1000 * time.Millisecond

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		SugarKVLogger.Infof(format, a...)
		// log.Printf(format, a...)
	}
}

// 设置三种操作状态
type OpType uint8

const (
	OpPut OpType = iota
	OpAppend
	OpGet
)

func (opType OpType) String() string {
	switch opType {
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	case OpGet:
		return "Get"
	}
	panic(fmt.Sprintf("unexpected OpType %d", opType))
}

// CommandArgs 客户端请求执行命令RPC参数
type CommandArgs struct {
	Key       string
	Value     string
	Op        OpType
	ClientId  int64
	CommandId int64
}

func (args CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v, Value:%v, Op:%v, ClientId:%v, Id:%v}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v, Value:%v}", reply.Err, reply.Value)
}

// OperationContext 用于记录某个客户端的操作历史和状态
type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

type Command struct {
	*CommandArgs
}

// kvraft响应客户端的错误状态
type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "Ok"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}
