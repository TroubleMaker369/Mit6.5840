package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk 构造客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	InitKVLogger()
	ck := &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.ExecuteCommand(&CommandArgs{
		Key: key,
		Op:  OpGet,
	})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// func (ck *Clerk) PutAppend(key string, value string, op string) {
// 	// You will have to modify this function.
// }

func (ck *Clerk) Put(key string, value string) {
	ck.ExecuteCommand(&CommandArgs{
		Key:   key,
		Value: value,
		Op:    OpPut,
	})
}
func (ck *Clerk) Append(key string, value string) {
	ck.ExecuteCommand(&CommandArgs{
		Key:   key,
		Value: value,
		Op:    OpAppend,
	})
}

// ExecuteCommand RPC调用
func (ck *Clerk) ExecuteCommand(args *CommandArgs) string {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		reply := new(CommandReply)
		if !ck.servers[ck.leaderId].Call("KVServer.ExecuteCommand", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId += 1
		return reply.Value
	}
}
