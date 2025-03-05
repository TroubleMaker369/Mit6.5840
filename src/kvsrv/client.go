package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// InitLogger()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	for !ck.server.Call("KVServer.Get", &args, &reply) { //无限重试，知道成功
		// SugarLogger.Infof("客户端调用Get失败,开始重试")
	}
	// You will have to modify this function.
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

//带日志的无法通过内存测试
// func (ck *Clerk) PutAppend(key string, value string, op string) string {
// 	// You will have to modify this function.
// 	messageID := nrand()
// 	args := PutAppendArgs{
// 		Key:         key,
// 		Value:       value,
// 		MessageType: Modify,
// 		MessageID:   messageID,
// 	}
// 	reply := PutAppendReply{}
// 	SugarLogger.Infof("客户端开始调用KVServer.%s;MessageID为:%d", op, messageID)
// 	for !ck.server.Call("KVServer."+op, &args, &reply) {
// 		SugarLogger.Infof("客户端调用KVServer.%s失败;MessageID为:%d", op, messageID)
// 	}
// 	SugarLogger.Infof("客户端调用KVServer.%s成功;MessageID为:%d;开始向服务器汇报完成", op, messageID)
// 	args = PutAppendArgs{
// 		MessageType: Report,
// 		MessageID:   messageID,
// 	}
// 	for !ck.server.Call("KVServer."+op, &args, &reply) {
// 		SugarLogger.Infof("MessageID为:%d 任务汇报", op, messageID)
// 	}
// 	SugarLogger.Infof("MessageID为:%d 任务汇报成功", op, messageID)
// 	return reply.Value
// }

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	messageID := nrand()
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		MessageType: Modify,
		MessageID:   messageID,
	}
	reply := PutAppendReply{}
	for !ck.server.Call("KVServer."+op, &args, &reply) {

	}
	args = PutAppendArgs{
		MessageType: Report,
		MessageID:   messageID,
	}
	for !ck.server.Call("KVServer."+op, &args, &reply) {
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
