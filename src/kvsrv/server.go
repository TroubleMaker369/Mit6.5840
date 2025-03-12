package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]string
	// Your definitions here.
	record sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.MessageType == Report { //此请求是针对某个已完成或已撤销的操作，用于通知服务器需要移除相关数据。
		kv.record.Delete(args.MessageID)
		return
	}
	res, ok := kv.record.Load(args.MessageID)
	if ok {
		reply.Value = res.(string) // 重复请求，返回之前的结果
		return
	}
	// 非重复请求
	kv.mu.Lock()
	ordValue := kv.data[args.Key] //旧值需要存入map中，再该请求没有被确认之前，返回的一定是之前的旧值重复请求可直接返回之前的缓存
	kv.data[args.Key] = args.Value
	reply.Value = ordValue
	kv.mu.Unlock()
	kv.record.Store(args.MessageID, ordValue)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.MessageID == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, ok := kv.record.Load(args.MessageID)
	if ok { //重复请求，返回缓存的结果
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	oldValue := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	kv.mu.Unlock()
	kv.record.Store(args.MessageID, oldValue)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	// InitLogger()
	// You may need initialization code here.

	return kv
}
