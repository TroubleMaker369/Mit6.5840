package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIds map[int]int //gid->leaderID
	clientID  int64
	commandID int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	InitLogger()
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		leaderIds: make(map[int]int),
		clientID:  nrand(),
		commandID: 0,
	}
	//从shardctrler中查询最新配置
	// DPrintf("客户端%v:初始化完成，查询最新配置", ck.clientID)
	ck.config = ck.sm.Query(-1)
	// DPrintf("客户端%v:查询最新配置完成,配置号为%d", ck.clientID, ck.config.Num)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: Get})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Put})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Append})
}

func (ck *Clerk) Command(args *CommandArgs) string {
	DPrintf("客户端%v发起%v请求", ck.clientID, args.Op)
	args.ClientId, args.CommandId = ck.clientID, ck.commandID
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			//如果没有设置，则设置默认的leader id为0
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeader := oldLeaderId
			for {
				reply := new(CommandReply)
				//发送请求到领导服务器
				ok := ck.make_end(servers[newLeader]).Call("ShardKV.Command", args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("客户端%v执行%v请求成功", ck.clientID, args.Op)
					ck.commandID++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					DPrintf("客户端%v执行%v请求失败，重试中", ck.clientID, args.Op)
					break
				} else {
					//尝试下一个服务器
					DPrintf("客户端%v执行%v请求失败，", ck.clientID, args.Op)
					newLeader = (newLeader + 1) % len(servers)
					//检查是否已尝试所有服务器
					if newLeader == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		//从shardctrler中查询最新配置
		ck.config = ck.sm.Query(-1)
	}
}
