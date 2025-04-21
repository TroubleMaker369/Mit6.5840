package shardkv

import "fmt"

type ShardStatus uint8

// ShardStatus表示分片状态的类型

const (
	Serving          ShardStatus = iota //表示该分片正在正常地为客户端提供读写服务
	Pulling                             //表明该分片正在从其他服务器拉取数据
	BePulling                           //BePulling状态意味着该分片的数据正在被其他服务器拉取
	GCing                               //GCing即垃圾回收（Garbage Collection）状态
	confirmMigration                    //完成迁移
)

func (status ShardStatus) String() string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case GCing:
		return "GCing"
	case confirmMigration:
		return "confirmMigration"
	default:
		panic(fmt.Sprintf("Unknown ShardStatus: %d", status))
	}
}

// Shard表示一个键值存储及其状态
type Shard struct {
	KV     map[string]string
	Status ShardStatus //当前分片的状态
}

// NewShard 创建并初始化一个新的Shard实例
func NewShard() *Shard {
	return &Shard{
		KV:     make(map[string]string),
		Status: Serving,
	}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (Shard *Shard) Put(key, value string) Err {
	Shard.KV[key] = value
	return OK
}
func (Shard *Shard) Append(key, value string) Err {
	Shard.KV[key] += value
	return OK
}

// deepCopy创建一个分片键值对的副本。
// 返回一个包含shard中所有键值对的新映射
func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}
