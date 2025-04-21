package shardctrler

import (
	"sort"
)

type ConfigStateMachine interface {
	Join(group map[int][]string) Err //键是副本组的唯一标识符（GID，非零整数），值是该组中服务器的地址列表（例如 ["server1:port", "server2:port"]）。
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	cf := &MemoryConfigStateMachine{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

// Join 添加新的组进入到配置中
func (cf *MemoryConfigStateMachine) Join(group map[int][]string) Err {
	DPrintf("状态机执开始行Join")
	lastConfig := cf.Configs[len(cf.Configs)-1]
	//基于最后一次的配置创建新的配置
	newConfig := Config{
		len(cf.Configs),
		lastConfig.Shards,
		deepCopy(lastConfig.Groups),
	}

	for gid, servers := range group {
		//如果group不在新配置里面，就添加
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	group2Shards := Group2Shards(newConfig) //gid-shards
	for {
		// 对组之间的分片进行负载均衡
		source, target := GetGidWithMaximumShards(group2Shards), GetGidWithMinimumShards(group2Shards)
		if source != 0 && len(group2Shards[source])-len(group2Shards[target]) <= 1 {
			break
		}
		group2Shards[target] = append(group2Shards[target], group2Shards[source][0])
		group2Shards[source] = group2Shards[source][1:]
	}
	//更新新配置中的分片分配
	var newShards [NShards]int
	for gid, shards := range group2Shards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	DPrintf("状态机执行Join完成")
	return OK

}

// Leave  从配置中删除指定的组
func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{
		len(cf.Configs),
		lastConfig.Shards,
		deepCopy(lastConfig.Groups),
	}

	group2Shards := Group2Shards(newConfig)
	//用于存储孤儿碎片
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		//如果新配置中存在该组，则删除它
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// 删除gid -> shards的映射
		if shards, ok := group2Shards[gid]; ok {
			delete(group2Shards, gid)
			orphanShards = append(orphanShards, shards...)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		//将孤立碎片重新分配给剩余的组
		for _, shard := range orphanShards {
			gid := GetGidWithMinimumShards(group2Shards)
			newShards[shard] = gid
			group2Shards[gid] = append(group2Shards[gid], shard)
		}

		//在新配置中更新分片分配
		for gid, shard := range group2Shards {
			for _, shard := range shard {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// Move 命令用于将指定的shard移动到指定的组中。
func (cf *MemoryConfigStateMachine) Move(shard, gid int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	//根据上次配置创建新配置
	newConfig := Config{
		len(cf.Configs),
		lastConfig.Shards,
		lastConfig.Groups,
	}
	//根据上次配置创建新配置
	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// 查询指定配置
func (cf *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	//如果配置号无效，则返回最新的配置
	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	return cf.Configs[num], OK
}

// GetGidWithMinimumShards 返回具有最大分片数的组ID
func GetGidWithMinimumShards(group2Shards map[int][]int) int {
	//函数的目的是找到一个活跃的副本组（gid != 0），以接收从其他组（如 GID 0 或分片多的组）移动来的分片。
	// 选择分片最少的有效组（如 GID 2 有 2 个分片），确保分配后负载更均衡（如接近 NShards / len(Groups)）

	// 获得所有的组Id
	var gids []int
	for gid := range group2Shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	index, minShards := -1, NShards+1
	for _, gid := range gids {
		// 不考虑0组
		if gid != 0 && len(group2Shards[gid]) < minShards {
			index, minShards = gid, len(group2Shards[gid])
		}
	}
	return index
}

// GetGIDWithMaximumShards 返回具有最大分片数的组ID
func GetGidWithMaximumShards(group2Shards map[int][]int) int {
	//Group表示未分配的组，如果有未分配的分片，则选择gid 0
	if shards, ok := group2Shards[0]; ok && len(shards) != 0 {
		//初始配置：Shards = [0,0,...,0]，所有分片分配给 GID 0，表示系统启动时无有效组（Groups = {}）。
		//组id0中的分片  都是未分配组的分片，需要优先处理
		return 0
	}
	var gids []int
	for gid := range group2Shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	index, maxShards := -1, -1
	//查找具有最大分片数的组ID
	for _, gid := range gids {
		if len(group2Shards[gid]) > maxShards {
			index, maxShards = gid, len(group2Shards[gid])
		}
	}
	return index
}

// Group2Shards Group2Shards 将 Config.Shards（分片到 GID 的数组）转换为 map[int][]int（GID 到分片列表的映射）。
func Group2Shards(config Config) map[int][]int {
	group2Shards := make(map[int][]int)
	for gid := range config.Groups {
		group2Shards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		group2Shards[gid] = append(group2Shards[gid], shard)
	}
	return group2Shards
}

// deepCopy 创建一个组映射的  深拷贝副本
func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gids, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gids] = newServers
	}
	return newGroups
}
