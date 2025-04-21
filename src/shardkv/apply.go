package shardkv

import "6.5840/shardctrler"

// updateShardStatus 根据下次配置更新分片状态。
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	for shardID := 0; shardID < shardctrler.NShards; shardID++ {
		//检查shard的组是否从当前配置更改到下一个配置。
		//这个shard不负责这个gid，但是下一个配置中的gid负责这个shard，所以需要拉出这个shard。
		if kv.currentConfig.Shards[shardID] != kv.gid && nextConfig.Shards[shardID] == kv.gid {
			//获取新组Id
			gid := kv.currentConfig.Shards[shardID]
			//如果group为0，则跳过该shard，因为这意味着该shard没有分配给任何组
			if gid != 0 {
				kv.stateMachine[shardID].Status = Pulling
			}
		}
		//检查shard的组是否从next更改为当前配置
		//这个shard由这个gid负责，但是下一个配置中的gid不负责这个shard，所以需要由其他组来拉

		if kv.currentConfig.Shards[shardID] == kv.gid && nextConfig.Shards[shardID] != kv.gid {
			//获取新组Id
			gid := nextConfig.Shards[shardID]
			//如果group为0，则跳过该shard，因为这意味着该shard没有分配给任何组
			if gid != 0 {
				kv.stateMachine[shardID].Status = BePulling
			}
		}
	}
}

// applyOperation 将给定的操作应用于KV状态机
func (kv *ShardKV) applyOperation(operation *CommandArgs) *CommandReply {

	reply := new(CommandReply)
	shardID := key2shard(operation.Key)

	// 检查服务器是否可以提供请求的分片
	if !kv.canServe(shardID) {
		DPrintf("错误分组")
		reply.Err = ErrWrongGroup
	} else {
		//检查操作是否重复（仅针对非get操作）
		if operation.Op != Get && kv.isDuplicateRequest(operation.ClientId, operation.CommandId) {
			DPrintf("{Node %v}{Group %v} does not apply duplicated commandId %v to stateMachine because maxAppliedCommandId is %v for clientId %v", kv.rf.GetId(), kv.gid, operation.CommandId, kv.lastOperations[operation.ClientId].MaxAppliedCommandId, operation.ClientId)
			lastReply := kv.lastOperations[operation.ClientId].LastReply
			reply.Value, reply.Err = lastReply.Value, lastReply.Err
		} else {
			//将操作应用到状态机
			reply = kv.applyLogToStateMachine(operation, shardID)
			//更新最后提交
			if operation.Op != Get {
				kv.lastOperations[operation.ClientId] = OperationContext{
					operation.CommandId,
					reply,
				}
			}
		}
	}
	return reply
}

// applylogtostatemmachine 将操作日志应用到状态机
func (kv *ShardKV) applyLogToStateMachine(operation *CommandArgs, shardID int) *CommandReply {
	reply := new(CommandReply)
	switch operation.Op {
	case Get:
		reply.Value, reply.Err = kv.stateMachine[shardID].Get(operation.Key)
	case Put:
		reply.Err = kv.stateMachine[shardID].Put(operation.Key, operation.Value)
	case Append:
		reply.Err = kv.stateMachine[shardID].Append(operation.Key, operation.Value)
	}
	return reply
}

// applyConfiguration应用一个新的配置到shard。
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandReply {
	reply := new(CommandReply)
	//检查新配置是否是当前配置的下一个配置
	if nextConfig.Num == kv.currentConfig.Num+1 {
		DPrintf("{Node %v}{Group %v} updates currentConfig from %v to %v", kv.rf.GetId(), kv.gid, kv.currentConfig, nextConfig)
		//基于新配置更新分片的状态
		kv.updateShardStatus(nextConfig)

		//保存最后的配置
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		reply.Err = OK
	} else {
		DPrintf("{Node %v}{Group %v} discards outdated configuration %v when currentConfig is %v", kv.rf.GetId(), kv.gid, nextConfig, kv.currentConfig)
		reply.Err = ErrOutDated
	}
	return reply
}

// applyInsertShards应用分片数据插入
func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationReply) *CommandReply {
	reply := new(CommandReply)
	//检查配置号是否匹配当前配置号
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v} accepts shards insertion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, shardsInfo, kv.currentConfig)
		for shardID, shardData := range shardsInfo.Shards {
			shard := kv.stateMachine[shardID]
			//只有当分片处于pull状态时才会pull
			if shard.Status == Pulling {
				for key, value := range shardData {
					shard.Put(key, value)
				}
				//更新shard状态为Garbage Collecting
				shard.Status = GCing
				reply.Err = OK
			} else {
				DPrintf("{Node %v}{Group %v} encounters duplicated shards insertion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, shardsInfo, kv.currentConfig)
				break
			}
		}
		for clientID, operationContext := range shardsInfo.LastOperations {
			if lastOperation, ok := kv.lastOperations[clientID]; !ok || lastOperation.MaxAppliedCommandId < operationContext.MaxAppliedCommandId {
				kv.lastOperations[clientID] = operationContext
			}
		}
	} else {
		DPrintf("{Node %v}{Group %v} discards outdated shards insertion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, shardsInfo, kv.currentConfig)
		reply.Err = ErrOutDated
	}
	return reply
}

// applydeletesards 应用删除分片数据
func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationArgs) *CommandReply {
	//检查配置号是否一致
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v}'s shards status are %v before accepting shards deletion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, kv.stateMachine, shardsInfo, kv.currentConfig)
		for _, shardID := range shardsInfo.ShardIDs {
			//删除指定分片
			shard := kv.stateMachine[shardID]
			if shard.Status == GCing { //如果该分片正在被垃圾收集，则更新状态为正在服务。
				shard.Status = Serving
			} else if shard.Status == BePulling { //如果shard正在被拉，重置shard为一个新的shard
				kv.stateMachine[shardID] = NewShard()
			} else { //如果碎片不在预期状态，则退出。
				DPrintf("{Node %v}{Group %v} encounters duplicated shards deletion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, shardsInfo, kv.currentConfig)
				break
			}
		}
		DPrintf("{Node %v}{Group %v}'s shards status are %v after accepting shards deletion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, kv.stateMachine, shardsInfo, kv.currentConfig)
		return &CommandReply{Err: OK}
	}

	DPrintf("{Node %v}{Group %v} discards outdated shards deletion %v when currentConfig is %v", kv.rf.GetId(), kv.gid, shardsInfo, kv.currentConfig)
	return &CommandReply{Err: OK}
}

// applyEmptyShards 处理空碎片的情况。这是为了防止状态机回滚
func (kv *ShardKV) applyEmptyShards() *CommandReply {
	return &CommandReply{Err: OK}
}
