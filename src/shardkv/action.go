package shardkv

import "sync"

// configurationAction 检查是否可以执行下一个配置,如果所有的shard都处于服务状态，则查询并应用下一个配置。
func (kv *ShardKV) configurationAction() {
	canPerformNextConfig := true
	kv.mu.RLock()
	//如果没有分片处于服务状态，则不能应用下一个配置
	for _, shard := range kv.stateMachine {
		if shard.Status != Serving {
			canPerformNextConfig = false
			DPrintf("{Node %v}{Group %v} will not try to fetch latest configuration because shards status are %v when currentConfig is %v", kv.rf.GetId(), kv.gid, kv.stateMachine, kv.currentConfig)
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()

	//如果允许，查询并应用下一个配置
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			DPrintf("{Node %v}{Group %v} fetches latest configuration %v when currentConfigNum is %v", kv.rf.GetId(), kv.gid, nextConfig, currentConfigNum)
			kv.Execute(NewConfigurationCommand(&nextConfig), new(CommandReply))
		}
	}
}

// migrationAction 执行迁移任务，从其他组中拉出分片数据
// func (kv *ShardKV) migrationAction() {
// 	kv.mu.RLock()
// 	gid2Shards := kv.getShardIDsByStatus(Pulling) //需要从旧组拉出哪些shardID gid->shardId
// 	var wg sync.WaitGroup                         //创建 sync.WaitGroup，用于等待所有异步拉取任务（goroutine）完成

// 	// 为每个组（GID）创建pull task
// 	for gid, shardIDs := range gid2Shards {
// 		DPrintf("{Node %v}{Group %v} starts a PullTask to get shards %v from group %v when config is %v", kv.rf.GetId(), kv.gid, shardIDs, gid, kv.currentConfig)
// 		wg.Add(1)
// 		go func(servers []string, configNum int, shardIDs []int) {
// 			defer wg.Done()
// 			pullTaskArgs := ShardOperationArgs{configNum, shardIDs}
// 			//尝试从组中的每个服务器提取分片数据
// 			for _, server := range servers {
// 				pullTaskReply := new(ShardOperationReply)
// 				srv := kv.make_end(server)
// 				// ok := srv.Call("ShardKV.GetShardsData", &pullTaskArgs, pullTaskReply)
// 				// if !ok || pullTaskReply.Err == ErrWrongLeader {
// 				// 	// 试下一个副本
// 				// 	break
// 				// }
// 				// if pullTaskReply.Err == ErrNotReady {
// 				// 	// 对方还没更新到 configNum，等会儿重试
// 				// 	time.Sleep(10 * time.Millisecond)
// 				// 	continue
// 				// }
// 				// if pullTaskReply.Err == OK {
// 				// 	// 数据准备好了，提交插入命令
// 				// 	kv.Execute(NewInsertShardsCommand(pullTaskReply), new(CommandReply))
// 				// }
// 				// break
// 				if srv.Call("ShardKV.GetShardsData", &pullTaskArgs, pullTaskReply) && pullTaskReply.Err == OK {
// 					//从这些服务器提取数据
// 					DPrintf("{Node %v}{Group %v} gets a PullTaskReply %v and tries to commit it when currentConfigNum is %v", kv.rf.GetId(), kv.gid, pullTaskReply, configNum)
// 					kv.Execute(NewInsertShardsCommand(pullTaskReply), new(CommandReply))
// 				}
// 			}
// 		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
// 	}
// 	kv.mu.RUnlock()
// 	wg.Wait() //等待所有的pull任务完成

// }
// Executes the migration task to pull shard data from other groups.
func (kv *ShardKV) migrationAction() {
	kv.mu.RLock()
	gid2Shards := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup

	// Create pull tasks for each group (GID)
	for gid, shardIDs := range gid2Shards {
		DPrintf("{Node %v}{Group %v} starts a PullTask to get shards %v from group %v when config is %v", kv.rf.GetId(), kv.gid, shardIDs, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskArgs := ShardOperationArgs{configNum, shardIDs}
			// Try to pull shard data from each server in the group
			for _, server := range servers {
				pullTaskReply := new(ShardOperationReply)
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskArgs, pullTaskReply) && pullTaskReply.Err == OK {
					//Pulling data from these servers
					DPrintf("{Node %v}{Group %v} gets a PullTaskReply %v and tries to commit it when currentConfigNum is %v", kv.rf.GetId(), kv.gid, pullTaskReply, configNum)
					kv.Execute(NewInsertShardsCommand(pullTaskReply), new(CommandReply))
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait() // Wait for all pull tasks to complete
}

// gcAction 执行垃圾收集（GC）任务，从其他组中删除分片数据。
func (kv *ShardKV) gcAction() {
	kv.mu.RLock()
	//获取之前负责这些分片的组，并清理不再负责的分片。
	gid2Shards := kv.getShardIDsByStatus(GCing)
	var wg sync.WaitGroup

	// 为每个组创建GCtask
	for gid, shardIDs := range gid2Shards {
		DPrintf("{Node %v}{Group %v} starts a GCTask to delete shards %v from group %v when config is %v", kv.rf.GetId(), kv.gid, shardIDs, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			gcTaskArgs := ShardOperationArgs{configNum, shardIDs}
			for _, server := range servers {
				gcTaskReply := new(ShardOperationReply)
				srv := kv.make_end(server)
				//这里RpC是将旧组中的BePulling状态的数据删除
				if srv.Call("ShardKV.DeleteShardsData", &gcTaskArgs, gcTaskReply) && gcTaskReply.Err == OK {
					DPrintf("{Node %v}{Group %v} deletes shards %v in remote group successfully when currentConfigNum is %v", kv.rf.GetId(), kv.gid, shardIDs, configNum)
					// 不是重复调用，这里是将新组中的Gc状态改为服务，
					kv.Execute(NewDeleteShardsCommand(&gcTaskArgs), new(CommandReply))
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

// checkEntryInCurrentTermAction 确保日志条目在当前期限中存在，以保持日志活动。
func (kv *ShardKV) checkEntryInCurrentTermAction() {
	// 如果当前期限内没有日志条目，则执行空命令
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyShardsCommand(), new(CommandReply))
	}
}

// GetShardsData 处理 从Leader服务器获取分片数据。
func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 场景：配置变更（Configuration 命令）导致分片 0 从副本组 GID 1 重新分配到 GID 2：
	// GID 1 的分片 0 进入 BePulling 状态，准备发送数据。
	// GID 2 的分片 0 进入 Pulling 状态，需要通过 RPC 调用 GID 1 的 GetShardsData 获取数据。
	// 目标：展示 GetShardsData 如何处理 GID 2 的请求，返回分片 0 的数据和上下文

	//只从leader处提取碎片
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	defer DPrintf("{Node %v}{Group %v} processes PullTaskRequest %v with PullTaskReply %v", kv.rf.GetId(), kv.gid, args, reply)

	// 检查当前配置是否与请求中的配置一致
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	//检查当前配置是否为所请求的操作做好了准备
	reply.Shards = make(map[int]map[string]string)
	for _, shardId := range args.ShardIDs {
		//将分片种的kv拿出来，存入reply中的shards
		reply.Shards[shardId] = kv.stateMachine[shardId].deepCopy()
	}

	reply.LastOperations = make(map[int64]OperationContext)
	for clientId, operationContext := range kv.lastOperations {
		reply.LastOperations[clientId] = operationContext.deepCopy()
	}

	// 设置配置号并返回OK
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

// 用于处理从 Leader 节点获取分片数据的请求，通常在分片迁移过程中由新组（Pulling 状态）向旧组（BePulling 状态）发起
func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	//只有是leader时才会删除
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("{Node %v}{Group %v} processes GCTaskRequest %v with GCTaskReply %v", kv.rf.GetId(), kv.gid, args, reply)
	kv.mu.RLock()

	//检查当前配置是否大于请求的配置
	if kv.currentConfig.Num > args.ConfigNum {
		DPrintf("{Node %v}{Group %v} encounters duplicated shards deletion request %v when currentConfig is %v", kv.rf.GetId(), kv.gid, args, kv.currentConfig)
		reply.Err = ErrOutDated
		kv.mu.RUnlock()
		return
	}
	if kv.currentConfig.Num < args.ConfigNum {
		DPrintf("{Node %v}{Group %v} encounters notready shards deletion request %v when currentConfig is %v", kv.rf.GetId(), kv.gid, args, kv.currentConfig)
		reply.Err = ErrNotReady
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	//DeleteShardsData
	commandReply := new(CommandReply)
	kv.Execute(NewDeleteShardsCommand(args), commandReply)
	reply.Err = commandReply.Err
}
