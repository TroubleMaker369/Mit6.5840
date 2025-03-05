package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := MessageSend{}
		reply := MessageReply{}
		call("Coordinator.RequestTask", &args, &reply)
		switch reply.TaskType {
		case MapTask:
			SugarLogger.Infof("worker 处理%d号Map任务", reply.TaskID)
			HandleMapTask(&reply, mapf)
		case ReduceTask:
			SugarLogger.Infof("worker 处理%d号Reduce任务", reply.TaskID)
			HandleReduceTask(&reply, reducef)
		case Wait:
			SugarLogger.Infof("%d号worker进入等待", reply.TaskID)
			time.Sleep(1 * time.Second)
		case Exit:
			SugarLogger.Infof("%d号worker退出", reply.TaskID)
			os.Exit(0)
		default:
			SugarLogger.Info("未知状态")
			time.Sleep(1 * time.Second)
		}
	}
}

// HandleMapTask 处理Map逻辑
func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) {
	// 1. 打开文件
	args := MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: MapTaskFailed,
	}
	file, err := os.Open(reply.TaskFile)
	if err != nil {
		SugarLogger.Infof("HandleMapTask  cannot open %s", reply.TaskFile)
		call("Coordinator.ReportTask", &args, &MessageReply{})
		return
	}

	// 2. 读文件
	content, err := io.ReadAll(file)
	if err != nil {
		SugarLogger.Infof("HandleMapTask  cannot read %s", reply.TaskFile)
		call("Coordinator.ReportTask", &args, &MessageReply{})
		return
	}
	file.Close()
	// 3. 送入map
	kva := mapf(reply.TaskFile, string(content))
	// 4. 构建数据结构 [][]KeyValue
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	// 5. 构建中间文件
	// 6. 写入中间文件
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskID, r)
		ofile, err := os.CreateTemp("", oname) //空字符串 "" 表示使用操作系统的默认临时目录（通常是系统的 /tmp ）
		if err != nil {
			SugarLogger.Infof("HandleMapTask cannot create tempfile %s", oname)
			call("Coordinator.ReportTask", &args, &MessageReply{})
			return
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
	// 7. 向Coordination报告执行完毕
	args = MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: MapTaskCompleted,
	}
	// 返回
	call("Coordinator.ReportTask", &args, &MessageReply{})
	SugarLogger.Infof("%d woker 执行 Map 任务完毕", args.TaskID)
}

// generateFileName 获取rReduce对应的所有的Nmap的输出文件名称
func generateFileName(r int, NMap int) []string {
	var filename []string
	for TaskId := 0; TaskId < NMap; TaskId++ {
		filename = append(filename, fmt.Sprintf("mr-%d-%d", TaskId, r))
	}
	return filename
}

// HandleReduceTask 处理Reduce逻辑
func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) {
	// 1. 构建读取的中间文件
	filenames := generateFileName(reply.TaskID, reply.NMap)

	// 构建失败消息
	args := MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: ReduceFailed,
	}
	// 2. 读取文件内容
	var intermediate []KeyValue
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			SugarLogger.Infof("cannot open %s", filename)
			call("Coordinator.ReportTask", &args, &MessageReply{})
			return
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 3. 按照Key 进行排序
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})
	// 4. 构建输出文件
	oname := fmt.Sprintf("mr-out-%v", reply.TaskID) //临时文件
	ofile, err := os.Create(oname)
	if err != nil {
		SugarLogger.Infof("cannot create %s", oname)
		call("Coordinator.ReportTask", &args, &MessageReply{})
		return
	}
	SugarLogger.Infof("创建输出文件%s成功", oname)
	// 5. 送入Reduce
	for i := 0; i < len(intermediate); {
		var value []string
		value = append(value, intermediate[i].Value)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			value = append(value, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, value)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// 6. 构建并写入输出文件
	ofile.Close()
	os.Rename(ofile.Name(), oname)
	// 7. 向Coordinator报告执行结果
	args = MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: ReduceCompleted,
	}

	call("Coordinator.ReportTask", &args, &MessageReply{})
	SugarLogger.Infof("%d woker 执行 Reduce 任务完毕", args.TaskID)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

/*
xy@xy:~/mit2024/6.5840/src/main$ bash test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
*/
