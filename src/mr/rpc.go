package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//总体框架是worker 需要向Coordinator发送消息

// 对于worker发送消息，worker需要和Coordinator报告Map或者Reduce任务的执行情况
type TaskCompletedStatus int

// woker 告知coordinator 自身任务的完成情况
const (
	MapTaskCompleted = iota
	MapTaskFailed
	ReduceCompleted
	ReduceFailed
)

// worker在发送给coordinator的时候还需要带自身task的一些状态
type MessageSend struct {
	TaskID              int
	TaskCompletedStatus TaskCompletedStatus
}

// 对于coordinator回复消息，需要告知其分配什么任务  map\reduce
type TaskType int

const (
	MapTask = iota
	ReduceTask
	Wait
	Exit
)

type MessageReply struct {
	TaskID   int      //task Id
	TaskType TaskType //任务类型
	TaskFile string   //map任务需要的输入文件
	NReduce  int      //Reduce 数量  10
	NMap     int      //Map任务的数量
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
