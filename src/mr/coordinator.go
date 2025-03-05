package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// coordinator 需要定义worker的状态
type TaskStatus int

const (
	Unassigned = iota //未分配
	Assigned          //已分配
	Completed         //完成
	Failed            //失败
)

type Taskinfo struct {
	TaskStatus TaskStatus //worker执行任务的状态
	TaskFile   string     //task file
	TimeStamp  time.Time  // worker开始工作的时间
}

type Coordinator struct {
	// Your definitions here.
	NMap                   int        //Map任务的数量
	NReduce                int        //Reduce数量
	MapTasks               []Taskinfo //Map任务的执行信息
	ReduceTasks            []Taskinfo //Reduce任务的执行信息
	AllMapTaskCompleted    bool
	AllReduceTaskCompleted bool
	Mutex                  sync.Mutex
}

// InitTask 初始化状态
func (c *Coordinator) InitTask(file []string) {
	for idx := range file {
		c.MapTasks[idx] = Taskinfo{
			TaskStatus: Unassigned,
			TaskFile:   file[idx],
			TimeStamp:  time.Now(),
		}
	}
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = Taskinfo{
			TaskStatus: Unassigned,
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:                   len(files),
		NReduce:                nReduce,
		MapTasks:               make([]Taskinfo, len(files)),
		ReduceTasks:            make([]Taskinfo, nReduce),
		AllMapTaskCompleted:    false,
		AllReduceTaskCompleted: false,
		Mutex:                  sync.Mutex{},
	}
	//初始化Logger
	InitLogger()
	// Your code here.
	c.InitTask(files)
	SugarLogger.Info("初始化任务成功")
	c.server()
	return &c
}

// RequestTask 分配任务给Map
func (c *Coordinator) RequestTask(args *MessageSend, reply *MessageReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// 1.如果有未分配的任务、之前执行失败、已分配但已经超时（10s）的Map任务，则选择这个任务进行分配；
	if !c.AllMapTaskCompleted {
		NMapTaskCompleted := 0
		for idx := range c.MapTasks {
			taskinfo := c.MapTasks[idx]
			taskStatus := c.MapTasks[idx].TaskStatus
			if taskStatus == Unassigned || taskStatus == Failed || (taskStatus == Assigned && time.Since(c.MapTasks[idx].TimeStamp) > 10*time.Second) {
				//设置回复信息，交给worker去做
				reply.TaskID = idx
				reply.TaskFile = taskinfo.TaskFile
				reply.TaskType = MapTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce

				//重新设置worker的运行信息
				c.MapTasks[idx].TimeStamp = time.Now()
				c.MapTasks[idx].TaskStatus = Assigned
				SugarLogger.Infof("Coordinator分配%d号Map任务", idx)
				return nil
			} else if taskStatus == Completed {
				NMapTaskCompleted++
			}
		}
		// 2.如果以上的Map任务均不存在，但Map又没有全部执行完成，告知Worker先等待；
		if NMapTaskCompleted == len(c.MapTasks) {
			SugarLogger.Info("所有Map任务完成!修改状态AllMapTaskCompleted为True")
			c.AllMapTaskCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	SugarLogger.Info("Coordinator 开始分配Reduce任务")
	// 3.Map任务全部执行完成的情况下，按照1和2相同的逻辑进行Reduce任务的分配；
	if !c.AllReduceTaskCompleted {
		NReduceTaskCompleted := 0
		for idx := range c.ReduceTasks {
			taskinfo := c.ReduceTasks[idx]
			taskStatus := c.ReduceTasks[idx].TaskStatus
			if taskStatus == Unassigned || taskStatus == Failed || (taskStatus == Assigned && time.Since(c.ReduceTasks[idx].TimeStamp) > 10*time.Second) {
				//设置回复信息，交给worker去做
				reply.TaskID = idx
				reply.TaskFile = taskinfo.TaskFile
				reply.TaskType = ReduceTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce

				//重新设置worker的运行信息
				c.ReduceTasks[idx].TimeStamp = time.Now()
				c.ReduceTasks[idx].TaskStatus = Assigned
				SugarLogger.Infof("Coordinator分配%d号Reduce任务", idx)
				return nil
			} else if taskStatus == Completed {
				NReduceTaskCompleted++
			}
		}
		// 2.如果以上的Reduce任务均不存在(都在已分配状态)，但Map又没有全部执行完成，告知Worker先等待；
		if NReduceTaskCompleted == len(c.ReduceTasks) {
			SugarLogger.Info("所有Reduce任务完成!修改状态NReduceTaskCompleted为True")
			c.AllReduceTaskCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	// 4.所有的任务都执行完成了, 告知Worker退出。
	reply.TaskType = Exit
	return nil
}

// ReportTask 根据Worker发送的消息任务完成状态来更新任务状态信息即可
func (c *Coordinator) ReportTask(args *MessageSend, reply *MessageReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.TaskCompletedStatus == MapTaskCompleted {
		c.MapTasks[args.TaskID].TaskStatus = Completed
		SugarLogger.Infof("Coordinator 确认%d Map任务完成", args.TaskID)
	} else if args.TaskCompletedStatus == MapTaskFailed {
		c.MapTasks[args.TaskID].TaskStatus = Failed
		SugarLogger.Infof("Coordinator 确认%d Map任务失败", args.TaskID)
	} else if args.TaskCompletedStatus == ReduceCompleted {
		c.ReduceTasks[args.TaskID].TaskStatus = Completed
		SugarLogger.Infof("Coordinator 确认%d Reduce任务成功", args.TaskID)
	} else {
		c.ReduceTasks[args.TaskID].TaskStatus = Failed
		SugarLogger.Infof("Coordinator 确认%d Reduce任务失败", args.TaskID)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// for _, maptask := range c.MapTasks {
	// 	if maptask.TaskStatus != Completed {
	// 		return false
	// 	}
	// }
	// for _, reduce := range c.ReduceTasks {
	// 	if reduce.TaskStatus != Completed {
	// 		return false
	// 	}
	// }

	return c.AllMapTaskCompleted && c.AllReduceTaskCompleted
	// return true
}
