package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskType: "map" or "reduce"
type GetTaskArgs struct {
}

type GetTaskReply struct {
	FileName string
	NReduce  int
	TaskType string
	TaskId   int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Printf("GetTask called, %d map tasks left, %d running, %d reduce tasks left, %d running\n", len(c.LeftMapTasks), len(c.RunningMapTasks), len(c.LeftReduceTasks), len(c.RunningReduceTasks))
	if len(c.RunningMapTasks) > 0 || len(c.MapTasks) > len(c.RunningMapTasks)+c.FinishedMapTasks {
		if len(c.MapTasks) > len(c.RunningMapTasks)+c.FinishedMapTasks {
			reply.NReduce = c.nReduce
			for _, task := range c.MapTasks {
				if _, ok := c.RunningMapTasks[task.TaskId]; !ok && !task.done {
					reply.FileName = task.FileName
					reply.TaskId = task.TaskId
					c.RunningMapTasks[reply.TaskId] = make(chan struct{})
					// fmt.Printf("Assigning map task %d for file %s\n", reply.TaskId, reply.FileName)
					reply.TaskType = "map"
					break
				}
			}
			// fmt.Printf("Assigning map task %d for file %s\n", reply.TaskId, reply.FileName)
			go TaskMonitor(10*time.Second, c.RunningMapTasks[reply.TaskId], Task{TaskType: "map", TaskId: reply.TaskId}, c)
			// c.RunningMapTasks = append(c.RunningMapTasks, reply.FileName)
		} else {
			reply.TaskType = "wait"
			// 	TODO:run backup for running map tasks
		}
	} else {
		// fmt.Printf("%d reduce tasks left, %d running\n", len(c.LeftReduceTasks), len(c.RunningReduceTasks))
		if len(c.ReduceTasks) > len(c.RunningReduceTasks)+c.FinishedReduceTasks {
			reply.NReduce = c.nReduce
			for _, task := range c.ReduceTasks {
				if _, ok := c.RunningReduceTasks[task.TaskId]; !ok && !task.done {
					reply.TaskId = task.TaskId
					c.RunningReduceTasks[reply.TaskId] = make(chan struct{})
					reply.TaskType = "reduce"
					break
				}
			}
			// fmt.Printf("Assigning reduce task %d\n", reply.TaskId)
			go TaskMonitor(10*time.Second, c.RunningReduceTasks[reply.TaskId], Task{TaskType: "reduce", TaskId: reply.TaskId}, c)
		} else {
			if len(c.RunningReduceTasks) > 0 {
				reply.TaskType = "wait"
			} else {
				reply.TaskType = "done"
			}
		}
	}
	return nil
}

type FinishTaskArgs struct {
	TaskType string
	FileName string
	TaskId   int
}

type FinishTaskReply struct {
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case "map":
		ch, ok := c.RunningMapTasks[args.TaskId]
		if ok && ch != nil {
			close(ch)
		}
		delete(c.RunningMapTasks, args.TaskId)
		if ok { // map 中有 key，这个任务首次被完成，增加计数
			c.FinishedMapTasks++
			c.MapTasks[args.TaskId].done = true
		}
	case "reduce":
		ch, ok := c.RunningReduceTasks[args.TaskId]
		if ok && ch != nil {
			close(ch)
		}
		delete(c.RunningReduceTasks, args.TaskId)
		if ok {
			c.FinishedReduceTasks++
			c.ReduceTasks[args.TaskId].done = true
		}
	}
	return nil
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type Task struct {
	TaskType string // "map" or "reduce"
	TaskId   int
}

type TimerMonitor struct {
	timer *time.Timer
	// 通知主程序：超时事件，发送超时提示字符串
	notifyCh chan string
	// cancelCh：外部用来取消这个定时器的信号
	cancelCh chan struct{}
}

func TaskMonitor(timeout time.Duration, cancelCh chan struct{}, task Task, c *Coordinator) {
	timer := time.NewTimer(timeout)
	select {
	case <-timer.C:
		// let task be re-assigned
		c.mu.Lock()
		defer c.mu.Unlock()
		switch task.TaskType {
		case "map":
			fmt.Printf("Map task %d timed out, reassigning\n", task.TaskId)
			ch, ok := c.RunningMapTasks[task.TaskId]
			if ok && ch != nil {
				close(ch)
			}
			delete(c.RunningMapTasks, task.TaskId)
		case "reduce":
			fmt.Printf("Reduce task %d timed out, reassigning\n", task.TaskId)
			ch, ok := c.RunningReduceTasks[task.TaskId]
			if ok && ch != nil {
				close(ch)
			}
			delete(c.RunningReduceTasks, task.TaskId)
		}
		return
	case <-cancelCh:
		if !timer.Stop() {
			<-timer.C
		}
		return
	}
}
