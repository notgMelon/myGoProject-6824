package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTask struct {
	FileName string
	TaskId   int
	done     bool
}

type ReduceTask struct {
	TaskId int
	done   bool
}

type Coordinator struct {
	// Your definitions here.
	mu                  sync.Mutex
	nReduce             int
	FinishedMapTasks    int
	FinishedReduceTasks int

	RunningMapTasks    map[int]chan struct{}
	MapTasks           []MapTask
	RunningReduceTasks map[int]chan struct{}
	ReduceTasks        []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	if c.FinishedMapTasks == len(c.MapTasks) && c.FinishedReduceTasks == len(c.ReduceTasks) {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for i, file := range files {
		c.MapTasks = append(c.MapTasks, MapTask{FileName: file, TaskId: i})
	}
	c.nReduce = nReduce
	c.RunningMapTasks = make(map[int]chan struct{})
	c.ReduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = ReduceTask{TaskId: i}
	}
	// fmt.Printf("Created %d map tasks\n", len(c.LeftMapTasks))
	// fmt.Printf("Created %d reduce tasks\n", len(c.LeftReduceTasks))
	c.RunningReduceTasks = make(map[int]chan struct{})
	c.server()
	return &c
}
