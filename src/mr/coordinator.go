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

type Coordinator struct {
	// Your definitions here.
	mu                  sync.Mutex
	nReduce             int
	FinishedMapTasks    int
	FinishedReduceTasks int
	RunningMapTasks     []string
	LeftMapTasks        []string
	RunningReduceTasks  []int
	LeftReduceTasks     []int
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
	if len(c.LeftMapTasks) == 0 && len(c.RunningMapTasks) == 0 && len(c.LeftReduceTasks) == 0 && len(c.RunningReduceTasks) == 0 {
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
	c.LeftMapTasks = append([]string(nil), files...)
	c.nReduce = nReduce
	c.RunningMapTasks = []string{}
	c.LeftReduceTasks = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.LeftReduceTasks[i] = i
	}
	// fmt.Printf("Created %d map tasks\n", len(c.LeftMapTasks))
	// fmt.Printf("Created %d reduce tasks\n", len(c.LeftReduceTasks))
	c.RunningReduceTasks = []int{}
	c.server()
	return &c
}
