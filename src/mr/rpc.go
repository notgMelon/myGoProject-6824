package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"strconv"
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
	if len(c.RunningMapTasks) > 0 || len(c.LeftMapTasks) > 0 {
		if len(c.LeftMapTasks) > 0 {
			reply.NReduce = c.nReduce
			reply.FileName = c.LeftMapTasks[0]
			reply.TaskType = "map"
			err := error(nil)
			c.LeftMapTasks, err = deleteFile(c.LeftMapTasks, reply.FileName)
			if err != nil {
				log.Fatalf("Error deleting file %s from LeftMapTasks: %v", reply.FileName, err)
			}
			c.RunningMapTasks = append(c.RunningMapTasks, reply.FileName)
			reply.TaskId = len(c.RunningMapTasks) - 1 + c.FinishedMapTasks
		} else {
			reply.TaskType = "wait"
			// 	TODO:run backup for running map tasks
		}
	} else {
		// fmt.Printf("%d reduce tasks left, %d running\n", len(c.LeftReduceTasks), len(c.RunningReduceTasks))
		if len(c.LeftReduceTasks) > 0 {
			reply.NReduce = c.nReduce
			reply.TaskId = c.LeftReduceTasks[0]
			c.LeftReduceTasks = c.LeftReduceTasks[1:]
			c.RunningReduceTasks = append(c.RunningReduceTasks, reply.TaskId)
			reply.TaskType = "reduce"
			// fmt.Printf("Assigning reduce task %d\n", reply.TaskId)
		} else {
			if len(c.RunningReduceTasks) > 0 && len(c.LeftReduceTasks) == 0 {
				reply.TaskType = "wait"
			}
			reply.TaskType = "done"
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
		err := error(nil)
		c.RunningMapTasks, err = deleteFile(c.RunningMapTasks, args.FileName)
		if err != nil {
			return err
		}
		c.FinishedMapTasks++
	case "reduce":
		err := error(nil)
		c.RunningReduceTasks, err = deleteFileInInts(c.RunningReduceTasks, args.TaskId)
		if err != nil {
			return err
		}
		c.FinishedReduceTasks++
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

// find and delete a file from a slice of strings
func deleteFile(files []string, file string) ([]string, error) {
	idx := -1
	for i, f := range files {
		if f == file {
			idx = i
			break
		}
	}

	if idx != -1 {
		files = append(files[:idx], files[idx+1:]...)
	} else {
		fmt.Printf("Warning: file %s not found in slice\n", file)
		return files, fmt.Errorf("file %s not found in slice", file)
	}

	return files, nil
}

// find and delete a file from a slice of ints
func deleteFileInInts(files []int, file int) ([]int, error) {
	idx := -1
	for i, f := range files {
		if f == file {
			idx = i
			break
		}
	}

	if idx != -1 {
		files = append(files[:idx], files[idx+1:]...)
	} else {
		fmt.Printf("Warning: file %d not found in slice\n", file)
		return files, fmt.Errorf("file %d not found in slice", file)
	}

	return files, nil
}
