package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			log.Fatal("call GetTask failed!")
		}
		// fmt.Printf("Worker received task: %v\n", reply)
		timeStr := time.Now().Format("-01-02_15-04-05")
		switch reply.TaskType {
		case "map":
			func() {

				fileContent, err := os.ReadFile(reply.FileName)
				if err != nil {
					log.Fatalf("cannot open %v", reply.FileName)
				}
				// Process the file content with the map function
				keyValues := mapf(reply.FileName, string(fileContent))
				// create output files for each reduce task
				files := make([]*os.File, reply.NReduce)
				for i := 0; i < reply.NReduce; i++ {
					file, err := os.Create(fmt.Sprintf("mr-%d-%d"+timeStr, reply.TaskId, i))
					if err != nil {
						log.Fatalf("cannot create output file for reduce task %d", i)
					}
					defer file.Close()
					files[i] = file
				}
				for _, kv := range keyValues {
					bucket := ihash(kv.Key) % reply.NReduce
					encoder := json.NewEncoder(files[bucket])
					err := encoder.Encode(kv)
					// fmt.Printf("Map task %d writing key %s to bucket %d\n", reply.TaskId, kv.Key, bucket)
					if err != nil {
						log.Fatalf("cannot write to output file for reduce task %d", bucket)
					}
				}
				// rename files to remove the timestamp suffix
				for i := 0; i < reply.NReduce; i++ {
					oldName := fmt.Sprintf("mr-%d-%d"+timeStr, reply.TaskId, i)
					newName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
					err := os.Rename(oldName, newName)
					if err != nil {
						log.Fatalf("cannot rename file %s to %s", oldName, newName)
					}
				}
				argsFinish := FinishTaskArgs{
					TaskType: "map",
					TaskId:   reply.TaskId,
					FileName: reply.FileName,
				}
				replyFinish := FinishTaskReply{}
				ok = call("Coordinator.FinishTask", &argsFinish, &replyFinish)
				if !ok {
					log.Fatal("call FinishTask failed!")
				}
				// fmt.Printf("Map task %d finished", reply.TaskId)
			}()
		case "reduce":
			func() {
				pattern := regexp.MustCompile(fmt.Sprintf("^mr-(\\d+)-%d$", reply.TaskId))
				tmpfilenames, err := filepath.Glob(fmt.Sprintf("mr-*-%d", reply.TaskId))
				if err != nil {
					log.Fatalf("cannot find intermediate files for reduce task %d", reply.TaskId)
				}
				var filenames []string
				// fmt.Printf("Reduce task %d processing files: %v\n", reply.TaskId, filenames)
				for _, filename := range tmpfilenames {
					if pattern.MatchString(filename) {
						filenames = append(filenames, filename)
					}
				}
				// fmt.Printf("Reduce task %d processing files: %v\n", reply.TaskId, filenames)
				var MapResults []KeyValue
				for _, filename := range filenames {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open intermediate file %v", filename)
					}
					defer file.Close()
					decoder := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err == io.EOF {
							break
						} else if err != nil {
							log.Fatalf("cannot decode intermediate file %v", filename)
						}
						MapResults = append(MapResults, kv)
					}
				}
				sort.Sort(ByKey(MapResults))
				oname := fmt.Sprintf("mr-out-%d"+timeStr, reply.TaskId)
				// fmt.Printf("Reduce task %d writing output to %s\n", reply.TaskId, oname)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatalf("cannot create output file for reduce task %d", reply.TaskId)
				}
				defer ofile.Close()
				i := 0
				for i < len(MapResults) {
					j := i + 1
					for j < len(MapResults) && MapResults[j].Key == MapResults[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, MapResults[k].Value)
					}
					output := reducef(MapResults[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", MapResults[i].Key, output)
					i = j
				}
				// rename the output file to remove the timestamp suffix
				oldName := fmt.Sprintf("mr-out-%d"+timeStr, reply.TaskId)
				newName := fmt.Sprintf("mr-out-%d", reply.TaskId)
				err = os.Rename(oldName, newName)
				if err != nil {
					log.Fatalf("cannot rename file %s to %s", oldName, newName)
				}
				argsFinish := FinishTaskArgs{
					TaskType: "reduce",
					TaskId:   reply.TaskId,
				}
				replyFinish := FinishTaskReply{}
				ok = call("Coordinator.FinishTask", &argsFinish, &replyFinish)
				if !ok {
					log.Fatal("call FinishTask failed!")
				}
			}()
		case "wait":
			// Wait for a while before requesting a new task
			// fmt.Println("Worker waiting for a task...")
			time.Sleep(time.Second)
		case "done":
			// fmt.Println("All tasks are done. Worker exiting.")
			return
		default:
			log.Fatal("unknown task type: ", reply.TaskType)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
