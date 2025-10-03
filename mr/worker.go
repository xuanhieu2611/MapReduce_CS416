package mr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Generate a unique worker ID using UUID
func generateWorkerID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Generate unique worker ID
	workerID := generateWorkerID()
	
	// Start heartbeat goroutine
	heartbeatChan := make(chan bool, 1)
	go sendHeartbeats(workerID, heartbeatChan)
	
	// Cleanup function
	defer func() {
		heartbeatChan <- true // Signal to stop heartbeat
	}()

	for {
        args := TaskRequestArgs{WorkerID: workerID}
        reply := TaskReply{}
        ok := call("Coordinator.AssignTask", &args, &reply)
        if !ok || reply.TaskType == "done" {
            break
        }
		
        if reply.TaskType == "wait" {
            time.Sleep(time.Second)
            continue
        }

        if reply.TaskType == "map" {
            // Read file and run mapf
            filename := reply.FileName
			// fmt.Print("Worker processing file: ", filename, "\n")
			content, err := os.ReadFile(filename)
            if err != nil {
                log.Fatalf("cannot read %v", filename)
            }
            kva := mapf(filename, string(content))

			// Partition kva by reduce task
            nReduce := reply.NReduce
            intermediate := make([][]KeyValue, nReduce)
            for _, kv := range kva {
                r := ihash(kv.Key) % nReduce
                intermediate[r] = append(intermediate[r], kv)
            }

            // Write each partition to a file
            for r := 0; r < nReduce; r++ {
                oname := fmt.Sprintf("mr-%d-%d", reply.TaskNum, r)
                tmpfile, err := os.CreateTemp("", "mr-tmp-*")
                if err != nil {
                    log.Fatalf("cannot create temp file for %v", oname)
                }
                enc := json.NewEncoder(tmpfile)
                for _, kv := range intermediate[r] {
                    if err := enc.Encode(&kv); err != nil {
                        log.Fatalf("cannot encode kv to temp file for %v", oname)
                    }
                }
                tmpfile.Close()
                if err := os.Rename(tmpfile.Name(), oname); err != nil {
                    log.Fatalf("cannot rename temp file to %v", oname)
                }
			}

            // Notify coordinator that map task is done
			doneArgs := TaskDoneArgs{
				TaskType: "map",
				TaskNum:  reply.TaskNum,
			}
			doneReply := TaskDoneReply{}
			call("Coordinator.TaskDone", &doneArgs, &doneReply)
        }
        
		if reply.TaskType == "reduce" {
			reduceNum := reply.TaskNum
			nMap := reply.NReduce // Actually, number of map tasks
			var kva []KeyValue
			// Read all intermediate files for this reduce partition
			for m := 0; m < nMap; m++ {
				iname := fmt.Sprintf("mr-%d-%d", m, reduceNum)
				ifile, err := os.Open(iname)
				if err != nil {
					continue // skip missing files
				}
				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				ifile.Close()
			}

			// Sort by key
			sort.Sort(ByKey(kva))
			
			// Group by key and apply reducef
			oname := fmt.Sprintf("mr-out-%d", reduceNum)
            tmpfile, err := os.CreateTemp("", "mr-out-tmp-*")
            if err != nil {
                log.Fatalf("cannot create temp file for %v", oname)
            }
            i := 0
            for i < len(kva) {
                j := i + 1
                for j < len(kva) && kva[j].Key == kva[i].Key {
                    j++
                }
                values := []string{}
                for k := i; k < j; k++ {
                    values = append(values, kva[k].Value)
                }
                output := reducef(kva[i].Key, values)
                fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
                i = j
            }
            tmpfile.Close()
            if err := os.Rename(tmpfile.Name(), oname); err != nil {
                log.Fatalf("cannot rename temp file to %v", oname)
            }
			// Notify coordinator
			doneArgs := TaskDoneArgs{
				TaskType: "reduce",
				TaskNum:  reduceNum,
			}
			doneReply := TaskDoneReply{}
			call("Coordinator.TaskDone", &doneArgs, &doneReply)
		}
    }

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

// Send periodic heartbeats to coordinator
func sendHeartbeats(workerID string, stopChan chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			args := HeartbeatArgs{WorkerID: workerID}
			reply := HeartbeatReply{}
			
			ok := call("Coordinator.Heartbeat", &args, &reply)
			if !ok {
				// Coordinator unreachable, exit immediately
				os.Exit(1)
			}
			
			if reply.Status == "shutdown" {
				// Coordinator is shutting down, exit gracefully
				os.Exit(0)
			}
		}
	}
}
