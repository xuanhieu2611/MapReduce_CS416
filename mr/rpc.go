package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskRequestArgs struct {
    // You can add worker ID or other info if needed
    WorkerID string
}

type TaskReply struct {
    TaskType string // "map", "reduce", "wait", "done"
    FileName string // For map tasks
    TaskNum  int    // Task number (for reduce, etc.)
    NReduce  int    // Number of reduce tasks
}

type TaskDoneArgs struct {
    TaskType string // "map" or "reduce"
    TaskNum  int
}

type TaskDoneReply struct{}

type HeartbeatArgs struct {
    WorkerID string
}

type HeartbeatReply struct {
    Status string // "alive", "shutdown"
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/416-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
