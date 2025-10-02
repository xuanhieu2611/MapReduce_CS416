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

type TaskStatus int
const (
    Idle TaskStatus = iota
    InProgress
    Completed
)

type TaskInfo struct {
    Status      TaskStatus
    StartTime   time.Time
}


type Coordinator struct {
	// Your definitions here.

	files    []string
    nReduce  int
    mapTasks []TaskInfo
    reduceTasks []TaskInfo
    phase    string // "map" or "reduce"
    mu sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    now := time.Now()
    if c.phase == "map" {
        allDone := true
        for i := range c.mapTasks {
            task := &c.mapTasks[i]
            switch task.Status {
            case Idle:
                reply.TaskType = "map"
                reply.FileName = c.files[i]
                reply.TaskNum = i
                reply.NReduce = c.nReduce
                task.Status = InProgress
                task.StartTime = now
                return nil
            case InProgress:
                if now.Sub(task.StartTime) > 10*time.Second {
                    // Timeout: reassign
                    reply.TaskType = "map"
                    reply.FileName = c.files[i]
                    reply.TaskNum = i
                    reply.NReduce = c.nReduce
                    task.StartTime = now
                    return nil
                }
                allDone = false
            case Completed:
                // nothing
            }
        }
        // If all map tasks done, switch to reduce phase
        for _, task := range c.mapTasks {
            if task.Status != Completed {
                allDone = false
                break
            }
        }
        if allDone {
            c.phase = "reduce"
        }
        reply.TaskType = "wait"
        return nil
    }
    if c.phase == "reduce" {
        allDone := true
        for r := range c.reduceTasks {
            task := &c.reduceTasks[r]
            switch task.Status {
            case Idle:
                reply.TaskType = "reduce"
                reply.TaskNum = r
                reply.NReduce = len(c.mapTasks)
                task.Status = InProgress
                task.StartTime = now
                return nil
            case InProgress:
                if now.Sub(task.StartTime) > 10*time.Second {
                    // Timeout: reassign
                    reply.TaskType = "reduce"
                    reply.TaskNum = r
                    reply.NReduce = len(c.mapTasks)
                    task.StartTime = now
                    return nil
                }
                allDone = false
            case Completed:
                // nothing
            }
        }
        for _, task := range c.reduceTasks {
            if task.Status != Completed {
                allDone = false
                break
            }
        }
        if allDone {
            reply.TaskType = "done"
            return nil
        }
        reply.TaskType = "wait"
        return nil
    }
    reply.TaskType = "done"
    return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    if args.TaskType == "map" {
        c.mapTasks[args.TaskNum].Status = Completed
    }
    if args.TaskType == "reduce" {
        c.reduceTasks[args.TaskNum].Status = Completed
    }
    return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
    c.mu.Lock()
    defer c.mu.Unlock()
	ret := true

	// Your code here.
	if c.phase != "reduce" {
        return false
    }
    for _, task := range c.reduceTasks {
        if task.Status != Completed {
            return false
        }
    }

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
        nReduce:    nReduce,
        mapTasks:   make([]TaskInfo, len(files)),
        reduceTasks: make([]TaskInfo, nReduce),
        phase:      "map",
	}

	// Your code here.


	c.server()
	return &c
}
