package mr

import (
	"fmt"
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
    WorkerID    string  // Track which worker is processing this task
}


type Coordinator struct {
	// Your definitions here.

	files    []string
    nReduce  int
    mapTasks []TaskInfo
    reduceTasks []TaskInfo
    phase    string // "map" or "reduce"
    mu sync.Mutex
    
    // Heartbeat tracking
    workerLastSeen map[string]time.Time  // workerID -> last heartbeat time
    shutdown       bool                  // coordinator shutdown flag
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
                task.WorkerID = args.WorkerID
                return nil
            case InProgress:
                if now.Sub(task.StartTime) > 10*time.Second {
                    // Timeout: reassign
                    reply.TaskType = "map"
                    reply.FileName = c.files[i]
                    reply.TaskNum = i
                    reply.NReduce = c.nReduce
                    task.StartTime = now
                    task.WorkerID = args.WorkerID
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
                task.WorkerID = args.WorkerID
                return nil
            case InProgress:
                if now.Sub(task.StartTime) > 10*time.Second {
                    // Timeout: reassign
                    reply.TaskType = "reduce"
                    reply.TaskNum = r
                    reply.NReduce = len(c.mapTasks)
                    task.StartTime = now
                    task.WorkerID = args.WorkerID
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

// Heartbeat RPC handler
func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // Update worker's last seen time
    c.workerLastSeen[args.WorkerID] = time.Now()
    
    // Check if coordinator is shutting down
    if c.shutdown {
        reply.Status = "shutdown"
    } else {
        reply.Status = "alive"
    }
    
    return nil
}

// Health monitoring function
func (c *Coordinator) checkWorkerHealth() {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    now := time.Now()
    crashedWorkers := make([]string, 0)
    
    // Check for workers that haven't sent heartbeat in 3 seconds
    for workerID, lastSeen := range c.workerLastSeen {
        if now.Sub(lastSeen) > 3*time.Second {
            crashedWorkers = append(crashedWorkers, workerID)
        }
    }
    
    // Clean up crashed workers
    for _, workerID := range crashedWorkers {
        delete(c.workerLastSeen, workerID)
        c.cleanupCrashedWorker(workerID)
    }
}

// Clean up tasks assigned to crashed workers
func (c *Coordinator) cleanupCrashedWorker(workerID string) {
    // Reset map tasks assigned to this worker
    for i := range c.mapTasks {
        if c.mapTasks[i].WorkerID == workerID && c.mapTasks[i].Status == InProgress {
            c.mapTasks[i].Status = Idle
            c.mapTasks[i].WorkerID = ""
            c.cleanupPartialFiles("map", i)
        }
    }
    
    // Reset reduce tasks assigned to this worker
    for i := range c.reduceTasks {
        if c.reduceTasks[i].WorkerID == workerID && c.reduceTasks[i].Status == InProgress {
            c.reduceTasks[i].Status = Idle
            c.reduceTasks[i].WorkerID = ""
            c.cleanupPartialFiles("reduce", i)
        }
    }
}

// Clean up partial files from crashed workers
func (c *Coordinator) cleanupPartialFiles(taskType string, taskNum int) {
    if taskType == "map" {
        // Remove partial intermediate files
        for r := 0; r < c.nReduce; r++ {
            filename := fmt.Sprintf("mr-%d-%d", taskNum, r)
            os.Remove(filename)
        }
    } else if taskType == "reduce" {
        // Remove partial output file
        filename := fmt.Sprintf("mr-out-%d", taskNum)
        os.Remove(filename)
    }
}

// Start health monitoring goroutine
func (c *Coordinator) startHealthMonitor() {
    go func() {
        ticker := time.NewTicker(2 * time.Second)
        for range ticker.C {
            c.checkWorkerHealth()
        }
    }()
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
        workerLastSeen: make(map[string]time.Time),
        shutdown:   false,
	}

	// Your code here.

	c.server()
	c.startHealthMonitor()
	return &c
}
