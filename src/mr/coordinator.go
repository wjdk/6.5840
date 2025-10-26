package mr

import (
    "log"
    "net"
    "os"
    "net/rpc"
    "net/http"
    "time"
)

type Task struct {
    fileName  string
    id        int
    startTime time.Time
    status    TaskStatus
    taskType  JobType
}

type Coordinator struct {
    files   []string
    nReduce int
    nMap    int
    phase   SchedulePhase
    tasks   []Task

    heartbeatCh chan heartbeatMsg
    reportCh    chan reportMsg
	doneCh chan bool
}

type heartbeatMsg struct {
    response *HeartbeatResponse
    ok       chan struct{}
}

type reportMsg struct {
    request *ReportRequest
    ok      chan struct{}
}

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
    msg := heartbeatMsg{response, make(chan struct{})}
    c.heartbeatCh <- msg
    <-msg.ok
    return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
    msg := reportMsg{request, make(chan struct{})}
    c.reportCh <- msg
    <-msg.ok
    return nil
}

func (c *Coordinator) schedule() {
    c.initMapPhase()
    
    for {
        select {
        case msg := <-c.heartbeatCh:
            c.handleHeartbeat(msg.response)
            msg.ok <- struct{}{}
        case msg := <-c.reportCh:
            c.handleReport(msg.request)
            msg.ok <- struct{}{}
        }
    }
}

func (c *Coordinator) handleHeartbeat(response *HeartbeatResponse) {
    response.NReduce = c.nReduce
    response.NMap = c.nMap

    switch c.phase {
    case MapPhase:
        if task := c.findIdleTask(MapJob); task != nil {
            response.JobType = MapJob
            response.FileName = task.fileName
            response.Id = task.id
            task.status = InProgress
            task.startTime = time.Now()
        } else if c.allTasksCompleted(MapJob) {
            c.initReducePhase()
            response.JobType = WaitJob
        } else {
            response.JobType = WaitJob
        }
        
    case ReducePhase:
        if task := c.findIdleTask(ReduceJob); task != nil {
            response.JobType = ReduceJob
            response.Id = task.id
            task.status = InProgress
            task.startTime = time.Now()
        } else if c.allTasksCompleted(ReduceJob) {
            c.phase = CompletePhase
			c.doneCh <- true
            response.JobType = CompleteJob
        } else {
            response.JobType = WaitJob
        }
        
    case CompletePhase:
        response.JobType = CompleteJob
    }
}

func (c *Coordinator) handleReport(request *ReportRequest) {
    if request.Id < len(c.tasks) {
        task := &c.tasks[request.Id]
        if request.Status == Completed {
            task.status = Completed
        }
    } else {
        log.Printf("Coordinator: invalid task ID %d in report", request.Id)
    }
}

func (c *Coordinator) initMapPhase() {
    c.phase = MapPhase
    c.nMap = len(c.files)
    c.tasks = make([]Task, c.nMap)
    
    for i, file := range c.files {
        c.tasks[i] = Task{
            fileName: file,
            id:       i,
            status:   Idle,
            taskType: MapJob,
        }
    }
}

func (c *Coordinator) initReducePhase() {
    c.phase = ReducePhase
    c.tasks = make([]Task, c.nReduce)
    
    for i := 0; i < c.nReduce; i++ {
        c.tasks[i] = Task{
            id:       i,
            status:   Idle,
            taskType: ReduceJob,
        }
    }
}

func (c *Coordinator) findIdleTask(taskType JobType) *Task {
    now := time.Now()
    for i := range c.tasks {
        task := &c.tasks[i]
        if task.taskType == taskType {
            if task.status == Idle {
                return task
            }
            if task.status == InProgress && now.Sub(task.startTime) > 10*time.Second {
                task.status = Idle
                return task
            }
        }
    }
    return nil
}

func (c *Coordinator) allTasksCompleted(taskType JobType) bool {
    for i := range c.tasks {
        if c.tasks[i].taskType == taskType && c.tasks[i].status != Completed {
            return false
        }
    }
    return true
}

func (c *Coordinator) Done() bool {
	select{
		case <-c.doneCh:
			return true
		default:
			return false
	}
}

func (c *Coordinator) server() {
    rpc.Register(c)
    rpc.HandleHTTP()
    sockname := coordinatorSock()
    os.Remove(sockname)
    l, e := net.Listen("unix", sockname)
    if e != nil {
        log.Fatal("listen error:", e)
    }
    go http.Serve(l, nil)
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        files:       files,
        nReduce:     nReduce,
        heartbeatCh: make(chan heartbeatMsg),
        reportCh:    make(chan reportMsg),
		doneCh:      make(chan bool),
    }
    go c.schedule()
    c.server()
    return &c
}