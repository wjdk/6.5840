package mr

import "os"
import "strconv"
//
// RPC definitions.
//

type JobType int

const (
    MapJob JobType = iota
    ReduceJob
    WaitJob
    CompleteJob
)

type TaskStatus int

const (
    Idle TaskStatus = iota
    InProgress
    Completed
)

type SchedulePhase int

const (
    MapPhase SchedulePhase = iota
    ReducePhase
    CompletePhase
)

// Heartbeat RPC
type HeartbeatRequest struct {
    // 空请求，worker只需要说"我准备好了"
}

type HeartbeatResponse struct {
    JobType  JobType
    FileName string
    Id       int
    NReduce  int
    NMap     int
}

// Report RPC  
type ReportRequest struct {
    Id     int
    Status TaskStatus
}

type ReportResponse struct {
    Accepted bool
}

// Cook up a unique-ish UNIX-domain socket name
func coordinatorSock() string {
    s := "/var/tmp/5840-mr-"
    s += strconv.Itoa(os.Getuid())
    return s
}