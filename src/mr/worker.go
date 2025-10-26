package mr

import (
    "encoding/json"
    "fmt"
    "hash/fnv"
    "io/ioutil"
    "log"
    "net/rpc"
    "os"
    "sort"
    "time"
)

type KeyValue struct {
	Key   string
	Value string
}

func doHeartbeat() *HeartbeatResponse {
    args := HeartbeatRequest{}
    reply := HeartbeatResponse{}
    
    ok := call("Coordinator.Heartbeat", &args, &reply)
    if !ok {
        return &HeartbeatResponse{JobType: CompleteJob}
    }
    return &reply
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
    content, err := ioutil.ReadFile(response.FileName)
    if err != nil {
        log.Printf("Worker: failed to read file %s: %v", response.FileName, err)
        return
    }

    kvs := mapf(response.FileName, string(content))
    
    buckets := make([][]KeyValue, response.NReduce)
    for _, kv := range kvs {
        reduceID := ihash(kv.Key) % response.NReduce
        buckets[reduceID] = append(buckets[reduceID], kv)
    }
    
    for reduceID, kvs := range buckets {
        filename := fmt.Sprintf("mr-%d-%d", response.Id, reduceID)
        err := atomicWriteKVs(filename, kvs)
        if err != nil {
            log.Printf("Worker: failed to write intermediate file %s: %v", filename, err)
			return 
        }
    }
    
    reportTaskCompletion(response.Id)
}

func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
    reduceID := response.Id
    intermediate := []KeyValue{}
    
    for mapID := 0; mapID < response.NMap; mapID++ {
        filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
        
        file, err := os.Open(filename)
        if err != nil {
            log.Printf("Worker: cannot open intermediate file %s: %v", filename, err)
            continue
        }
        
        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break
            }
            intermediate = append(intermediate, kv)
        }
        file.Close()
    }
    
    sort.Sort(ByKey(intermediate))
    
	// 使用原子写入输出文件
    outputFile := fmt.Sprintf("mr-out-%d", reduceID)
    tempFile, err := ioutil.TempFile(".", fmt.Sprintf("mr-out-temp-%d-*", time.Now().UnixNano()))
    if err != nil {
        log.Printf("Worker: failed to create temp output file: %v", err)
        return
    }
    i := 0
    for i < len(intermediate) {
        j := i + 1
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
            j++
        }
        
        values := []string{}
        for k := i; k < j; k++ {
            values = append(values, intermediate[k].Value)
        }
        
        output := reducef(intermediate[i].Key, values)
        fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
        
        i = j
    }
    tempFile.Close()
    if err := os.Rename(tempFile.Name(), outputFile); err != nil {
        log.Printf("Worker: failed to rename output file: %v", err)
        os.Remove(tempFile.Name())
        return
    }
    reportTaskCompletion(response.Id)
}

func atomicWriteKVs(filename string, kvs []KeyValue) error {
    tempFile, err := ioutil.TempFile(".", "mr-temp-*")
    if err != nil {
        return err
    }
    
    enc := json.NewEncoder(tempFile)
    for _, kv := range kvs {
        if err := enc.Encode(&kv); err != nil {
            tempFile.Close()
            os.Remove(tempFile.Name())
            return err
        }
    }
    
    if err := tempFile.Close(); err != nil {
        os.Remove(tempFile.Name())
        return err
    }
    
    return os.Rename(tempFile.Name(), filename)
}

func reportTaskCompletion(taskID int) {
    args := ReportRequest{Id: taskID, Status: Completed}
    reply := ReportResponse{}
    call("Coordinator.Report", &args, &reply)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
    for {
        response := doHeartbeat()
        
        switch response.JobType {
        case MapJob:
            doMapTask(mapf, response)
        case ReduceJob:
            doReduceTask(reducef, response)
        case WaitJob:
            time.Sleep(100 * time.Millisecond)
        case CompleteJob:
            return
        }
    }
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
    sockname := coordinatorSock()
    c, err := rpc.DialHTTP("unix", sockname)
    if err != nil {
        return false
    }
    defer c.Close()

    err = c.Call(rpcname, args, reply)
    if err != nil {
        return false
    }
    return true
}