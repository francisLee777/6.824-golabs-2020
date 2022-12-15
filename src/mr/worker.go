package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	startTime := int64(0)
	for true {
		startTime = time.Now().UnixNano()
		// 索要任务，得到的可能是 Map 或者 Reduce 的任务
		job := CallFetchJob()
		// 需要等待流转到 reduce
		if job.NeedWait {
			time.Sleep(BackgroundInterval)
			continue
		}
		// 任务都做完了，停吧
		if job.FetchCount == 0 {
			fmt.Println(logTime() + WorkerLogPrefix + "任务都做完了，worker退出")
			break
		}
		// 做任务
		job.DoJob(mapf, reducef)
		// 做完了，提交
		CallCommitJob(&JobDoneReq{job.Job})
		fmt.Println(WorkerLogPrefix+"一次worker循环耗时[毫秒]:", (time.Now().UnixNano()-startTime)/1e6)
	}
}

func CallFetchJob() JobFetchResp {
	req := JobFetchReq{}
	resp := JobFetchResp{}
	call("Master.JobFetch", &req, &resp)
	fmt.Printf(WorkerLogPrefix+"CallFetchJob job resp %+v\n", resp)
	return resp
}

func CallCommitJob(job *JobDoneReq) {
	fmt.Printf(WorkerLogPrefix+"CallCommitJob job req %+v\n", *job)
	resp := JobDoneResp{}
	call("Master.JobDone", job, &resp)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
