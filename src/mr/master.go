package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	// 要维护很多状态, 例如任务阶段， worker的运行状态，心跳检测。
	Files        []string // 原始输入的文件列表
	ReduceNumber int      //  Reduce 的数量   Map任务的数量就是 files 的长度

	MapJobList    []*Job     // 分发给 worker 的 map 任务
	ReduceJobList []*Job     // 分发给 worker 的 reduce 任务
	JobListMutex  sync.Mutex // 访问or修改任务状态时要加锁   todo 可以减少锁的粒度

	CurrentStates int32 // 所处的阶段， map、reduce、都完成

	// 全局锁
	GlobalMutex sync.Mutex
}

// Job 任务，当初始化的时候，生成对应的 M 任务
type Job struct {
	FileName     string // map任务输入文件名
	ListIndex    int    // 在任务列表中的 index
	ReduceID     int    // reduce 任务号  从0-N  和上面一个属性重复了
	ReduceNumber int    // reduce 数量个数，分区用到
	JobFinished  bool   // 任务是否正确完成
	JobType      int    // 任务类型  map 还是 reduce
	StartTime    int64  // 运行的开始时间[初始为0]，如果超过2s，就当失败处理，重新分配
	FetchCount   int    // 任务分配次数，统计信息
	// 规定： map产生的中间文件的名字格式是  输入文件名_分区号    reduce 产生的最终文件是 mr-out-reduce任务号
}

const (
	// InMap 所处的阶段
	InMap       = 1
	InReduce    = 2
	AllFinished = 3
	// MapJob 任务类型
	MapJob    = 1
	ReduceJob = 2
	// JobTimeoutSecond 任务的超时时间，超过 2s 当做客户端crash了，重新分配任务
	JobTimeoutSecond = 2
	// BackgroundInterval 后台运行间隔
	BackgroundInterval = 500 * time.Millisecond
	MasterLogPrefix    = "master_log: "
	WorkerLogPrefix    = "worker_log: "
)

// DoJob 开始工作
func (job *Job) DoJob(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var err error
	switch job.JobType {
	case MapJob:
		if err = job.DoMapJob(mapf); err != nil {
			fmt.Println("DoMapJob_error ", err)
		}
	case ReduceJob:
		if err = job.DoReduceJob(reducef); err != nil {
			fmt.Println("DoReduceJob_error ", err)
		}
	}
	// 成功做完修改状态
	if err == nil {
		job.JobFinished = true
	}
	fmt.Printf(WorkerLogPrefix+"DoMapJob_finished %v\n ", toJsonString(job))
}

func (job *Job) DoReduceJob(reducef func(string, []string) string) error {
	resFile, err := os.OpenFile("mr-out-"+fmt.Sprint(job.ReduceID), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer resFile.Close()
	keyValueList2 := make([][]*KeyValue, 0)
	// 先遍历一下目录内的文件， 找出 _reduceID 结尾的
	dir, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}
	kvCount := 0
	for _, file := range dir {
		// 找出 .txt_reduceID 结尾的, 读取文件并解析，加入结果集
		if strings.HasSuffix(file.Name(), fmt.Sprint(".txt_", job.ReduceID)) {
			content, err := job.ReadAndParseFile(file.Name())
			if err != nil {
				return err
			}
			keyValueList2 = append(keyValueList2, content)
			kvCount += len(content)
		}
	}
	sortedList := make([]*KeyValue, 0, 1000)
	// 对keyValueList2进行reduceNumber路归并排序, 内维已经排好
	indexList := make([]int, len(keyValueList2))
	for minI := findMinIndex(keyValueList2, indexList); minI != -1; minI = findMinIndex(keyValueList2, indexList) {
		sortedList = append(sortedList, keyValueList2[minI][indexList[minI]])
		indexList[minI]++
	}
	// 一维聚合, cv 样例中的代码
	i := 0
	for i < len(sortedList) {
		j := i + 1
		for j < len(sortedList) && sortedList[j].Key == sortedList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, sortedList[k].Value)
		}
		output := reducef(sortedList[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(resFile, "%v %v\n", sortedList[i].Key, output)
		i = j
	}
	fmt.Printf(WorkerLogPrefix+"DoReduceJob_finished %v\n", toJsonString(job))
	return nil
}

// 多路归并， 找最小值
func findMinIndex(keyValueList2 [][]*KeyValue, indexList []int) int {
	// z 比 Z 的 ascii 码大
	tempStr := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	minI := -1
	for i, j := range indexList {
		if j < len(keyValueList2[i]) && keyValueList2[i][j].Key < tempStr {
			minI = i
			tempStr = keyValueList2[i][j].Key
		}
	}
	return minI
}

func (job *Job) DoMapJob(mapf func(string, string) []KeyValue) error {
	str, err := job.ReadFile(job.FileName)
	if err != nil {
		return err
	}
	keyValueList := mapf(job.FileName, str)
	// 排序
	sort.Sort(ByKey(keyValueList))
	// 开ReduceNumber个临时文件， 复写
	fileList := make([]*os.File, job.ReduceNumber)
	// 每个文件都 open 前清除一下写模式
	for i := range fileList {
		// 原始的 fimeName 为 ../pg.txt
		temp := strings.Split(job.FileName, "/")
		fileList[i], err = os.OpenFile(temp[1]+"_"+fmt.Sprint(i), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		defer fileList[i].Close()
	}
	// 遍历每个 kv 对，根据 hash 值写到对应的分区文件里面去
	for _, kv := range keyValueList {
		fileList[ihash(kv.Key)%job.ReduceNumber].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
	}
	return nil
}

func (job *Job) ReadFile(filename string) (string, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", err
	}
	return string(content), nil
}

func (job *Job) ReadAndParseFile(filename string) ([]*KeyValue, error) {
	content, err := job.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	str := content
	lineList := strings.Split(strings.TrimSpace(str), "\n")
	res := make([]*KeyValue, 0, len(lineList))
	// line 格式    apple 2 3
	for _, line := range lineList {
		separatorIndex := strings.Index(line, " ")
		if separatorIndex == -1 {
			// 为什么会出现空行呢
			continue
		}
		res = append(res, &KeyValue{Key: line[0:separatorIndex], Value: line[separatorIndex+1:]})
	}
	return res, nil
}

// JobFetchReq 任务获取的请求体  简单点服务端不需要知道客户端的信息
type JobFetchReq struct {
	// 不需要有东西
}

type JobFetchResp struct {
	NeedWait bool // 需要等待下次轮询任务， 具体等的是 map 阶段到 reduce 阶段的过渡
	Job
}

// JobDoneReq 任务完成提交的请求体
type JobDoneReq struct {
	Job
}

// JobDoneResp 任务完成提交的返回体
type JobDoneResp struct {
	// 不需要有东西
}

// InitMaster 初始化函数
func (m *Master) InitMaster(files []string, nReduce int) {
	m.Files = files
	m.ReduceNumber = nReduce
	m.CurrentStates = InMap
	m.initMapJob()
}

// 初始化 Map 任务
func (m *Master) initMapJob() {
	m.MapJobList = make([]*Job, 0, len(m.Files))
	for i, file := range m.Files {
		m.MapJobList = append(m.MapJobList, &Job{
			JobType:      MapJob,
			FileName:     file,
			ListIndex:    i,
			ReduceNumber: m.ReduceNumber,
		})
	}
	fmt.Printf(MasterLogPrefix+"master init finished %+v\n", toJsonString(m))
}

func toJsonString(inter interface{}) string {
	bytes, _ := json.Marshal(inter)
	return string(bytes)
}

func (m *Master) JobFetch(req *JobFetchReq, resp *JobFetchResp) error {
	m.JobListMutex.Lock()
	defer m.JobListMutex.Unlock()
	curTime := time.Now().Unix()
	// 看看有哪些没完成的任务，分配出去
	jobList := m.MapJobList
	switch atomic.LoadInt32(&m.CurrentStates) {
	case AllFinished:
		return nil
	case InMap:
		jobList = m.MapJobList
	case InReduce:
		jobList = m.ReduceJobList
	}
	for _, job := range jobList {
		// 任务没完成，且是第一次运行或者之前超时了
		if !job.JobFinished && (job.StartTime == 0 || curTime-job.StartTime > int64(JobTimeoutSecond)) {
			job.FetchCount++
			job.StartTime = curTime
			resp.Job = *job
			return nil
		}
	}
	// 需要等待流转到 reduce
	if atomic.LoadInt32(&m.CurrentStates) == InMap {
		resp.NeedWait = true
	}
	return nil
}

func (m *Master) JobDone(req *JobDoneReq, resp *JobDoneResp) error {
	m.JobListMutex.Lock()
	defer m.JobListMutex.Unlock()
	finished := req.JobFinished
	switch atomic.LoadInt32(&m.CurrentStates) {
	case InMap:
		m.MapJobList[req.ListIndex].JobFinished = finished
	case InReduce:
		m.ReduceJobList[req.ListIndex].JobFinished = finished
	}
	return nil
}

// Background 后台扫描，任务是否都完成，是否有卡死的任务，是否进入下一个阶段，每隔100毫秒扫描一次
func (m *Master) Background() {
	for atomic.LoadInt32(&m.CurrentStates) != AllFinished {
		// 循环遍历任务
		m.JobListMutex.Lock()
		isAllJobDone := true
		leftCount := 0
		switch atomic.LoadInt32(&m.CurrentStates) {
		case InMap:
			for _, job := range m.MapJobList {
				if !job.JobFinished {
					isAllJobDone = false
					leftCount++
				}
			}
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 还剩 %v 个 map 任务\n", leftCount)
			// map 任务都做完了，流转到 reduce 状态, 而且要生成 reduce 任务
			if isAllJobDone {
				leftCount = 0
				atomic.StoreInt32(&m.CurrentStates, InReduce)
				m.generateReduceMap()
				fmt.Printf(MasterLogPrefix+"background: CurrentStates change from %v to %v\n", InMap, InReduce)
			}
		case InReduce:
			for _, job := range m.ReduceJobList {
				if !job.JobFinished {
					isAllJobDone = false
					leftCount++
				}
			}
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 还剩 %v 个 reduce 任务\n", leftCount)
			// reduce 任务都做完了，流转到 结束 状态
			if isAllJobDone {
				atomic.StoreInt32(&m.CurrentStates, AllFinished)
				fmt.Printf(MasterLogPrefix+"background: CurrentStates change from %v to %v\n", InReduce, AllFinished)
			}
		}
		m.JobListMutex.Unlock()
		time.Sleep(BackgroundInterval)
	}
}

// 调用前需要持有锁
func (m *Master) generateReduceMap() {
	reduceJobList := make([]*Job, 0, m.ReduceNumber)
	for i := 0; i < m.ReduceNumber; i++ {
		reduceJobList = append(reduceJobList, &Job{
			ListIndex:    i,
			ReduceID:     i,
			ReduceNumber: m.ReduceNumber,
			JobType:      ReduceJob,
		})
	}
	m.ReduceJobList = reduceJobList
	fmt.Printf(MasterLogPrefix+" generateReduceMap finished,m.ReduceJobList: %+v\n", toJsonString(reduceJobList))
}

func logTime() string {
	return fmt.Sprint((time.Now().UnixNano()/1e6)%10000, " ")
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	for atomic.LoadInt32(&m.CurrentStates) != AllFinished {
		time.Sleep(time.Second)
	}
	ret = true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.InitMaster(files, nReduce)
	go m.Background()
	m.server()
	return &m
}
