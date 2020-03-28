package mapr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type Master struct {
	address string //
	workers []string
	done 	chan bool // 当前job是否完成
	files   string
	job     Job  // job缓冲队列，无job则阻塞，有job执行
	//jobqueue chan Job
	newAddWorker bool
	sync.Mutex //
	cond *sync.Cond
	l    net.Listener
}

func (mr *Master) GetWorkers() *[]string {
	return &mr.workers
}

func (mr *Master) GetJob() Job {
	return mr.job
}

func (mr *Master) Register(args *RegisterArgs,reply *ReplyMessage) error {
	mr.Lock()
	mr.workers = append(mr.workers,args.WorkAddress)
	debug("Register: worker %s\n", args.WorkAddress)
	mr.newAddWorker = true
	mr.cond.Broadcast()
	mr.Unlock()
	reply.Ok = true
	return nil
}

func (mr *Master) RemoveWorker(workeradd string) {
	mr.Lock()
	i := 0
	for ; i < len(mr.workers); i++{
		if mr.workers[i] == workeradd {
			break
		}
	}
	mr.workers = append(mr.workers[:i],mr.workers[i+1:]...)
	mr.Unlock()
}

func (mr *Master) MonitorSingleWorker(workeraddress string) bool {
	var reply ReplyMessage
	islive := "islive"
	ok := Call(workeraddress,"Worker.Islive",islive,&reply)
	return ok
}

func (mr *Master) MonitorWorkers() {
	for {
		for _,workeradd := range mr.workers {
			ok := mr.MonitorSingleWorker(workeradd)
			if !ok {
				fmt.Println("now is removing %s",workeradd)
				mr.RemoveWorker(workeradd)
				fmt.Println("removed %s",workeradd)
			}
		}
		time.Sleep(time.Second*3)
	}
}

func (mr *Master) Initial(master string)  {
	mr.address = master
	mr.workers = make([]string,0)
	mr.done = make(chan bool)
	//mr.jobqueue = make(chan Job,20)
	mr.cond = sync.NewCond(mr)
	mr.newAddWorker = false
	mr.files = "H:/毕业设计/myMapReduce/files/master"
	return
}

func (mr *Master) StartRPCServer() {
	rpc.Register(mr)
	l,err := net.Listen("tcp",mr.address)
	if err != nil {
		panic("listen error")
	}
	mr.l = l
	go rpc.Accept(mr.l)
	fmt.Println("Master Already startRPCServer")
}

func (mr *Master) AddJob(jb Job,reply *ReplyMessage) error {
	mr.job = jb
	if mr.job.Conf == nil {
		mr.job.Conf = NewDefaultConfig()
	}
	reply.Ok = true
	return nil
}

func (mr *Master) SendJobToWorker() {
	n := len(mr.workers)
	var wg sync.WaitGroup
	wg.Add(n)
	for _,workerAddress := range mr.workers {
		go func(args string) {
			ok := Call(args,"Worker.GetJob",mr.job,nil)
			if !ok {
				fmt.Println("%s is down!",args)
			} else {
				wg.Done()
			}
		}(workerAddress)
	}
	wg.Wait()
}

func GetAllFile(pathName string)  []string {
	tempFiles := make([]string,0)
	rd, err := ioutil.ReadDir(pathName)
	if err != nil {
		panic("readDir file error")
	}
	for _, fi := range rd {
		if fi.IsDir() {
			nextFiles := GetAllFile(pathName + "/" +  fi.Name())
			tempFiles = append(tempFiles, nextFiles...)
		} else {
			tempFiles = append(tempFiles, pathName + "/" + fi.Name())
		}
	}
	return tempFiles
}

func (mr *Master) Split(pathName string) []string {
	fmt.Println("Now is spliting %s",pathName)
	f,err := os.Stat(pathName)
	if err != nil {
		panic("get fileinfo err")
	}
	var allSingleFileName []string
	allSplitFileNames := make([]string,0)
	if f.IsDir() {
		allSingleFileName = GetAllFile(pathName)
	} else {
		allSingleFileName = append(allSingleFileName, pathName)
	}
	CreateDir(DefaultSplitFiles)
	for _,singleFile := range allSingleFileName {
		infile,err := os.Open(singleFile)
		if err != nil {
			panic("open file err")
		}
		defer infile.Close()
		singleSplitedOutFileName := MapName(DefaultSplitFiles,mr.job.Jobname,0)
		singleSplitedOutFile,err := os.Create(singleSplitedOutFileName)
		if err != nil {
			panic("create file err")
		}
		writer := bufio.NewWriter(singleSplitedOutFile)
		mapTaskID := 1
		bytes := 0

		scanner := bufio.NewScanner(infile)
		for scanner.Scan() {
			if bytes >= mr.job.Conf.ChunkSize {
				writer.Flush()
				singleSplitedOutFile.Close()
				allSplitFileNames = append(allSplitFileNames,singleSplitedOutFileName)
				singleSplitedOutFileName = MapName(DefaultSplitFiles,mr.job.Jobname,mapTaskID)
				singleSplitedOutFile,err = os.Create(singleSplitedOutFileName)
				if err != nil {
					panic("create file err")
				}
				writer = bufio.NewWriter(singleSplitedOutFile)
				mapTaskID += 1
				bytes = 0
			}
			line := scanner.Text() + "\n"
			writer.WriteString(line)
			bytes += len(line)
		}
		if bytes != 0 {
			writer.Flush()
			singleSplitedOutFile.Close()
			allSplitFileNames = append(allSplitFileNames,singleSplitedOutFileName)
		}
	}
	return allSplitFileNames
}

func (mr *Master) NotifyWorkers(offJobWorker chan string) {
	mr.Lock()
	for _,workerAdd := range mr.workers {
		offJobWorker <- workerAdd
	}
	mr.newAddWorker = false
	mr.Unlock()
	for {
		mr.Lock()
		if mr.newAddWorker == true {
			newWorkerAdd := mr.workers[len(mr.workers)-1]
			go func() {
				offJobWorker <- newWorkerAdd
			}()
			mr.newAddWorker = false
		} else {
			mr.cond.Wait()
		}
		mr.Unlock()
	}
}

func (mr *Master) ScheduleMap(phase jobPhase, files... string) {
	offJobWorker := make(chan string,3)
	go mr.NotifyWorkers(offJobWorker)

	totaltask := len(files)
	var wg sync.WaitGroup
	wg.Add(totaltask)

	for i := 0; i < totaltask; i++ {
		mapTaskArgs := MapTaskArgs{
			JobName:    mr.job.Jobname,
			File:       files[i],
			NReduce:    mr.job.NReduce,
			TaskNumber: i,
		}
		go func(args MapTaskArgs) {
			ok := false
			var workerAddress string
			for !ok {
				workerAddress = <- offJobWorker
				//debug("ready workerAddress is %s\n",workerAddress)
				ok = Call(workerAddress,"Worker.DoMapTask",args,nil)
			}
			go func() {
				offJobWorker <- workerAddress
			}()
			wg.Done()
		}(mapTaskArgs)
	}
	wg.Wait()
}

func (mr *Master) ScheduleReduce(phase jobPhase) {
	offJobWorker := make(chan string,3)
	go mr.NotifyWorkers(offJobWorker)

	totaltask := mr.job.NReduce
	var wg sync.WaitGroup
	wg.Add(totaltask)

	for i := 0; i < totaltask; i++ {
		reduceTaskArgs := ReduceTaskArgs{
			JobName:      mr.job.Jobname,
			WorkerAddress:  mr.workers,
			NReduce:      mr.job.NReduce,
			ReduceNumber: i,
		}
		go func(args ReduceTaskArgs) {
			ok := false
			var workerAddress string
			for !ok {
				workerAddress = <- offJobWorker
				//debug("ready workerAddress is %s\n",workerAddress)
				ok = Call(workerAddress,"Worker.DoReduceTask",args,nil)
			}
			go func() {
				offJobWorker <- workerAddress
			}()
			wg.Done()
		}(reduceTaskArgs)
	}
	wg.Wait()
}

func (mr *Master) Merge() error {
	CreateDir(mr.files)
	allReduceOutPutFileName := make([]string,0)
	for _,singleWorkerAdd := range mr.workers {
		reduceOutPutFileDir := GetNeedDir(singleWorkerAdd,"reduceoutfiles")
		rd,err := ioutil.ReadDir(reduceOutPutFileDir)
		if err != nil {
			panic("read dir err")
		}
		for _,fi := range rd {
			allReduceOutPutFileName = append(allReduceOutPutFileName,reduceOutPutFileDir + "/" + fi.Name())
		}
	}

 	allKv := KeyValueSlice{}
	for _,reduceFile := range allReduceOutPutFileName {
		//fmt.Println(reduceFile)
		file,err := os.Open(reduceFile)
		defer file.Close()
		if err != nil {
			panic("open file err")
		}
		//debug("reduceFile Name is %s\n",reduceFile)
		decode := json.NewDecoder(file)
		singleFileKv := make([]KeyValue,0)
		tempKv := new(KeyValue)
		for {
			decErr := decode.Decode(tempKv)
			if decErr == io.EOF {
				break
			}
			singleFileKv = append(singleFileKv,*tempKv)
		}
		allKv = append(allKv,singleFileKv...)
	}
	sort.Sort(allKv)

	outFile := ResultName(mr.files,mr.job.Jobname)
	outputfile,err := os.Create(outFile)
	defer outputfile.Close()
	if err != nil {
		panic(err)
	}
	outputJson := json.NewEncoder(outputfile)
	outputJson.Encode(allKv)
	return nil
}

func (mr *Master) Finish() {
	mr.done <- true
}

func (mr *Master) CleanFiles() {
	os.RemoveAll(DefaultSplitFiles)
	for _,singleWorkerAdd := range mr.workers {
		Call(singleWorkerAdd,"Worker.RemoveFiles","nil",nil)
	}
	//os.RemoveAll(mr.files)
}

func (mr *Master) RunJob() {
	mr.SendJobToWorker()
	splitInputFiles := mr.Split(mr.job.Inputfiles)
	mr.ScheduleMap(mapPhase,splitInputFiles...)
	mr.ScheduleReduce(reducePhase)
	mr.Merge()
	mr.CleanFiles()
	time.Sleep(time.Second*1)
	mr.Finish()
}

func (mr *Master) Wait() {
	<- mr.done
}