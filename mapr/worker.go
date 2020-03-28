package mapr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Worker struct{
	Address 			string
	MasterAddress 		string
	Map func(string) 	[]KeyValue
	Reduce func(string,[]string) KeyValue
	IntermediateFiles 	string
	ReduceOutFiles    	string
	sync.Mutex
	l 					net.Listener
}

func (wk *Worker) Register() {
	args := &RegisterArgs{wk.Address}
	var reply ReplyMessage
	ok := Call(wk.MasterAddress,"Master.Register",args,&reply)
	if ok {
		fmt.Println(wk.Address + "Register successed!")
	} else {
		fmt.Println("Register failed")
	}
}

func (wk *Worker) Islive(p string,r *ReplyMessage) error {
	r.Ok = true
	return nil
}

func (wk *Worker) StartRPCServer(port int) {
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	l,err := net.Listen("tcp","127.0.0.1:1001" + strconv.Itoa(port))
	if err != nil {
		panic("listen error")
	}
	wk.l = l
	go rpcs.Accept(wk.l)
	debug("Worker %v has already startrpc\n",port)
}

func (wk *Worker) CreateDirs() {
	dir := []rune("H:/毕业设计/myMapReduce/files/" + wk.Address)
	dir[len(dir)-6] = '.'
	wk.IntermediateFiles = string(dir) + "/intermediatefiles"
	wk.ReduceOutFiles = string(dir) + "/reduceoutfiles"
	CreateDir(wk.IntermediateFiles)
	CreateDir(wk.ReduceOutFiles)
}

func RunSingleWorker(wg *sync.WaitGroup,id int,workeraddress string,masterAddress string) {
	wk := new(Worker)
	wk.Address = workeraddress
	wk.MasterAddress = masterAddress
	wk.Register()
	wk.StartRPCServer(id)
	wk.CreateDirs()
	wg.Done()
}

func (wk *Worker) GetJob(job Job,_ *struct{}) error {
	wk.Lock()
	wk.Map = job.Map
	wk.Reduce = job.Reduce
	wk.Unlock()
	return nil
}

func (wk *Worker) RemoveFiles(_ string,_ *struct{}) error {
	fmt.Printf("wk %v is removefile\n",wk.Address)
	if ok,_ := PathExists(wk.IntermediateFiles); ok {
		os.RemoveAll(wk.IntermediateFiles)
	}
	if ok,_ := PathExists(wk.ReduceOutFiles); ok {
		os.RemoveAll(wk.ReduceOutFiles)
	}
	return nil
}

func (wk *Worker) DoMap(jobName string,file string,NReduce int,taskId int) error {
	fileBytes ,err := ioutil.ReadFile(file)
	if err != nil {
		panic("read file err")
	}
	fileContent := string(fileBytes)
	//fmt.Printf("wk.address = %v,wk.map = %v\n",wk.Address,wk.Map)
	kvList := wk.Map(fileContent)
	partitionReduceKv := make(map[int][]KeyValue)
	for _,kv := range kvList {
		index := ihash(kv.Key) % NReduce
		partitionReduceKv[index] = append(partitionReduceKv[index],kv)
	}

	for i := 0; i < NReduce; i++ {
		interMediateFileName := ReduceName(wk.IntermediateFiles,jobName,taskId,i)
		interMediateFile,err := os.Create(interMediateFileName)
		defer interMediateFile.Close()

		if err != nil {
			panic("create file err")
		}
		encode := json.NewEncoder(interMediateFile)
		ok := encode.Encode(partitionReduceKv[i])
		if ok != nil {
			panic("encode err")
		}
	}
	return nil
}

func IsThisParitionFile(file string,partitionId int) bool {
	words := strings.Split(file,"-")
	return strings.Compare(words[1],strconv.Itoa(partitionId)) == 0
}

func (wk *Worker) DoReduce(jobName string,nReduce int,reduceNumber int,workerAddress []string) error {
	allPartions := make([]string,0)
	for _,singleWorkerAdd := range workerAddress {
		workerIntermediateDir := GetNeedDir(singleWorkerAdd,"intermediatefiles")
		rd,err := ioutil.ReadDir(workerIntermediateDir)
		if err != nil {
			panic("read dir err")
		}
		for _,fi := range rd {
			if IsThisParitionFile(fi.Name(),reduceNumber) {
				allPartions = append(allPartions,workerIntermediateDir + "/" + fi.Name())
			}
		}
	}

	mapKvs := make(map[string][]string)
	for _,intermediateFileName := range allPartions {
		file,err := os.Open(intermediateFileName)
		defer file.Close()
		if err != nil {
			panic("open file err")
		}
		decode := json.NewDecoder(file)
		singleFileKv := make([]KeyValue,0)
		decErr := decode.Decode(&singleFileKv)
		if decErr != nil {
			panic(err)
		}
		for _,kv := range singleFileKv {
			mapKvs[kv.Key] = append(mapKvs[kv.Key],kv.Value)
		}
	}
	//存所有的key，用来排序
	keyList := make([]string,0)
	//排序
	for k,_ := range mapKvs {
		keyList = append(keyList,k)
	}
	sort.Strings(keyList)
	//将输出结果按照key的顺序写入到json文件中
	outFile := MergeName(wk.ReduceOutFiles,jobName,reduceNumber)
	outputfile,err := os.Create(outFile)
	defer outputfile.Close()
	if err != nil {
		panic(err)
	}
	outputJson := json.NewEncoder(outputfile)
	for _,key := range keyList {
		vals := mapKvs[key]
		kv := wk.Reduce(key,vals)//调用reduce函数
		ok := outputJson.Encode(kv)
		if ok != nil {
			panic("json encoder err")
		}
	}
	return nil
}

func (wk *Worker) DoMapTask(args MapTaskArgs,_ *struct{}) error {
	debug("wk name = %s\n",wk.Address)
	return wk.DoMap(args.JobName, args.File, args.NReduce, args.TaskNumber)
}

func (wk *Worker) DoReduceTask(args ReduceTaskArgs,_ *struct{}) error {
	debug("wk name = %s reduceId = %d\n",wk.Address,args.ReduceNumber)
	return wk.DoReduce(args.JobName,args.NReduce,args.ReduceNumber,args.WorkerAddress)
}