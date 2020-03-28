package main

import (
	"mapr"
	"strconv"
	"sync"
)

func RunMaster()  *mapr.Master {
	mst := new(mapr.Master)
	mst.Initial("127.0.0.1:10010")
	mst.StartRPCServer()
	return mst
}

func RunWorkers() {
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 1; i <= 3; i++ {
		go mapr.RunSingleWorker(&wg,i,"127.0.0.1:1001" + strconv.Itoa(i),"127.0.0.1:10010")
	}
	wg.Wait()
}

func main() {
	mr := RunMaster()
	RunWorkers()
	go mr.MonitorWorkers()
	mapr.AddJob()
	go mr.RunJob()
	mr.Wait()
}