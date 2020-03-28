package mapr

import (
	"fmt"
	"hash/fnv"
	"net/rpc"
)

// Debugging enabled?
const debugEnabled = true
const MasterAddress = "127.0.0.1:10010"
const DefaultSplitFiles = "H:/毕业设计/myMapReduce/files/splitFiles"
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

type KeyValue struct{
	Key string
	Value string
}

type KeyValueSlice []KeyValue

func (kvs KeyValueSlice) Len() int {
	return len(kvs)
}

func (kvs KeyValueSlice) Less(i,j int) bool {
	return kvs[i].Key < kvs[j].Key
}

func (kvs KeyValueSlice) Swap(i,j int) {
	kvs[i],kvs[j] = kvs[j],kvs[i]
}

type Config struct {
	ChunkSize int
}

func NewDefaultConfig() *Config {
	return &Config{600}
}

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type RegisterArgs struct {
	WorkAddress string
}

type ReplyMessage struct {
	Ok bool
}

type MapTaskArgs struct {
	JobName string
	File string
	NReduce int
	TaskNumber int
}

type ReduceTaskArgs struct {
	JobName string
	WorkerAddress []string
	NReduce int
	ReduceNumber int
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func Call(srv string, rpcmethod string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcmethod, args, reply)
	if err == nil {
		return true
	}
	fmt.Println("call err is ",err)
	return false
}