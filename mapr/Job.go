package mapr

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type IMapreduce interface {
	Map(filename string) []KeyValue
	Reduce(key string,value []string) KeyValue
}

type Job struct {
	Jobname 	string
	NReduce 	int
	Inputfiles 	string
	Conf		*Config
}

func (jb *Job) Map(contents string) []KeyValue {
	bs := []rune(contents)
	for i,v := range bs {
		if !unicode.IsLetter(v) {
			bs[i] = ' '
		}
	}
	words := strings.Fields(string(bs))

	/*words := strings.FieldsFunc(contents, func(ch rune) bool {
		return !unicode.IsLetter(ch)
	})*/
	kvList := make([]KeyValue,0)
	for _,word := range words {
		kvList = append(kvList,KeyValue{word,"1"})
	}
	return kvList
}

func (jb *Job) Reduce(key string,values []string) KeyValue {
	return KeyValue{key,strconv.Itoa(len(values))}
}

func AddJob() {
	submitjob := &Job{"job01",3,"input.txt",nil}
    var reply ReplyMessage
	ok := Call(MasterAddress,"Master.AddJob",submitjob,&reply)
	if ok {
		fmt.Println("submit job successed")
	} else {
		fmt.Println("submit job failed")
	}
}
