package mapr

import (
	"os"
	"strconv"
)

//判断路径是否存在，true存在，false不存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// 判断所给路径是否为文件夹
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

// 判断所给路径是否为文件
func IsFile(path string) bool {
	return !IsDir(path)
}

func CreateDir(path string) {
	ok,_ := PathExists(path);
	if ok {
		os.RemoveAll(path)
	}
	os.MkdirAll(path,os.ModePerm)
}

func GetNeedDir(address string,location string) string {
	dir := []rune("H:/毕业设计/myMapReduce/files/" + address)
	dir[len(dir)-6] = '.'
	return string(dir) + "/" + location
}


func MapName(pathName string, jobName string, mapTaskID int) string {
	return pathName + "/" + jobName + "mapTask" + strconv.Itoa(mapTaskID)
}

func ReduceName(pathName string,jobName string,mapTaskId int,partitionId int) string {
	return pathName + "/" +jobName + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(partitionId)
}

func MergeName(pathName string,jobName string,partitionId int) string {
	return pathName + "/" +jobName + "-" + strconv.Itoa(partitionId)
}

func ResultName(pathName string,jobName string) string {
	return pathName + "/" + jobName + "-result"
}