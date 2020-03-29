package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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
	// CallExample()

	callSuccess, taskSuccess, taskType, taskNumber := GetTask(mapf, reducef)
	time.Sleep(time.Second)
	notifySuccess := NotifyTaskStatus(taskSuccess, taskType, taskNumber)

	for (callSuccess && notifySuccess) {
		callSuccess, taskSuccess, taskType, taskNumber = GetTask(mapf, reducef)
		time.Sleep(time.Second)
		notifySuccess = NotifyTaskStatus(taskSuccess, taskType, taskNumber)
	}
}

func NotifyTaskStatus(taskSuccess bool, taskType string, taskNumber int) bool {
	args := GetNotifyTaskArgs{}

	// fill in the argument(s).
	args.TaskSuccess = taskSuccess
	args.TaskType = taskType
	args.TaskNumber = taskNumber

	// declare a reply structure.
	reply := GetNotifyTaskReply{}
	// log.Print("Notify taskType: " + taskType  + " taskNumber: " + strconv.Itoa(taskNumber) + " taskSuc: " + strconv.FormatBool(taskSuccess))

	// send the RPC request, wait for the reply.
	notifySuccess := call("Master.NotifyTaskStatus", &args, &reply)

	return notifySuccess
}

//
// RPC call to get task from master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (bool, bool, string, int) {

	// declare an argument structure.
	args := GetTaskArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	callSuccess := call("Master.GetTask", &args, &reply)
	taskSuccess := false
	if (callSuccess) {
		taskSuccess = DoTask(reply, mapf, reducef);
	}

	return callSuccess, taskSuccess, reply.TaskType, reply.TaskNumber;
}


func DoTask(reply GetTaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

		switch(reply.TaskType) {
		case "MAP":
			// log.Print("got MAP task")
			return DoMapTask(mapf, reply.FileName, reply.TaskNumber, reply.NumReducers);
		case "REDUCE":
			// log.Print("got REDUCE task")
			return DoReduceTask(reducef, reply.TaskNumber, reply.NumFiles);
		case "WAIT":
			// log.Print("got WAIT task")
			return false;
		}
		return false;
}

func DoMapTask(mapf func(string, string) []KeyValue, fileName string, taskNumber int, numReducers int) bool {

		var mapTempFiles []*os.File
		var mapTempFileNames []string
		var mapFileEncoders []*json.Encoder

		for i := 0; i < numReducers; i++ {
			tempFile, err := ioutil.TempFile(".", "map")
			if err != nil {
				log.Fatal(err)
				return false
			}
			mapFileEncoders = append(mapFileEncoders, json.NewEncoder(tempFile))
			mapTempFileNames = append(mapTempFileNames, tempFile.Name())
			mapTempFiles = append(mapTempFiles, tempFile)
		}

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
			return false
		}
		file.Close()

		kva := mapf(fileName, string(content))

		for _, kv := range kva {
			key := kv.Key;
			reducerNumber := ihash(key) % numReducers

			fileEncoder := mapFileEncoders[reducerNumber]
			fileEncoder.Encode(&kv)
		}

		for index, tempFile := range mapTempFiles{
			tempFile.Close();
			tempFileName := mapTempFileNames[index]
			mapFileName := "mr-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(index);
			os.Rename(tempFileName, mapFileName);
		}

		return true;
}

func DoReduceTask(reducef func(string, []string) string, taskNumber int, numFiles int) bool {

	kva := []KeyValue{}

	for i:=0; i<numFiles; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNumber);
		file, err := os.Open(fileName);
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return false
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close();
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	tempFile, _ := ioutil.TempFile(".", "reduce")
	tempFileName := tempFile.Name();

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tempFile.Close()
	reduceFileName := "mr-out-" + strconv.Itoa(taskNumber);
	os.Rename(tempFileName, reduceFileName);
	return true;
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
