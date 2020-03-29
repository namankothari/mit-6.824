package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	taskType string;
	taskNumber int;
	hasCompleted bool;
}

type Master struct {
	// Your definitions here.
	mu sync.Mutex;
	nReduce int;
	files []string;
	mapTasks []*Task;
	reduceTasks []*Task;
	currentMapTasks []*Task;
	currentReduceTasks []*Task;
}

// Your code here -- RPC handlers for the worker to call.

// RPC handler for GetTask
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	m.mu.Lock();
	// log.Print("mapTasks: " + strconv.Itoa(len(m.mapTasks)) + " currentMapTasks: " + strconv.Itoa(len(m.currentMapTasks)))

	if (len(m.mapTasks) > 0) {

		task := m.mapTasks[0];
		m.mapTasks = m.mapTasks[1:];
		m.currentMapTasks = append(m.currentMapTasks, task);

		m.mu.Unlock();

		reply.TaskType = task.taskType;
		reply.TaskNumber = task.taskNumber;
		reply.NumReducers = m.nReduce;
		reply.NumFiles = len(m.files)
		reply.FileName = m.files[task.taskNumber];

		go func() {
			time.Sleep(time.Second * 10);

			m.mu.Lock();

			if !task.hasCompleted {
				// log.Print("**** Map task not complete: " + strconv.Itoa(task.taskNumber))
				m.mapTasks = append(m.mapTasks, task)

				var taskIndex int;
				for index, t := range m.currentMapTasks {
					if t.taskNumber == task.taskNumber && t.taskType == task.taskType {
						taskIndex = index
					}
				}
				m.currentMapTasks = append(m.currentMapTasks[:taskIndex], m.currentMapTasks[:taskIndex + 1]...)
			}
			m.mu.Unlock();
		}()

		return nil
	} else if (len(m.mapTasks) == 0 && len(m.currentMapTasks) != 0) {
		reply.TaskType = "WAIT";
		m.mu.Unlock();
		return nil;
	} else if (len(m.reduceTasks) > 0) {

		task := m.reduceTasks[0];
		m.reduceTasks = m.reduceTasks[1:];
		m.currentReduceTasks = append(m.currentReduceTasks, task);

		m.mu.Unlock();

		reply.TaskType = task.taskType;
		reply.TaskNumber = task.taskNumber;
		reply.NumReducers = m.nReduce;
		reply.NumFiles = len(m.files)

		go func() {
			time.Sleep(time.Second * 10);
			m.mu.Lock();

			if !task.hasCompleted {
				m.reduceTasks = append(m.reduceTasks, task)
				var taskIndex int;
				for index, t := range m.currentReduceTasks {
					if t.taskNumber == task.taskNumber && t.taskType == task.taskType {
						taskIndex = index
					}
				}
				m.currentReduceTasks = append(m.currentReduceTasks[:taskIndex], m.currentReduceTasks[:taskIndex + 1]...)
			}
			m.mu.Unlock();
		}()

		return nil;
	} else if (len(m.reduceTasks) == 0 && len(m.currentReduceTasks) != 0) {
		reply.TaskType = "WAIT";
		m.mu.Unlock();
		return nil;
	}
	m.mu.Unlock();
	return errors.New("error in get task rpc handler");
}

func (m *Master) NotifyTaskStatus(args *GetNotifyTaskArgs, reply *GetNotifyTaskReply) error {

	taskSuccess, taskType, taskNumber := args.TaskSuccess, args.TaskType, args.TaskNumber
	// log.Print("Notify taskType: " + taskType  + " taskNumber: " + strconv.Itoa(taskNumber) + " taskSuc: " + strconv.FormatBool(taskSuccess))

	if taskSuccess {
		if taskType == "MAP" {
			taskIndex := -1;
			m.mu.Lock();

			for index, t := range m.currentMapTasks {
				if t.taskNumber == taskNumber && t.taskType == taskType {
					taskIndex = index
					t.hasCompleted = true
					// log.Print("Assigned true to map task: " + strconv.Itoa(taskNumber))
				}
			}

			if taskIndex != -1 {
				m.currentMapTasks = append(m.currentMapTasks[:taskIndex], m.currentMapTasks[taskIndex + 1:]...)
			}
			m.mu.Unlock();
			return nil
		} else if taskType == "REDUCE" {
			taskIndex := -1;
			m.mu.Lock();

			for index, t := range m.currentReduceTasks {
				if t.taskNumber == taskNumber && t.taskType == taskType {
					taskIndex = index
					t.hasCompleted = true
				}
			}
			if taskIndex != -1 {
				m.currentReduceTasks = append(m.currentReduceTasks[:taskIndex], m.currentReduceTasks[taskIndex + 1:]...)
			}
			m.mu.Unlock();
			return nil
		}
	} else {
		if taskType == "WAIT" {
			return nil
		}
	}

	return errors.New("error in notify rpc handler")
}

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

	// Your code here.
	m.mu.Lock();
	status := len(m.mapTasks) == 0 && len(m.reduceTasks) == 0 && len(m.currentMapTasks) == 0 && len(m.currentReduceTasks) == 0
	m.mu.Unlock()

	return status
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce;
	m.files = files;

	for i:= 0; i < len(files); i++ {
		task := Task{
			taskType: "MAP",
			taskNumber: i,
			hasCompleted: false,
		}
		m.mapTasks = append(m.mapTasks, &task)
	}

	for i:= 0; i < nReduce; i++ {
		task := Task{
			taskType: "REDUCE",
			taskNumber: i,
			hasCompleted: false,
		}
		m.reduceTasks = append(m.reduceTasks, &task)
	}

	m.server()
	return &m
}
