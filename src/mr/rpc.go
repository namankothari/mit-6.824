package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskNumber int;
	TaskType string;
	FileName string;
	NumFiles int;
	NumReducers int;
}


type GetNotifyTaskArgs struct {
	TaskSuccess bool;
	TaskType string;
	TaskNumber int
}

type GetNotifyTaskReply struct {
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
