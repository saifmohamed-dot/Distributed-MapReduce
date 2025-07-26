package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type Task struct {
	TaskId             int
	Files              []string
	TypeOfTask         TaskType
	PartitionOrNreduce int // has one of two value : nreduce for map tasks , partitionNo for reduce task
}
type AskArgs struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
func (task *Task) Copy(tsk *Task) {
	task.Files = tsk.Files
	task.PartitionOrNreduce = tsk.PartitionOrNreduce
	task.TypeOfTask = tsk.TypeOfTask
	task.TaskId = tsk.TaskId
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
