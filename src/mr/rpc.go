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

type TakeJobArgs struct {
}

type FinishReply struct {
	Succeed bool
}

type Job struct {
	Type JobType
	MapFileName string
	// Result string
	Index int
	NReduce int
	NMap int
}

type JobDoneReply struct {
}

type JobType int

const (
	ShutdownJob JobType = iota
	MapJob 
	ReduceJob
)

type WorkerJob struct {
	Type JobType
	Name string
}

type MasterPhase int

const (
	MapPhase = iota
	ReducePhase
)

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
