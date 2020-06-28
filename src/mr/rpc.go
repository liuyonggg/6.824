package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"
import "sync"

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

// Add your RPC definitions here.
type MRArgs struct {
	ID int
	Status int
}

type MRReply struct {
	N int
	M int
	Input string
	Output string
	Task int
	ID int
	mux sync.Mutex
}

func printInfo(name string, req *MRArgs, reply *MRReply) {
	fmt.Printf("[info]: [%v]: req : ID=%v, status=%v, rep : ID=%v, input=%v, output=%v, task=%v, N=%v, M=%v\n", name, req.ID, req.Status, reply.ID, reply.Input, reply.Output, reply.Task, reply.N, reply.M)
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
