package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"


type Master struct {
	// Your definitions here.
	ID int
	inputfiles []string
	intermediate string
	output string
	task int
	finished int
	M int
	N int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskJob(req *MRArgs, reply *MRReply) error {
	printInfo("enter: master.AskJob", req, reply)
	reply.mux.Lock()
	reply.Task = m.task
	reply.N = m.N
	reply.M = m.M
	reply.ID = m.ID
	m.ID += 1
	if m.task == 0 {
		// in map phase
		if m.ID <= len(m.inputfiles) + 1 {
			reply.Input = m.inputfiles[reply.ID - 1]
			reply.Output = m.intermediate
		} else {
			// still in processing, need ask worker wait
			reply.ID = -1
		}
	} else {
		// in reduce phase
		if m.ID <= m.N + 1 {
			reply.Input = m.intermediate
			reply.Output = m.output
		} else {
			reply.ID = -1
		}
	}
	reply.mux.Unlock()
	printInfo("exit: master.AskJob", req, reply)
	return nil
}


func (m *Master) ReportJob(req *MRArgs, reply *MRReply) error {
	printInfo("enter: master.ReportJob", req, reply)
	reply.ID = req.ID
	reply.Task = m.task
	if req.Status == 0 {
		m.finished += 1
		if m.task == 0 && m.finished == len(m.inputfiles) || m.task == 1 && m.finished == m.N {
			m.ID = 1
			fmt.Printf("[info]: finished phase %v\n", m.task)
			m.task += 1
			m.finished = 0
		}
	}
	fmt.Println("[log]: exit: master.ReportJob, finished: ", m.finished)
	return nil
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
	ret := false

	// Your code here.
	if m.task > 1 {
		fmt.Println("master job is done")
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	fmt.Println("info: MakeMaster is called with: ", files, nReduce)
	m.ID = 1
	m.inputfiles = files
	m.intermediate = "mr-inter"
	m.output = "mr-out"
	m.task = 0
	m.finished = 0
	m.M = len(files)
	m.N = nReduce
	m.server()
	return &m
}
