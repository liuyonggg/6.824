package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "io/ioutil"
import "io"
import "time"


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
	req := MRArgs{}
	reply := MRReply{}
	for {
		reply.ID = -1
		askjob(&req, &reply)
		reply.mux.Lock()
		if reply.Task > 1 {
			reply.mux.Unlock()
			break
		}
		if reply.ID < 0 {
			reply.mux.Unlock()
			time.Sleep(3 * time.Second)
		} else {
			runjob(&reply, mapf, reducef)
			req.ID = reply.ID
			reportjob(&req, &reply)
			reply.mux.Unlock()
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func askjob(req *MRArgs, reply *MRReply) {
	printInfo("enter: worker.askjob", req, reply)
	call("Master.AskJob", req, reply)
	printInfo("exit: worker.askjob", req, reply)
}

func runjob(reply *MRReply,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	printInfo("enter: worker.runjob", &MRArgs{}, reply)
	if reply.Task == 0 {
		// map
		file, err := os.Open(reply.Input)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Input)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Input)
		}
		file.Close()
		kva := mapf(reply.Input, string(content))
		for _, kv := range kva {
			var file *os.File
			fname := fmt.Sprintf("%s-%v-%v", reply.Output, ihash(kv.Key) % reply.N, reply.ID - 1)
			var _, err = os.Stat(fname)
			if os.IsNotExist(err) {
				file, err = os.Create(fname)
				if err != nil {
					log.Fatalf("cannot create %v", fname)
				}
			} else {
				file, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("cannot open %v", fname)
				}
			}
			enc := json.NewEncoder(file)
			err = enc.Encode(&kv)
			file.Close()
			// fmt.Printf("[info]: %v is written to %v\n", kv, fname)
		}
	} else {
		// reduce
		var res map[string]int
		res = make(map[string]int)

		var fname string
		for i := 0; i < reply.M; i += 1 {
			fname = fmt.Sprintf("%s-%v-%v", reply.Input, reply.ID - 1, i)
			file, err := os.Open(fname)
			if err != nil {
				log.Fatalf("cannot open %v", fname)
			}
			dec := json.NewDecoder(file)
			var m KeyValue
			for {
				if err := dec.Decode(&m); err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
				// fmt.Printf("[info]: read %v %v\n", m.Key, m.Value)
				if _, ok := res[m.Key]; ok {
					res[m.Key] += 1
				} else {
					res[m.Key] = 1
				}
				// fmt.Printf("%s: %s\n", m.Name, m.Text)
			}
			file.Close()
		}
		fname = fmt.Sprintf("%s-%v", reply.Output, reply.ID - 1)
		file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		for key, value := range res {
			fmt.Fprintf(file, "%v %v\n", key, value)
		}
		file.Close()
	}
	printInfo("exit: worker.runjob", &MRArgs{}, reply)
}

func reportjob(req *MRArgs, reply *MRReply) {
	printInfo("enter: worker.reportjob", req, reply)
	call("Master.ReportJob", req, reply)
	printInfo("exit: worker.reportjob", req, reply)
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
