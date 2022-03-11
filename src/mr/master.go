package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"


type Master struct {
	// Your definitions here.
	MapTaskQueue []string
	MapTaskComplete []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	if( len(m.MapTaskQueue) > 0 ){
		reply.TaskType = "map"
		reply.File = m.MapTaskQueue[0]
		m.MapTaskQueue = m.MapTaskQueue[1:]
		fmt.Printf("Server reply %v %v\n", reply.TaskType, reply.File)
	}
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
	if len(m.MapTaskQueue) == 0 {
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
	/**
	* Make a queue of map tasks by reading the file inputs
	* Wait for a worker to ask for a task
	* Assign the worker a map task
	* Worker performs the task and writes intermediate results to disk
	* Worker reports map task completion
	* Worker asks for another task
	* Once all map tasks are done, read the intermediate files to get a list of reduce tasks 
	*/


	for _, filename := range files {
		m.MapTaskQueue = append(m.MapTaskQueue, filename)
	}
	
	// for _, filename := range m.MapTaskQueue {
	// 	fmt.Printf("%v\n",filename)
	// }

	m.server()
	return &m
}
