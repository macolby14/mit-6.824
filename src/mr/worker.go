package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
)

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

type control struct {
	locks map[int]*sync.Mutex
	mu sync.Mutex
} 

var cont = &control{}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the master.
	jobActive := true
	for jobActive{
		jobActive = CallMaster(mapf, reducef)
	}

	fmt.Printf("worker is done")
}

func CallMaster(mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	args := FetchTaskArgs{}
	reply := FetchTaskReply{}
	res := call("Master.FetchTask",&args,&reply)
	filename := reply.File	
	buckets := reply.IntermediateSize
	file, err := os.Open(filename)
	if err != nil {
		log.Panicf("cannot read %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Panicf("cannot read %v", filename)
	}
	file.Close()
	
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		bucket := ihash(kv.Key) % buckets
		cont.mu.Lock()
		if cont.locks == nil{
			cont.locks = make(map[int]*sync.Mutex)
		}

		mu, done := cont.locks[bucket]
		if !done {
			cont.locks[bucket] = &sync.Mutex{}
			mu = cont.locks[bucket]
		} 
		mu.Lock()
		cont.mu.Unlock()
		intermediateFile := fmt.Sprintf("mr-int-%v",bucket)
		file, err := os.OpenFile(intermediateFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY,0777)
		if err != nil {
			log.Panicf("Could not open or append to intermediate %v because %v",intermediateFile, err)
		}
		_, err = file.WriteString(fmt.Sprintf("%v %v\n",kv.Key,kv.Value))
		if err != nil {
			log.Panicf("Did not write to the file %v due to %v",intermediateFile, err)
		}
		file.Close()
		mu.Unlock()
	}
	log.Printf("Processed file %v with map task\n",filename)
	return res
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
