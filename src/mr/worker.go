package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the master.
	jobActive := true
	for jobActive{
		jobActive = CallMaster(mapf, reducef)
		time.Sleep(time.Second)
	}

	fmt.Printf("worker is done")
}

func CallMaster(mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	args := FetchTaskArgs{}
	reply := FetchTaskReply{}
	res := call("Master.FetchTask",&args,&reply)
	filename := reply.File	
	buckets := reply.IntermediateSize
	taskType := reply.TaskType

	if taskType == "map"{
		mapTask(mapf, filename, buckets)
	} else if taskType == "reduce"{
		fmt.Println("Performing reduce task")
		reduceTask(reducef, filename)
	}else{
		fmt.Println("error in task type")
	}
	reportCompletion(filename)

	return res
}

func reportCompletion(filename string) bool{
	args := ReportCompletionArgs{}
	args.File = filename
	reply := ReportCompletionReply{}
	res := call("Master.ReportCompletion",&args,&reply)
	return res
}

func mapTask(mapf func(string, string) []KeyValue, filename string, buckets int){
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
		intermediateFile := fmt.Sprintf("mr-int-%v",bucket)
		file, err := os.OpenFile(intermediateFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY,0777)
		if err != nil {
			log.Panicf("Could not open or append to intermediate %v because %v",intermediateFile, err)
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Panicf("Something wen wrong encoding JSON")
		}
		file.Close()
	}
	log.Printf("Processed file %v with map task\n",filename)
}


func reduceTask(reducef func(string, []string) string, filename string){
	kva := make([]KeyValue,0)
	file, err := os.Open(filename)
	if err != nil {
		log.Panicf("cannot read %v", filename)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	i := 0
	for j := i+1; j <= len(kva); j++ {	
		if j<len(kva) && kva[i].Key == kva[j].Key{
			j++
		}else{
			values := make([]string, j-i)
			for ind, value := range kva[i:j]{
				values[ind] = value.Value 
			}
			res := reducef(kva[i].Key, values)
			log.Printf("%v %v", kva[i].Key, res)
			i = j
		}
	}


	file.Close()
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
