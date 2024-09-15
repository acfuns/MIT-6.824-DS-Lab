package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 不停询问coordinator是否有任务，如果有，则执行任务，否则等待
	for {
		task := CallTask()
		switch task.State {
		case _map:
			workerMap(mapf, task)
		case _reduce:
			workerReduce(reducef, task)
		case _wait:
			time.Sleep(time.Duration(time.Second * 5))
		case _end:
			fmt.Println("Process End")
			return
		default:
			fmt.Println("Unknown State")
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallTask() *TaskState {
	args := ExampleArgs{}
	reply := TaskState{}
	call("Coordinator.HandleTaskRequest", &args, &reply)
	return &reply
}

func CallTaskDone(task *TaskState) {
	reply := ExampleReply{}
	call("Coordinator.TaskDone", task, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func workerMap(mapf func(string, string) []KeyValue, task *TaskState) {
	log.Printf("workerMap task %vth file %s\n", task.MapIndex, task.FileName)

	intermediate := []KeyValue{}
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("workerMap open file %s error %v\n", task.FileName, err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("workerMap read file %s error %v\n", task.FileName, err)
	}
	file.Close()

	kva := mapf(task.FileName, string(content))
	intermediate = append(intermediate, kva...)

	nR := task.R
	outprefix := "mr-tmp/mr-"
	outprefix += strconv.Itoa(task.MapIndex)
	outprefix += "-"

	outFiles := make([]*os.File, nR)
	fileEncoders := make([]*json.Encoder, nR)
	for i := 0; i < nR; i++ {
		outFiles[i], _ = os.CreateTemp("mr-tmp", "mr-tmp-*")
		fileEncoders[i] = json.NewEncoder(outFiles[i])
	}

	for _, kv := range intermediate {
		i := ihash(kv.Key) % nR
		enc := fileEncoders[i]
		// 把key-value对写入对应的reduce文件
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("workerMap encode key %s value %s error %v\n", kv.Key, kv.Value, err)
			panic("workerMap encode error")
		}
	}

	for i, file := range outFiles {
		outname := outprefix + strconv.Itoa(i)
		oldpath := filepath.Join(file.Name())

		os.Rename(oldpath, outname)
		file.Close()
	}

	CallTaskDone(task)
}

func workerReduce(reducef func(string, []string) string, task *TaskState) {
	log.Printf("workerReduce task %vth file %s\n", task.ReduceIndex, task.FileName)

	outname := "mr-out-" + strconv.Itoa(task.ReduceIndex)

	innamepredix := "mr-tmp/mr-"
	innamesuffix := "-" + strconv.Itoa(task.ReduceIndex)

	intermediate := []KeyValue{}
	for i := 0; i < task.M; i++ {
		inname := innamepredix + strconv.Itoa(i) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			log.Printf("workerReduce open file %s error %v\n", inname, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	ofile, err := os.CreateTemp("mr-tmp", "mr-*")
	if err != nil {
		log.Printf("workerReduce create file %s error %v\n", outname, err)
		panic("workerReduce create file error")
	}

	i := 0
	// 为什么这里不直接使用全部的key的value累加成一个字符串数组
	// 因为hash冲突的原因，可能有不相同的key，所以需要判断要累加的key是否相同
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()

	CallTaskDone(task)
}
