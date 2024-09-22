package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SliKey []KeyValue

func (s SliKey) Len() int { return len(s) }

func (s SliKey) Swap(a, b int) { s[a], s[b] = s[b], s[a] }

func (s SliKey) Less(a, b int) bool { return s[a].Key < s[b].Key }

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
	for {
		args := WorkArgs{}
		reply := WorkReply{}
		if ok := call("Master.AllocateTask", &args, &reply); !ok || reply.TaskType == 3 {
			break
		}
		if reply.TaskType == 0 {
			interMadia := SliKey{}
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("open file err:%s", err.Error())
			}
			data, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("read file err:%s", err.Error())
			}
			file.Close()

			kvaule := mapf(reply.FileName, string(data))
			interMadia = append(interMadia, kvaule...)

			buckets := make([]SliKey, reply.NumberReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{{Key: string(i)}}
			}
			for _, kva := range interMadia {
				buckets[ihash(kva.Key)%reply.NumberReduce] = append(buckets[ihash(kva.Key)%reply.NumberReduce], kva)
			}
			for i := range buckets {
				oname := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
				ofile, err := os.CreateTemp("", oname+"*")
				if err != nil {
					continue
				}
				enc := json.NewEncoder(ofile)
				for _, kva := range buckets {
					if err := enc.Encode(kva); err != nil {
						log.Fatalf("can't write to the file %s", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}
			finishWorks := WorkArgs{reply.MapTaskNumber, -1}
			finishReply := WorkReply{}
			call("Master.ReciveFinishMap", &finishWorks, &finishReply)
		} else if reply.TaskType == 1 {
			interMediate := SliKey{}
			for i := 0; i < reply.NumberMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("can't open the file %s", iname)
				}
				enc := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := enc.Decode(&kv); err != nil {
						break
					}
					interMediate = append(interMediate, kv)
				}
				file.Close()
			}
			sort.Sort(interMediate)
			oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNumber)
			ofile, _ := os.CreateTemp("", oname+"*")

			i := 0
			for i < len(interMediate) {
				j := i + 1
				for j < len(interMediate) && interMediate[i].Key == interMediate[j].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, interMediate[k].Value)
				}
				output := reducef(interMediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", interMediate[i].Key, output)
				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()

			for i := 0; i < reply.NumberMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				if err := os.Remove(iname); err != nil {
					log.Fatalf("can't open delete file %s\n", iname)

				}
			}
			finishArgs := WorkArgs{-1, reply.ReduceTaskNumber}
			finishReply := WorkReply{}
			call("Master.ReceiveFinishReduce", &finishArgs, finishReply)
		}
		time.Sleep(time.Second)
	}
	// return
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
