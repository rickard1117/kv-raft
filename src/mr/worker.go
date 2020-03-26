package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	// "ioutil"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		job := TakeJob()

		if job.Type == ShutdownJob {
			log.Printf("shutdown job, do nothing")
			break
		}

		log.Printf("TakeJob : %+v\n", job)
		if job.Type == MapJob {
			doMap(mapf, job)
			FinishJob(job)
		} else {
			doReduce(reducef, job)
			FinishJob(job)
		}
	}
	
	log.Println("Worker done, will exit")
}

func doMap(mapf func(string, string) []KeyValue, job Job) {
	dat, err := ioutil.ReadFile(job.MapFileName)
	if err != nil {
		log.Fatalf("ReadFile %s failed : %s", job.MapFileName, err)
	}
	kvs := mapf(job.MapFileName, string(dat))
	emit(kvs, job.Index, job.NReduce)
}

func readMidFile(nmap int, reducei int) []KeyValue {
	var kvs []KeyValue
	for i := 0; i < nmap; i++ {
		func() {
			name := interFileName(i, reducei)
			file, err := os.Open(name)
			if err != nil {
				log.Fatalf("open %s file error : %v\n", name, err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				var kv KeyValue
				if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
					log.Fatal("json.Unmarshal failed")
				}
				kvs = append(kvs, kv)
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		}()
	}
	return kvs
}

func reduceFile(reducei int) *os.File {
	name := "mr-out-" + strconv.Itoa(reducei)
	log.Printf("going to generate output file : %v", name)
	file, err := os.Create(name)
	if err != nil {
		log.Fatalf("create file %v failed : %v", name, err)
	}
	return file
}

func doReduce(reducef func(string, []string) string, job Job) {
	reduceFile := reduceFile(job.Index)
	defer reduceFile.Close()
	kvs := readMidFile(job.NMap, job.Index)
	sort.Sort(ByKey(kvs))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key  {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}

		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(reduceFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	log.Printf("emit reduce file %v\n", reduceFile.Name())
}

func createTempFiles(num int) []*os.File {
	var files []*os.File
	for i := 0; i < num; i++ {
		tmpfile, err := ioutil.TempFile("./", "worker")
		if err != nil {
			log.Fatal(err)
		}
		files = append(files, tmpfile)
	}
	return files
}

func interFileName(mapi int, reducei int) string {
	return "mr-" + strconv.Itoa(mapi) + "-" + strconv.Itoa(reducei) + ".tmp"
}

func emit(kvs []KeyValue, index int, nreduce int) {
	if nreduce <= 0 {
		panic("nreduce must > 0")
	}
	files := createTempFiles(nreduce)

	for _, kv := range kvs {
		b, err := json.Marshal(kv)
		if err != nil {
			panic("json.Marshal failed")
		}
		files[ihash(kv.Key)%nreduce].WriteString(string(b) + "\n")
	}
	for i, file := range files {
		file.Close()
		os.Rename(file.Name(), interFileName(index, i))
	}
}

// TakeJob takes a job from master, it could be map or reduce job.
func TakeJob() Job {
	args := TakeJobArgs{}
	job := Job{}
	if !call("Master.TakeJob", &args, &job) {
		return emptyJob
	}
	return job
}

func FinishJob(job Job) {
	reply := FinishReply{}
	if !call("Master.Finish", &job, &reply) {
		log.Fatalf("finish job wrong : %v", job)
	}
	log.Printf("finish job %+v : result %t\n", job, reply.Succeed)
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
