package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type ConcurrentList struct {
	mtx sync.Mutex
	kvs []KeyValue
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	logFile, err := os.OpenFile("MapReduce.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()

	// 2. Set output of standard logger to the file
	log.SetOutput(logFile)

	// 3. Optional: customize log format
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	for {
		// periodically ask for task (map / reduce) <-
		if task, ok := callForTask("Coordinator.AskForTask"); ok {
			files, err := handleTask(task, mapf, reducef)
			// if there is an error , keep running this taks till you ticker up
			if err != nil {
				// set up a timer for 6sec after
				// after this time you stop re-compute the same failed task
				// the coordinator responsiblity to re-schduling it
				exit := false
				time.AfterFunc(6*time.Second, func() {
					exit = true
				})
				for err != nil && exit == false {
					files, err = handleTask(task, mapf, reducef)
				}
				if err != nil {
					log.Printf("worker abandon taskId : %d", task.TaskId)
				} else {
					trySubmitTask("Coordinator.SubmitCompletedTask", files, task)
				}
			} else {
				trySubmitTask("Coordinator.SubmitCompletedTask", files, task)
			}

		} else { // sleep for one second if there no tasks <-
			log.Println("No Available tasks")
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func trySubmitTask(rpcname string, files []string, task *Task) {
	if ok := callForSubmitTask(rpcname, files, task); ok {
		log.Printf("Task : %d submitted ", task.TaskId)
		log.Println()
	} else {
		log.Printf("Task : %d failed to submit ", task.TaskId)
		log.Println()
	}
}
func handleTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) ([]string, error) {
	files := []string{}
	var err error
	switch task.TypeOfTask {
	case Map:
		files, err = handleMap(task, mapf)
	case Reduce:
		log.Printf("reduce %v  got this map files \n", task.PartitionOrNreduce)
		log.Println(task.Files)
		files, err = handleReduce(task, reducef)
	}
	return files, err
}
func handleReduce(task *Task, reducef func(string, []string) string) ([]string, error) {
	concList := ConcurrentList{
		sync.Mutex{},
		[]KeyValue{},
	}
	wg := sync.WaitGroup{}
	kvs := []KeyValue{}
	chErr := make(chan error)
	for _, file := range task.Files {
		wg.Add(1)
		go accumulateIntermedaites(&concList, file, chErr, &wg)
	}
	wg.Wait()
	kvs = concList.kvs
	if len(chErr) > 0 {
		for len(chErr) > 0 {
			log.Println(<-chErr)
		}
		return []string{}, fmt.Errorf("Error trying to create output files")
	}
	// after checking there no errors
	// sort kvs based on key
	sort.Sort(ByKey(kvs))
	outputname := "mr-out-" + strconv.Itoa(task.PartitionOrNreduce)
	res, err := writeToOutputFiles(outputname, kvs, reducef)
	return []string{res}, err
}
func accumulateIntermedaites(concList *ConcurrentList, file string, ch chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	kv := readIntermediateFile(file, ch)
	concList.mtx.Lock()
	defer concList.mtx.Unlock()
	concList.kvs = append(concList.kvs, kv...)
}

// take all the key-value entries and feed it to the reduce function
// accumulate the result key by key
// generate the output files out-x
func writeToOutputFiles(filename string, intermediate []KeyValue, reducef func(string, []string) string) (string, error) {
	log.Printf("file : %v got intermedait len %v ", filename, len(intermediate))
	dir := filepath.Dir(filename)
	file, err := os.CreateTemp(dir, "outtmp-*")
	if err != nil {
		log.Println("Error During Creating Temp file for output file")
		return "", err
	}
	defer file.Close()
	defer os.Remove(file.Name())
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//log.Println(intermediate[i].Key)
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Remove(filename)
	os.Rename(file.Name(), filename)
	return filename, nil
}
func readIntermediateFile(filename string, chErr chan error) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		chErr <- fmt.Errorf("Error open file %s", filename)
		return []KeyValue{}
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	kvs := []KeyValue{}
	var kv KeyValue
	for {
		if err := decoder.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}

	return kvs
}
func handleMap(task *Task, mapf func(string, string) []KeyValue) ([]string, error) {
	// use the getFileContent function to get the content of file as string ->
	// feed this string to the map function
	intermediate := []KeyValue{} // accumlate the intermediate values <-
	for _, file := range task.Files {
		content, err := getFileContent(file)
		if err != nil {
			// what will happen when we can't open a file from the input list
			// do we re-compute the map again
			// or we continue without this file
			// log.Fatal("on MapId : %d" , task.TaskId , err)
			return []string{}, fmt.Errorf("can't read file's %s content", file)
		}
		// pass the file path and it's content to the map function
		kvs := mapf(file, content)
		intermediate = append(intermediate, kvs...)
	}
	buckets := partitionIntermediateValues(intermediate, task.PartitionOrNreduce)
	wg := sync.WaitGroup{}
	errChan := make(chan error, task.PartitionOrNreduce /*it's a map task in this case partitionorNreduce will be nreduce*/)
	intermediateFilesPaths := make([]string, task.PartitionOrNreduce)
	// store partition on files ->
	for idx, kv := range buckets {
		wg.Add(1)
		filename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(idx)
		intermediateFilesPaths[idx] = filename
		go createIntermediateJsonFile(filename, kv, errChan, &wg)
	}
	// wait until all go routines finishes ->
	wg.Wait()
	// check if there is an error happened in one or more go routine ->
	if len(errChan) > 0 {
		for len(errChan) > 0 {
			log.Println(<-errChan)
		}
		return []string{}, fmt.Errorf("Error trying to create intermediate files")
	}
	return intermediateFilesPaths, nil
}
func createIntermediateJsonFile(filename string, content []KeyValue, errChan chan error, wg *sync.WaitGroup) {
	// all os methods used here are atomic
	// no two temp files will have the same name
	defer wg.Done()
	dir := filepath.Dir(filename)
	file, err := os.CreateTemp(dir, "intertmp-*")
	if err != nil {
		errChan <- fmt.Errorf("Error creating temp file for filename : %s", filename)
		return
	}
	defer file.Close()
	defer os.Remove(file.Name()) // in case partial writes
	// data, err := json.MarshalIndent(content, "", " ")
	// if err != nil {
	// 	errChan <- fmt.Errorf("Error Marshaling content for filename : %s", filename)
	// 	return
	// }
	// if _, err := file.Write(data); err != nil {
	// 	errChan <- fmt.Errorf("Error writing on temp file for filename : %s", filename)
	// 	return
	// }
	enc := json.NewEncoder(file)
	for _, kv := range content {
		if err := enc.Encode(&kv); err != nil {
			log.Println("Error Writing INtermediate file")
			errChan <- err
			return
		}
	}
	os.Remove(filename)                                      // if a file found with same name cuz a prev crashed worker
	if err := os.Rename(file.Name(), filename); err != nil { // after write operation completion
		errChan <- fmt.Errorf("Error renaming temp file for filename : %s", filename)
	}

}
func partitionIntermediateValues(intermediate []KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	// distribute each key on it's partition based on the partitioning function
	for _, kv := range intermediate {
		bucketNo := getBucketNo(kv.Key, nReduce)
		buckets[bucketNo] = append(buckets[bucketNo], kv)
	}
	return buckets
}

// open the file
// read all it's content
// return content as string
func getFileContent(file string) (string, error) {
	fptr, err := os.Open(file)
	if err != nil {
		return "", fmt.Errorf(" Trying To Open A file %s", file)
	}
	fbytes, err := ioutil.ReadAll(fptr)
	if err != nil {
		return "", fmt.Errorf(" Trying To ReadBytes of file %s", file)
	}
	fptr.Close()
	return string(fbytes), nil
}

func getBucketNo(key string, nReduce int) int {
	return ihash(key) % nReduce
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
func callForTask(rpcname string) (*Task, bool) {
	args := AskArgs{}
	task := Task{}
	ok := call(rpcname, args, &task)
	return &task, ok
}
func callForSubmitTask(rpcname string, files []string, task *Task) bool {
	tasksub := *task
	tasksub.Files = files
	args := AskArgs{}
	return call(rpcname, &tasksub, &args)
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
