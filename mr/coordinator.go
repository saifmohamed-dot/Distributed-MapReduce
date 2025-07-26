package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// A thread-safe definition for the remaining task that's not finished yet <-
type TasksRem struct {
	cnt int
	mtx sync.Mutex
}

// Generate A new Id For Each Task <-
type IDGenerator struct {
	nextId int
}
type CompletedTasks struct {
	mp  map[int][]string // task-id => [R0 , R1 , ... , Rn]
	mtx sync.Mutex
}

type TaskWaitQueue struct {
	queue chan Task
	mtx   sync.Mutex
}
type InProgressTasks struct {
	mp  map[int]*time.Timer
	mtx sync.Mutex
}

func (ipt *InProgressTasks) isInProgressTask(taskId int) bool {
	ipt.mtx.Lock()
	defer ipt.mtx.Unlock()
	_, ok := ipt.mp[taskId]
	return ok
}
func (ipt *InProgressTasks) setTaskTimer(taskId int, timer *time.Timer) {
	ipt.mtx.Lock()
	defer ipt.mtx.Unlock()
	if t, ok := ipt.mp[taskId]; ok {
		// preventing old timers leak (if found) <-
		t.Stop()
	}
	ipt.mp[taskId] = timer
}
func (ipt *InProgressTasks) stopTaskTimer(taskId int) {
	ipt.mtx.Lock()
	defer ipt.mtx.Unlock()
	if timer, ok := ipt.mp[taskId]; ok {
		timer.Stop()
		delete(ipt.mp, taskId)
	}
}
func (twq *TaskWaitQueue) pushTask(task Task) {
	twq.mtx.Lock()
	defer twq.mtx.Unlock()
	twq.queue <- task
}
func (twq *TaskWaitQueue) popTask() (Task, error) {
	// lock on the wait queue to make sure
	// that no two requests ask for task and the queue only got one
	// so a call will take a task and the other will be blocked forever <-
	twq.mtx.Lock()
	defer twq.mtx.Unlock()
	if len(twq.queue) == 0 {
		return Task{}, fmt.Errorf("No Task Available")
	}
	return <-twq.queue, nil
}
func (tr *TasksRem) get() int {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()
	return tr.cnt
}
func (tr *TasksRem) decrement(val int) {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()
	tr.cnt -= val
}

// group each reduce task's data (m1rx , m2rx , m3rx , m1ry , m2ry , ....)
func (ct *CompletedTasks) shuffle(splites int, nreduce int) ([][]string, error) {
	ct.mtx.Lock()
	defer ct.mtx.Unlock()
	if len(ct.mp) != splites {
		return [][]string{{}}, fmt.Errorf("%v splites not completed yet", splites-len(ct.mp))
	}
	shuffMtrx := make([][]string, nreduce)
	for i := 0; i < nreduce; i++ {
		shuffMtrx[i] = make([]string, splites)
	}
	i := 0
	for _, value := range ct.mp {
		for idx, val := range value {
			shuffMtrx[idx][i] = val
		}
		i++
	}
	return shuffMtrx, nil
}
func (ct *CompletedTasks) trySetNew(key int, value []string) bool {
	// make a write operation <-
	ct.mtx.Lock()
	defer ct.mtx.Unlock()
	_, ok := ct.mp[key]
	// if the task is found in completed taks already
	// then we don't need to added again
	// so we don't decrement the rem counter
	// (the global counter for map + reduce tasks) <-
	if ok == false {
		ct.mp[key] = value
	}
	return !ok
}

// check if a task in the completedTask
func (ct *CompletedTasks) isCompleted(key int) bool {
	ct.mtx.Lock()
	defer ct.mtx.Unlock()
	_, found := ct.mp[key]
	return found
}
func (idGen *IDGenerator) generateId() int {
	idGen.nextId += 1
	return idGen.nextId
}

type Coordinator struct {
	// Your definitions here.
	// Blocked task waiting queue (a buffered channel of task) <-
	taskWaitingQueue TaskWaitQueue
	// type CompleteMapTasks struct Map[taskId][]string with lock <-
	completedMapTasks CompletedTasks
	// inprogress tasks have to have a timer
	// this timer will be fire if the task not isn't done already (after 10s)
	// and we will re-schdule the task again in the waiting queue
	// cancel this re-schduling if the task finished before 10s <-
	inProgressTasks InProgressTasks
	// counter for the the over all completed tasks (M + R)
	// this is how we will if we done or not
	// make a struct with lock on it <-
	remainingTasks TasksRem
	// id Generator for each task <-
	idGenerator IDGenerator
	// nreduce
	nreduce int
	// M splites
	splites int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) initMapTasks(mapFiles []string) {
	for _, file := range mapFiles {
		// populate the channel (blocking queue) with the Map tasks
		c.taskWaitingQueue.pushTask(
			Task{c.idGenerator.generateId(), []string{file}, Map, c.nreduce},
		)
	}
}

func (c *Coordinator) initReduceTasks(nReducePartitions [][]string) {
	for idx, partitions := range nReducePartitions {
		c.taskWaitingQueue.pushTask(
			Task{c.idGenerator.generateId(), partitions, Reduce, idx},
		)
	}
}

func (c *Coordinator) AskForTask(args AskArgs, task *Task) error {
	tsk, err := c.taskWaitingQueue.popTask()
	if err != nil { // no tasks available
		return err
	}
	// check the case when a machine late in complete a task
	// so we re-schdule the task
	// but the late machine happen to finish before the re-schduled task got picked
	// so when the worker ask for this task that already got submited
	// we didn't re-compute this task again <-
	if c.completedMapTasks.isCompleted(tsk.TaskId) {
		return fmt.Errorf("This Task Already Done")
	}
	task.Copy(&tsk)
	// if the task didn't finshed after ten second
	// consider that the machine gone
	// reschduling this task again in the waitqueue <-
	timer := time.AfterFunc(10*time.Second, func() {
		if c.completedMapTasks.isCompleted(tsk.TaskId) == false {
			c.taskWaitingQueue.pushTask(tsk)
		}
	})
	go c.inProgressTasks.setTaskTimer(tsk.TaskId, timer)
	return nil
}
func (c *Coordinator) SubmitCompletedTask(task *Task, args *AskArgs) error {
	if len(task.Files) != c.nreduce && task.TypeOfTask == Map {
		return fmt.Errorf("Not Enough nReduce files ")
	}
	// double check if the task reduce actually done without partial write
	if task.TypeOfTask == Reduce {
		if len(task.Files) != 1 || task.Files[0] != ("mr-out-"+strconv.Itoa(task.PartitionOrNreduce)) {
			return fmt.Errorf("Not valid reduce submition")
		}
	}
	// check if the taskId one of the coordinator valid tasks
	// not a made up one from the worker <-
	if c.inProgressTasks.isInProgressTask(task.TaskId) == false {
		return fmt.Errorf("This's not an Inprogress task ")
	}
	// maybe you want to check ( if bug occured )
	// if the taskId is in the range of the generated IDs <-
	// try to insert the newly completed task <-
	if c.completedMapTasks.trySetNew(task.TaskId, task.Files) {
		// if new then decrement the remaining counter <-
		go c.remainingTasks.decrement(1)
	}
	go c.inProgressTasks.stopTaskTimer(task.TaskId)
	return nil
}

// a thread that periodically check if the map tasks ends
func (c *Coordinator) checkMapsEnds() {
	for c.isMapTasksRunning() == true {
		fmt.Println("Still There Are Map Tasks ... ")
		time.Sleep(2 * time.Second)
	}
	// shuffle the completedTasks ->
	shuffled, err := c.completedMapTasks.shuffle(c.splites, c.nreduce)
	if err != nil {
		log.Fatal(err)
	}
	// init reduce tasks <-
	go c.initReduceTasks(shuffled)
}

func (c *Coordinator) isMapTasksRunning() bool {
	return c.remainingTasks.get() != c.nreduce || len(c.taskWaitingQueue.queue) != 0
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// if the remaining task is zero then we finshed
	return c.remainingTasks.get() == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init your coordinator here
	c := Coordinator{
		TaskWaitQueue{make(chan Task, len(files)+nReduce), sync.Mutex{}},
		CompletedTasks{make(map[int][]string), sync.Mutex{}},
		InProgressTasks{make(map[int]*time.Timer), sync.Mutex{}},
		TasksRem{len(files) + nReduce, sync.Mutex{}},
		IDGenerator{0},
		nReduce,
		len(files),
	}
	// Your code here.
	go c.initMapTasks(files)
	go c.checkMapsEnds()
	c.server()
	return &c
}
