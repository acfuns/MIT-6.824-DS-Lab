package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	beginTime time.Time
	// 任务编号
	mapIndex    int
	reduceIndex int
	// 文件名 用于map程序读取文件
	FileName string
	// 任务类型的数量
	R int
	M int
}

// 计算差值
func (t *Task) TimeOut() bool {
	return time.Since(t.beginTime) > time.Second*10
}

// 设置时差
func (t *Task) SetTime() {
	t.beginTime = time.Now()
}

type TaskQueue struct {
	tasks []*Task
	mutex sync.Mutex
}

func (t *TaskQueue) Size() int {
	return len(t.tasks)
}

func (t *TaskQueue) Enqueue(task *Task) {
	t.mutex.Lock()
	if task == nil {
		t.mutex.Unlock()
		return
	}
	t.tasks = append(t.tasks, task)
	t.mutex.Unlock()
}

func (t *TaskQueue) Dequeue() (*Task, error) {
	t.mutex.Lock()
	if len(t.tasks) == 0 {
		t.mutex.Unlock()
		return &Task{}, errors.New("empty task queue")
	}
	task := t.tasks[0]
	t.tasks = t.tasks[1:t.Size()]
	t.mutex.Unlock()
	return task, nil
}

func (t *Task) NewTaskState(s int) TaskState {
	state := TaskState{}
	state.MapIndex = t.mapIndex
	state.ReduceIndex = t.reduceIndex
	state.FileName = t.FileName
	state.R = t.R
	state.M = t.M

	switch s {
	case _map:
		state.State = _map
	case _reduce:
		state.State = _reduce
	}

	return state
}

// 任务队列中超时的任务
func (t *TaskQueue) TimeOut() []Task {
	var ret []Task
	t.mutex.Lock()
	for i := 0; i < t.Size(); {
		task := t.tasks[i]
		if task.TimeOut() {
			ret = append(ret, *task)
			t.tasks = append(t.tasks[:i], t.tasks[i+1:]...)
		} else {
			i++
		}
	}
	t.mutex.Unlock()
	return ret
}

func (t *TaskQueue) Remove(fileIndex, partIndex int) {
	t.mutex.Lock()
	for i := 0; i < t.Size(); {
		task := t.tasks[i]
		if fileIndex == task.mapIndex && partIndex == task.reduceIndex {
			t.tasks = append(t.tasks[:i], t.tasks[i+1:]...)
			break
		} else {
			i++
		}
	}
	t.mutex.Unlock()
}

type Coordinator struct {
	// Your definitions here.
	fileName []string

	idleMapTasks          TaskQueue
	inProgressMapTasks    TaskQueue
	idleReduceTasks       TaskQueue
	inProgressReduceTasks TaskQueue

	isDone bool
	R      int
}

// Your code here -- RPC handlers for the worker to call.

// 分配任务，让任务进入不同运行状态的队列
func (c *Coordinator) HandleTaskRequest(args *ExampleArgs, reply *TaskState) error {
	if c.isDone {
		return nil
	}

	mapTask, err := c.idleMapTasks.Dequeue()
	// 如果有空闲的map任务，分配给worker
	if err == nil {
		mapTask.SetTime()
		c.inProgressMapTasks.Enqueue(mapTask)

		// 给worker分配任务
		*reply = mapTask.NewTaskState(_map)
		fmt.Println("Submit map task...")
		return nil
	}

	reduceTask, err := c.idleReduceTasks.Dequeue()
	// 如果有空闲的reduce任务，分配给worker
	if err == nil {
		reduceTask.SetTime()
		c.inProgressReduceTasks.Enqueue(reduceTask)

		*reply = reduceTask.NewTaskState(_reduce)
		fmt.Println("Submit reduce task...")
		return nil
	}

	// 没有未分配的空闲任务，等待进行中的任务完成
	if c.inProgressMapTasks.Size() > 0 || c.inProgressReduceTasks.Size() > 0 {
		reply.State = _wait
		return nil
	}

	// 所有任务都完成，结束
	reply.State = _end
	c.isDone = true
	return nil
}

// 任务完成，将任务从相应的队列中移除
func (c *Coordinator) TaskDone(args *TaskState, reply *ExampleReply) error {
	switch args.State {
	case _map:
		fmt.Printf("map task on %vth file %v is done\n", args.MapIndex, args.FileName)
		c.inProgressMapTasks.Remove(args.MapIndex, args.ReduceIndex)
		if c.inProgressMapTasks.Size() == 0 && c.idleMapTasks.Size() == 0 {
			// 所有map任务完成，分配reduce任务
			c.distrubuteReduceTasks()
		}
	case _reduce:
		fmt.Printf("reduce task on %vth file %v is done\n", args.ReduceIndex, args.FileName)
		c.inProgressReduceTasks.Remove(args.MapIndex, args.ReduceIndex)
	default:
		fmt.Println("unknown task state")
	}
	return nil
}

func (c *Coordinator) distrubuteReduceTasks() {
	for i := 0; i < c.R; i++ {
		reduceTask := Task{
			mapIndex:    0,
			reduceIndex: i,
			FileName:    "",
			R:           c.R,
			M:           len(c.fileName),
		}

		c.idleReduceTasks.Enqueue(&reduceTask)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	return c.isDone
}

// 每隔1秒检查是否有任务超时，将超时的任务分配给空闲队列
func (c *Coordinator) findTaskTimeout() {
	for {
		// 这里有个坑点 如果设置的轮询时间过高比如5秒，最后一个crash test过不去
		// 因为最后一个测试会把worker的进程crash掉，然后coordinator检查超时太慢就会测试失败。
		time.Sleep(time.Second)
		mapTask := c.inProgressMapTasks.TimeOut()
		if len(mapTask) > 0 {
			for _, task := range mapTask {
				c.idleMapTasks.Enqueue(&task)
			}
		}
		reduceTask := c.inProgressReduceTasks.TimeOut()
		if len(reduceTask) > 0 {
			for _, task := range reduceTask {
				c.idleReduceTasks.Enqueue(&task)
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]*Task, 0)
	for i, fileName := range files {
		mapTask := &Task{
			mapIndex:    i,
			reduceIndex: 0,
			FileName:    fileName,
			R:           nReduce,
			M:           len(files),
		}
		mapTasks = append(mapTasks, mapTask)
	}

	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Printf("create mr-tmp error: %v\n", err)
			panic("create mr-tmp error")
		}
	}

	c := Coordinator{
		fileName:     files,
		idleMapTasks: TaskQueue{tasks: mapTasks},
		isDone:       false,
		R:            nReduce,
	}

	go c.findTaskTimeout()
	c.server()
	return &c
}
