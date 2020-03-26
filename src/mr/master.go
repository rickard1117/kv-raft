package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const masterTimeout = 10 * 1000

type Master struct {
	// Your definitions here.
	ch      map[int]chan interface{} // one channel for each job
	jobs    *jobSet
	nReduce int
	nMap    int
	timeout int64
	wg      sync.WaitGroup
	mu      sync.Mutex
}

var emptyJob Job

func (m *Master) TakeJob(args *TakeJobArgs, job *Job) error {
	*job = m.jobs.fetch() // would block
	if *job == emptyJob {
		log.Println("fetch empty job")
		return nil
	}
	log.Printf("job fetch : %+v", job)

	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.ch[job.Index] == nil {
			m.ch[job.Index] = make(chan interface{}, 1)
		}
		go func(j Job, ch <-chan interface{}) {
			m.wg.Add(1)
			defer m.wg.Done()
			select {
			case <-time.After(time.Duration(m.timeout) * time.Millisecond):
				m.jobTimeout(j)
			case <-ch:
				m.jobDone(j)
			}
		}(*job, m.ch[job.Index])
	}()

	return nil
}

func (m *Master) Finish(job Job, reply *FinishReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ch[job.Index] == nil {
		reply.Succeed = false
		return nil
	}
	reply.Succeed = true
	log.Printf("going to finish job : %+v\n", job)

	m.ch[job.Index] <- struct{}{}
	close(m.ch[job.Index])
	delete(m.ch, job.Index)
	m.jobs.finish(job)
	if m.jobs.empty(MapJob) {
		m.jobs.changeToReduceSet(m.nMap, m.nReduce)
		log.Printf("changeToReduceSet job ok : %v", m.nReduce)
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// Your code here.
	return m.jobs.empty(ReduceJob)
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := NewMaster(files, max(nMap, nReduce), masterTimeout, nReduce)

	// Your code here.
	m.server()
	return m
}

func NewMaster(files []string, maxJobNum int, to int64, nreduce int) *Master {
	if maxJobNum < nreduce {
		panic("maxJobNum < nreduce")
	}
	m := Master{
		timeout: to,
		ch:      make(map[int]chan interface{}),
		jobs:    newMapJobSet(files, maxJobNum, nreduce),
		nReduce: nreduce,
		nMap:    len(files),
	}

	return &m
}

func (m *Master) jobTimeout(job Job) {
	m.jobs.reset(job)
	log.Printf("timeout job %+v succeed\n", job)
}

func (m *Master) jobDone(job Job) {
	log.Printf("jobDone job %+v succeed\n", job)

}

// WaitAll only for testing
func (m *Master) WaitAll() {
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, ch := range m.ch {
			ch <- struct{}{}
			close(ch)
		}
	}()

	m.wg.Wait()
}
