package mr

import (
	"sync"
)

type set map[string]bool

type jobSet struct {
	stype    JobType // 默认是Map类型
	ch       chan Job
	total    int //  job总数
	pending  int // 待执行job数
	finished int // 已经执行完的Job数
	mu       sync.Mutex
}

func newMapJobSet(mapFileNames []string, capacity int, nreduce int) *jobSet {
	js := jobSet{
		ch:       make(chan Job, capacity),
		stype:    MapJob,
		total:    len(mapFileNames),
		pending:  len(mapFileNames),
		finished: 0,
	}
	for i, name := range mapFileNames {
		js.ch <- Job{Type: MapJob, MapFileName: name, Index: i, NMap: len(mapFileNames), NReduce: nreduce}
	}
	return &js
}

func (js *jobSet) changeToReduceSet(nmap int, nreduce int) {
	js.mu.Lock()
	defer js.mu.Unlock()

	js.stype = ReduceJob
	js.total = nreduce
	js.pending = nreduce
	js.finished = 0

	for i := 0; i < nreduce; i++ {
		js.ch <- Job{Type: ReduceJob, Index: i, NReduce: nreduce, NMap: nmap}
	}
}

func (js *jobSet) empty(t JobType) bool {
	js.mu.Lock()
	defer js.mu.Unlock()
	return js.finished == js.total && js.stype == t || js.total == 0
}

func (js *jobSet) fetch() Job {
	if js.empty(ReduceJob) {
		return emptyJob
	}
	job, ok := <-js.ch
	if !ok {
		return emptyJob
	}
	func() {
		js.mu.Lock()
		defer js.mu.Unlock()
		js.pending--
	}()

	return job
}

func (js *jobSet) reset(job Job) {
	func() {
		js.mu.Lock()
		defer js.mu.Unlock()
		js.pending++
	}()

	js.ch <- job
}

func (js *jobSet) finish(job Job) {
	js.mu.Lock()
	defer js.mu.Unlock()

	js.finished++
	if js.finished == js.total && js.stype == ReduceJob {
		close(js.ch)
	}
}
