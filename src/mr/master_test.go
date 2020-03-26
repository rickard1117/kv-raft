package mr

import (
	"testing"
	"time"
)

func TestMasterNoJobs(t *testing.T) {
	files := []string{}
	m := NewMaster(files, 3, 3000, 3)

	request := TakeJobArgs{}
	reply := Job{}

	_ = m.TakeJob(&request, &reply)
	if reply.Type != ShutdownJob {
		t.Fatal("reply.Type != ShutdownJob")
	}
}

func TestMasterOneJob(t *testing.T) {
	files := []string{"a.txt"}
	m := NewMaster(files, 1, 3000, 1)

	request := TakeJobArgs{}
	reply := Job{}
	_ = m.TakeJob(&request, &reply)

	j := Job{Type: MapJob, MapFileName: "a.txt", Index: 0, NMap: 1, NReduce: 1}
	if reply != j {
		t.Fatalf("TakeJob error : %+v", reply)
	}
	m.WaitAll()
}

func TestReTakeTheTimeoutJob(t *testing.T) {
	files := []string{"a1.txt"}
	m := NewMaster(files, 1, 10, 1)

	request := TakeJobArgs{}
	reply := Job{}
	m.TakeJob(&request, &reply)
	time.Sleep(100 * time.Millisecond)

	m.TakeJob(&request, &reply)
	j := Job{Type: MapJob, MapFileName: "a1.txt", Index: 0, NMap: 1, NReduce: 1}
	if reply != j {
		t.Fatalf("not re take the same job %+v", reply)
	}

	m.WaitAll()
}

func TestFinishAndTimeoutJob(t *testing.T) {
	files := []string{"a2.txt", "b2.txt", "c2.txt"}
	request := TakeJobArgs{}
	var job1, job2, job3 Job

	m := NewMaster(files, 3, 500, 3)

	// taking all jobs
	m.TakeJob(&request, &job1)
	m.TakeJob(&request, &job2)
	m.TakeJob(&request, &job3)

	// but only 2 jobs done
	reply := FinishReply{}
	m.Finish(job1, &reply)
	if !reply.Succeed {
		t.Fatalf("finish job failed : %+v", job1)
	}
	m.Finish(job2, &reply)
	if !reply.Succeed {
		t.Fatalf("finish job failed : %+v", job2)
	}
	time.Sleep(600 * time.Millisecond)
	m.TakeJob(&request, &job3)
	j := Job{MapFileName: "c2.txt", Type: MapJob, Index: 2, NMap: 3, NReduce: 3}
	if job3 != j {
		t.Fatalf("TestFinishAndTimeoutJob failed : %+v", job3)
	}
	m.WaitAll()
}

func TestFinishSameJob(t *testing.T) {
	files := []string{"a2.txt", "b2.txt", "c2.txt"}
	request := TakeJobArgs{}
	m := NewMaster(files, 3, 500, 3)

	job := Job{}
	j := Job{Type: MapJob, MapFileName: "a2.txt", Index: 0, NMap: 3, NReduce: 3}
	m.TakeJob(&request, &job)
	if job != j {
		t.Fatalf("TakeJob err : %+v", job)
	}
	reply := FinishReply{}
	m.Finish(job, &reply)
	if !reply.Succeed {
		t.Fatalf("finish job %+v failed", job)
	}
	m.Finish(job, &reply)
	if reply.Succeed {
		t.Fatalf("re finish job %+v should failed", job)
	}

}

func TestTakeMultipleMapJob(t *testing.T) {
	files := []string{"a3.txt", "b3.txt", "c3.txt"}
	request := TakeJobArgs{}
	reply := Job{}
	m := NewMaster(files, 3, 999*1000, 3)

	jobs := map[string]bool{}
	for range files {
		m.TakeJob(&request, &reply)
		jobs[reply.MapFileName] = true
	}

	if !jobs["a3.txt"] || !jobs["b3.txt"] || !jobs["c3.txt"] {
		t.Fatal("TestTakeMultipleMapJob Error")
	}

	m.WaitAll()
}

func TestRedoOneTimeoutJob(t *testing.T) {
	files := []string{"a4.txt"}
	request := TakeJobArgs{}
	reply := Job{}
	m := NewMaster(files, 1, 10, 1)
	m.TakeJob(&request, &reply)
	if reply.Type != MapJob {
		t.Fatal("TakeJob Error")
	}
	time.Sleep(20 * time.Millisecond)

	m.TakeJob(&request, &reply)
	if reply.Type != MapJob {
		t.Fatal("TakeJob Error")
	}
	time.Sleep(20 * time.Millisecond)

	m.TakeJob(&request, &reply)
	if reply.Type != MapJob {
		t.Fatal("TakeJob Error")
	}

	m.WaitAll()
}

func TestRedoMultipleTimeoutJobs(t *testing.T) {
	files := []string{"a5.txt", "b5.txt", "c5.txt"}
	request := TakeJobArgs{}
	reply := Job{}
	m := NewMaster(files, 3, 10, 3)

	for range files {
		m.TakeJob(&request, &reply)
		if reply.Type != MapJob {
			t.Fatal("TakeJob Error")
		}
	}
	time.Sleep(20 * time.Millisecond)

	for range files {
		m.TakeJob(&request, &reply)
		if reply.Type != MapJob {
			t.Fatal("TakeJob Error")
		}
	}

	time.Sleep(20 * time.Millisecond)
	for range files {
		m.TakeJob(&request, &reply)
		if reply.Type != MapJob {
			t.Fatal("TakeJob Error")
		}
	}

	m.WaitAll()
}

func TestSwitchToReduceJob(t *testing.T) {
	files := []string{"a6.txt"}
	request := TakeJobArgs{}
	job := Job{}
	m := NewMaster(files, 5, 3000, 3)
	m.TakeJob(&request, &job)
	f := FinishReply{}
	m.Finish(job, &f)
	if !f.Succeed {
		t.Fatalf("finish job error %+v", job)
	}
	var job2 Job
	m.TakeJob(&request, &job2)
	if job2.Type != ReduceJob || job2.Index != 0 || job2.NReduce != 3 || job2.NMap != 1 {
		t.Fatalf("wrong result of TakeJob : %+v", job)
	}
	m.WaitAll()
}
