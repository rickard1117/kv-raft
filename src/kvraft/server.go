package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	OpPut    = "OpPut"
	OpAppend = "OpAppend"
	OpGet    = "OpGet"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key     string
	Value   string
	Type    OpType
	ReqID   int64 // ID for client request
	ClerkID int64
}

type clerkMeta struct {
	id        int64 // Clerk ID
	lastReqID int64
	result    ExecResult
	waiting   int
	ch        chan ExecResult
	mu        sync.Mutex
}

type ExecResult struct {
	ReqID int64
	Err   Err
	Value string
}

type KVServer struct {
	mu        sync.Mutex
	storelock sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxraftstate  int // snapshot if log grows this big
	lastExecIndex int
	// Your definitions here.
	store   map[string]string
	clients map[int64]*clerkMeta
}

type SnapshotClient struct {
	LastReqID int64
	Result    ExecResult
}

type Snapshot struct {
	Store   map[string]string
	Clients map[int64]SnapshotClient
}

func (clerk *clerkMeta) add() {
	clerk.mu.Lock()
	defer clerk.mu.Unlock()

	if clerk.waiting == 0 {
		clerk.ch = make(chan ExecResult, 1024)
	}
	clerk.waiting++
}

func (clerk *clerkMeta) reduce() {
	clerk.mu.Lock()
	defer clerk.mu.Unlock()

	clerk.waiting--
	if clerk.waiting == 0 {
		clerk.ch = nil
	} else if clerk.waiting < 0 {
		log.Fatalf("clerk.waiting : %d\n", clerk.waiting)
	}
}

func (kv *KVServer) execute(op Op) (result ExecResult) {
	// var result ExecResult

	log.Printf("[%d] execute op : %+v\n", kv.me, op)
	result.ReqID = op.ReqID

	if op.Type == OpPut {
		kv.store[op.Key] = op.Value
		result.Err = OK
		return
	}

	if op.Type == OpGet {
		if val, ok := kv.store[op.Key]; !ok {
			result.Err = ErrNoKey
		} else {
			result.Err = OK
			result.Value = val
		}
		return
	}

	result.Err = OK
	kv.store[op.Key] = kv.store[op.Key] + op.Value
	log.Printf("[%d] after append, kv.store[%+v] = %+v\n", kv.me, op.Key, kv.store[op.Key])
	return
}

func (kv *KVServer) snapshotHandler() {
	for {
		time.Sleep(30 * time.Millisecond)
		if kv.killed() {
			log.Printf("[%d] exit snapshotHandler\n", kv.me)
			return
		}
		if kv.maxraftstate == -1 {
			continue
		}

		if kv.rf.StateSize() >= kv.maxraftstate {
			kv.save()
		}
	}
}

func (kv *KVServer) save() {
	kv.mu.Lock()
	kv.storelock.Lock()
	s := Snapshot{Store: kv.store, Clients: make(map[int64]SnapshotClient)}
	for k, v := range kv.clients {
		s.Clients[k] = SnapshotClient{LastReqID: v.lastReqID, Result: v.result}
	}
	idx := kv.lastExecIndex
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(s)
	kv.storelock.Unlock()
	kv.mu.Unlock()

	kv.rf.SaveSnapshot(buf.Bytes(), idx)
}

func (kv *KVServer) replaySnapshot(store interface{}, lastIndex int) {
	kv.mu.Lock()
	kv.storelock.Lock()
	defer kv.storelock.Unlock()
	defer kv.mu.Unlock()

	d := labgob.NewDecoder(bytes.NewBuffer(store.([]byte)))
	var s Snapshot
	if err := d.Decode(&s); err != nil {
		log.Fatalf("[%d] Decode error : %+v\n", kv.me, err)
	}

	kv.store = s.Store
	kv.lastExecIndex = lastIndex
	kv.clients = make(map[int64]*clerkMeta)
	for k, v := range s.Clients {
		if kv.clients[k] == nil {
			kv.clients[k] = &clerkMeta{id: k}
		}
		kv.clients[k].lastReqID = v.LastReqID
		kv.clients[k].result = v.Result
	}
}

func (kv *KVServer) opExecuter() {
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if kv.killed() {
				log.Printf("[%d] exit opExecuter\n", kv.me)
				return
			}
		case msg := <-kv.applyCh:
			if !msg.CommandValid && msg.CommandType == raft.CommandSnapshot {

				kv.replaySnapshot(msg.Command, msg.CommandIndex)
				continue

			}

			op := msg.Command.(Op)
			cli := kv.getClerkMeta(op.ClerkID)

			kv.storelock.Lock()
			kv.lastExecIndex = msg.CommandIndex
			if cli.lastReqID != op.ReqID {
				log.Printf("[%d] going to execute command %+v, last ReqID = %d\n", kv.me, msg, cli.lastReqID)
				cli.lastReqID = op.ReqID
				cli.result = kv.execute(op)
			} else {
				log.Printf("[%d] repeated request %d\n", kv.me, op.ReqID)
			}
			kv.storelock.Unlock()

			cli.mu.Lock()
			if cli.waiting > 0 {
				cli.ch <- cli.result
			}
			cli.mu.Unlock()
		}
	}

}

func (kv *KVServer) getClerkMeta(id int64) *clerkMeta {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.clients[id] == nil {
		kv.clients[id] = &clerkMeta{id: id}
	}
	return kv.clients[id]
}

func (kv *KVServer) dealRPC(clerkid int64, op Op) (result ExecResult) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		result.Err = ErrWrongLeader
		return
	}
	log.Printf("[%d] request coming : %+v\n", kv.me, op)
	clerk := kv.getClerkMeta(clerkid)

	clerk.add()
	defer clerk.reduce()

	_, term, isleader := kv.rf.Start(op)
	if !isleader {
		result.Err = ErrWrongLeader
		return
	}

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if kv.killed() {
				result.Err = ErrServiceKilled
				log.Printf("[%d] service killed\n", kv.me)
				return
			}
			currentTerm, _ := kv.rf.GetState()
			if currentTerm != term {
				result.Err = ErrWrongLeader
				return
			}
		case res := <-clerk.ch:
			if res.ReqID != op.ReqID {
				continue
			}
			result.Err = res.Err
			result.Value = res.Value
			log.Printf("[%d] reply for request %+v : %+v\n", kv.me, op, res)
			return
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	result := kv.dealRPC(args.ClerkID, args.toOp())
	if result.Err == ErrServiceKilled {
		return
	}
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	result := kv.dealRPC(args.ClerkID, args.toOp())
	if result.Err == ErrServiceKilled {
		return
	}
	reply.Err = result.Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	log.Printf("[%d] KVService is going to shutdown\n", kv.me)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	log.Printf("[%d] StartKVServer!!!\n", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 100)

	kv.store = make(map[string]string)
	kv.clients = make(map[int64]*clerkMeta)
	go kv.opExecuter()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.snapshotHandler()
	// You may need initialization code here.

	return kv
}
