package kvraft

import (
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
	id int64 // Clerk ID
	// WaitingIdx int
	lastReqID int64
	// ResultID   int64
	// ResultIdx  int
	result  execResult
	waiting int
	ch      chan execResult
	mu      sync.Mutex
	// chs map[int64]chan execResult
}

type execResult struct {
	reqID int64
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store   map[string]string
	clients map[int64]*clerkMeta
}

func (clerk *clerkMeta) add() {
	clerk.mu.Lock()
	defer clerk.mu.Unlock()

	if clerk.waiting == 0 {
		clerk.ch = make(chan execResult, 1024)
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

func (kv *KVServer) execute(op Op) (result execResult) {
	// var result ExecResult

	log.Printf("[%d] execute op : %+v\n", kv.me, op)
	result.reqID = op.ReqID
	if op.Type == OpPut {
		kv.store[op.Key] = op.Value
		result.err = OK
		return
	}

	if op.Type == OpGet {
		if val, ok := kv.store[op.Key]; !ok {
			result.err = ErrNoKey
		} else {
			result.err = OK
			result.value = val
		}
		return
	}

	result.err = OK
	kv.store[op.Key] = kv.store[op.Key] + op.Value
	log.Printf("[%d] after append, kv.store[%+v] = %+v\n", kv.me, op.Key, kv.store[op.Key])
	return
	// val, ok := kv.store[op.Key]
	// if !ok {
	// 	result.err = ErrNoKey
	// 	return
	// }

	// result.err = OK
	// if op.Type == OpGet {
	// 	result.value = val
	// 	log.Printf("[%d] after get, kv.store[%+v] = %+v\n", kv.me, op.Key, result.value)
	// 	return
	// }

	// // Append
	// kv.store[op.Key] = val + op.Value
	// log.Printf("[%d] after append, kv.store[%+v] = %+v\n", kv.me, op.Key, kv.store[op.Key])
	// return
}

func (kv *KVServer) opExecuter() {

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if kv.killed() {
				return
			}
		case msg := <-kv.applyCh:
			op := msg.Command.(Op)
			cli := kv.getClerkMeta(op.ClerkID)
			if cli.lastReqID != op.ReqID {
				cli.lastReqID = op.ReqID
				cli.result = kv.execute(op)
				log.Printf("[%d] cli.result = %+v\n", kv.me, cli.result)
			} else {
				log.Printf("[%d] repeat request, not execute again%+v\n", kv.me, op)
			}

			cli.mu.Lock()
			if cli.waiting > 0 {
				log.Printf("[%d] before sending result %d\n", kv.me, len(cli.ch))
				cli.ch <- cli.result
				log.Printf("[%d] result %+v send to client %+v\n", kv.me, cli.result, op.ClerkID)
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("[%d] Get request %+v\n", kv.me, *args)
	clerk := kv.getClerkMeta(args.ClerkID)

	clerk.add()
	defer clerk.reduce()

	op := args.toOp()
	_, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if kv.killed() {
				return
			}
			currentTerm, _ := kv.rf.GetState()
			if currentTerm != term {
				reply.Err = ErrWrongLeader
				return
			}
		case result := <-clerk.ch:
			if result.reqID != args.ReqID {
				continue
			}
			reply.Err = result.err
			reply.Value = result.value
			log.Printf("[%d] Get reply for request %+v : %+v\n", kv.me, *args, *reply)
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("[%d] PutAppend request %+v\n", kv.me, *args)
	clerk := kv.getClerkMeta(args.ClerkID)

	clerk.add()
	defer clerk.reduce()

	op := args.toOp()
	_, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if kv.killed() {
				return
			}
			currentTerm, _ := kv.rf.GetState()
			if currentTerm != term {
				reply.Err = ErrWrongLeader
				return
			}
		case result := <-clerk.ch:
			if result.reqID != args.ReqID {
				continue
			}
			reply.Err = result.err
			log.Printf("[%d] PutAppend reply for request %+v : %+v\n", kv.me, *args, *reply)
			return
		}
	}

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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.clients = make(map[int64]*clerkMeta)
	go kv.opExecuter()
	// You may need initialization code here.

	return kv
}
