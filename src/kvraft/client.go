package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	svridx int
	id     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{Key: key, ReqID: nrand(), ClerkID: ck.id}

	for {
		var reply GetReply

		ok := ck.servers[ck.svridx].Call("KVServer.Get", args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.svridx = (ck.svridx + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}

		if reply.Err == OK {
			return reply.Value
		}

		log.Fatalf("reply.Err not OK or ErrWrongLeader : %+v, %+v", reply, *args)
	}
	panic("out of for range")
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ReqID:   nrand(),
		ClerkID: ck.id,
	}

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.svridx].Call("KVServer.PutAppend", args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.svridx = (ck.svridx + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if reply.Err == OK {
			break
		}

		log.Fatalf("reply.Err not OK or ErrWrongLeader : %+v, %+v", reply, *args)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
