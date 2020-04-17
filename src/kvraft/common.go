package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrServiceKilled = "ErrServiceKilled"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqID   int64
	ClerkID int64
}

func (args *PutAppendArgs) toOp() Op {
	op := Op{
		Key:     args.Key,
		Value:   args.Value,
		ReqID:   args.ReqID,
		ClerkID: args.ClerkID,
	}

	if args.Op == "Put" {
		op.Type = OpPut
	} else {
		op.Type = OpAppend
	}
	return op
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ReqID   int64
	ClerkID int64
}

func (args *GetArgs) toOp() Op {
	return Op{
		Key:     args.Key,
		Type:    OpGet,
		ReqID:   args.ReqID,
		ClerkID: args.ClerkID,
	}
}

type GetReply struct {
	Err   Err
	Value string
}
