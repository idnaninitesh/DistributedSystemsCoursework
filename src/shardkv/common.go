package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClerkId      int64
	OperationId  int
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	// You'll have to add definitions here.
	ClerkId      int64
	OperationId  int
	Key string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrateArgs struct {
	Shard       int
	ConfigNum   int
}

type MigrateReply struct {
	Success           bool
	Shard             int
	KVMap             map[string]string
	ClerkRequestMap   map[int64]int

}
