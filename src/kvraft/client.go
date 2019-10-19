package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
// import "fmt"

type Clerk struct {
	servers       []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader        int
	clerkId       int64
	operationId   int
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
	// You'll have to add code here.
	ck.leader = 0
	ck.clerkId = nrand()
	// fmt.Printf("Generated new clerk with clerk id %d\n", ck.clerkId)
	ck.operationId = 1

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
	args := GetArgs{ck.clerkId, ck.operationId, key}

	ck.operationId++
	leader := ck.leader
	serversLen := len(ck.servers)

	duration, _ := time.ParseDuration("50ms")
	for {
		for i, _ := range(ck.servers) {
			var reply GetReply
			ok := ck.servers[(i+leader)%serversLen].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.WrongLeader == false {
					ck.leader = (i+leader)%serversLen
					if reply.Err == ErrNoKey {
						reply.Value = ""
					}
					return reply.Value
				}
			}
			time.Sleep(duration)
		}
	}
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

	args := PutAppendArgs{ck.clerkId, ck.operationId, key, value, op}

	ck.operationId++
	leader := ck.leader
	serversLen := len(ck.servers)

	duration, _ := time.ParseDuration("50ms")
	for {
		for i, _ := range(ck.servers) {
			var reply PutAppendReply
			ok := ck.servers[(i+leader)%serversLen].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.WrongLeader == false {
					ck.leader = (i+leader)%serversLen
					return
				}
			}
			time.Sleep(duration)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
