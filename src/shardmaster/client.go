package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

// import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader      int
	clerkId     int64
	operationId int
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
	// Your code here.
	ck.leader = 0
	ck.clerkId = nrand()
	ck.operationId = 1

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.OperationId = ck.operationId
	ck.operationId++
	leader := ck.leader
	args.Num = num
	serversLen := len(ck.servers)

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply QueryReply
			ok := ck.servers[(i+leader)%serversLen].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = (i + leader) % serversLen
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.OperationId = ck.operationId
	ck.operationId++
	leader := ck.leader
	args.Servers = servers
	serversLen := len(ck.servers)

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply JoinReply
			ok := ck.servers[(i+leader)%serversLen].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = (i + leader) % serversLen
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.OperationId = ck.operationId
	ck.operationId++
	leader := ck.leader
	args.GIDs = gids
	serversLen := len(ck.servers)

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply LeaveReply
			ok := ck.servers[(i+leader)%serversLen].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = (i + leader) % serversLen
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.OperationId = ck.operationId
	ck.operationId++
	leader := ck.leader
	args.Shard = shard
	args.GID = gid
	serversLen := len(ck.servers)

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply MoveReply
			ok := ck.servers[(i+leader)%serversLen].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = (i + leader) % serversLen
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
