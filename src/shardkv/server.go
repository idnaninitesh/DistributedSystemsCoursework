package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "time"
import "fmt"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId       int64
	OperationId   int
	Operation     string
	Key           string
	Value         string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap              map[int]map[string]string
	clerkRequestMap    map[int]map[int64]int
	logEntryReplyChMap map[int]chan Op
	persister          *raft.Persister
	masterClerk        *shardmaster.Clerk
	lastConfig         shardmaster.Config
	handleShards       map[int]bool

	incomingShardCount int
	outgoingShardCount int

	isAlive            bool

}

/*
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//

func (kv *ShardKV) persist(lastIncludedIndex int) {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(kv.xxx)
	// e.Encode(kv.yyy)
	// data := w.Bytes()
	// kv.persister.SaveRaftState(data)

	kv.mu.Lock()

	currentSize := kv.persister.RaftStateSize()
	sizeThreshold := int(float64(kv.maxraftstate) * 0.85)

	if currentSize < sizeThreshold {
		kv.mu.Unlock()
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.clerkRequestMap)
	data := w.Bytes()
	kv.mu.Unlock()
	kv.rf.PersistStateAndSnapshot(data, lastIncludedIndex)
}

//
// restore previously persisted state.
//
func (kv *KVServer) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   kv.xxx = xxx
	//   kv.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var clerkRequestMap map[int64]int

	if d.Decode(&kvMap) != nil || d.Decode(&clerkRequestMap) != nil {
		// fmt.Printf("Failed to decode persistent fields\n")
	} else {
		kv.mu.Lock()
		kv.kvMap = kvMap
		kv.clerkRequestMap = clerkRequestMap
		kv.mu.Unlock()
	}
}
*/

func (kv *ShardKV) GetShardData(args *MigrateArgs, reply *MigrateReply) {

	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	lastConfigNum := kv.lastConfig.Num
	kv.mu.Unlock()
	if isLeader == false || lastConfigNum != args.ConfigNum {
		reply.Success = false
		return
	}

	kv.mu.Lock()

	kvMap := make(map[string]string)
	clerkRequestMap := make(map[int64]int)

	for k, v := range kv.kvMap[args.Shard] {
		kvMap[k] = v
	}

	for k, v := range kv.clerkRequestMap[args.Shard] {
		clerkRequestMap[k] = v
	}

	reply.KVMap = kvMap
	reply.ClerkRequestMap = clerkRequestMap
	reply.Shard = args.Shard
	reply.Success = true

	fmt.Printf("%d %d is sending shard %d %d\n", kv.gid, kv.me, args.Shard, len(reply.KVMap))
	kv.mu.Unlock()
	// fmt.Printf("Adding entry %v by %d %d\n", *args, kv.gid, kv.me)
	kv.rf.Start(*args)

}

func (kv *ShardKV) handleIncomingShards(incomingShards map[int]int, groups map[int][]string, configNum int) {

	for s, g := range incomingShards {
		go func(shard, gid int) {
			if servers, ok := groups[gid]; ok {
				args := MigrateArgs{}
				args.Shard = shard
				args.ConfigNum = configNum
				// try each server for the shard.
				// fmt.Printf("%d asked for shard %d from %d\n", kv.gid, args.Shard, gid)
				shardFound := false
				duration, _ := time.ParseDuration("50ms")
				for shardFound == false {
					for si := 0; si < len(servers); si++ {
						// kv.mu.Lock()
						srv := kv.make_end(servers[si])
						// kv.mu.Unlock()
						var reply MigrateReply
						fmt.Printf("%d asked for shard %d from %d\n", kv.gid, args.Shard, gid)
						ok := srv.Call("ShardKV.GetShardData", &args, &reply)
						if ok && reply.Success == true {
							fmt.Printf("%d received shard %d from %d\n", kv.gid, reply.Shard, gid)
							// fmt.Printf("Adding entry %v by %d %d\n", reply, kv.gid, kv.me)
							shardFound = true
							kv.rf.Start(reply)
						}
					}
					time.Sleep(duration)
				}
			}
		}(s, g)
	}

}

func (kv *ShardKV) runPollMaster() {

	duration, _ := time.ParseDuration("50ms")
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		incomingShardCount := kv.incomingShardCount
		outgoingShardCount := kv.outgoingShardCount
		kv.mu.Unlock()

		if isLeader == true && incomingShardCount == 0 && outgoingShardCount == 0 {

			kv.mu.Lock()
			lastConfigNum := kv.lastConfig.Num+1
			kv.mu.Unlock()
			newConfig := kv.masterClerk.Query(lastConfigNum)
			if newConfig.Num == lastConfigNum {
				kv.rf.Start(newConfig)
			}
		}
		time.Sleep(duration)
	}
}

func (kv *ShardKV) runMainLoop() {

	for {

		// Exit the routine if server is killed
		kv.mu.Lock()
		if kv.isAlive == false {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		// Handle applyCh for the corresponding raft server
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid == false {
				// kv.readPersist(kv.persister.ReadSnapshot())
			} else {
				// fmt.Printf("Applying message : %v %d %d\n", applyMsg.Command, kv.gid, kv.me)
				switch msg := applyMsg.Command.(type) {
				case shardmaster.Config:
					_, isLeader := kv.rf.GetState()
					kv.mu.Lock()

					// fmt.Printf("New entry added at %d for %d %d - %v\n", lastConfigNum, kv.gid, kv.me, newConfig)
					newConfig := msg
					newHandleShards := make(map[int]bool)

					for i, _ := range newConfig.Shards {
						if newConfig.Shards[i] == kv.gid {
							newHandleShards[i] = true
						}
					}

					oldConfig := kv.lastConfig
					oldHandleShards := kv.handleShards

					if isLeader == true {
						if len(oldConfig.Groups) != 0 {

							incomingShards := make(map[int]int)
							// outgoingShards := make(map[int]int)

							// incoming shards - shards in newHandleShards and not in oldHandleShards
							// count := 0
							for key, _  := range newHandleShards {
								_, ok := oldHandleShards[key]
								if !ok {
									// count++
									incomingShards[key] = oldConfig.Shards[key]
								}
							}
							// kv.incomingShardCount = count
							kv.incomingShardCount = len(incomingShards)
							kv.handleIncomingShards(incomingShards, oldConfig.Groups, newConfig.Num)

							// outgoing shards - shard in oldHandleShards and not in newHandleShards
							count := 0
							for key, _ := range oldHandleShards {
								_, ok := newHandleShards[key]
								if !ok {
									count++
									// outgoingShards[key] = newConfig.Shards[key]
								}
							}

							kv.outgoingShardCount += count
							// fmt.Printf("In - %d Out - %d for %d\n", kv.incomingShardCount, kv.outgoingShardCount, kv.gid)
							// kv.outgoingShardCount = len(outgoingShards)
							// kv.handleOutgoingShards(outgoingShards, newConfig.Groups)
						}
					}

					kv.handleShards = newHandleShards
					kv.lastConfig = newConfig
					kv.mu.Unlock()

				case MigrateArgs:
					_, isLeader := kv.rf.GetState()
					kv.mu.Lock()
					delete(kv.kvMap, msg.Shard)
					delete(kv.clerkRequestMap, msg.Shard)
					if isLeader == true {
						fmt.Printf("Shard %d deleted by %d\n", msg.Shard, kv.gid)
						kv.outgoingShardCount--
						fmt.Printf("In - %d Out - %d for %d\n", kv.incomingShardCount, kv.outgoingShardCount, kv.gid)
					}
					kv.mu.Unlock()
				case MigrateReply:
					_, isLeader := kv.rf.GetState()
					kv.mu.Lock()
					// fmt.Printf("\t\t\t\t\tApplying entry %v by %d %d\n", msg, kv.gid, kv.me)
					kv.kvMap[msg.Shard] = msg.KVMap
					kv.clerkRequestMap[msg.Shard] = msg.ClerkRequestMap
					if isLeader == true {
						fmt.Printf("Shard %v added by %d\n", msg, kv.gid)
						kv.incomingShardCount--
						fmt.Printf("In - %d Out - %d for %d\n", kv.incomingShardCount, kv.outgoingShardCount, kv.gid)
					}
					kv.mu.Unlock()
				case Op:
					kv.mu.Lock()
					operationMsg := msg
					shardIndex := key2shard(operationMsg.Key)
					lastOperationId, ok := kv.clerkRequestMap[shardIndex][operationMsg.ClerkId]
					// Check for duplicate operations
					if !ok || operationMsg.OperationId > lastOperationId {
						if operationMsg.Operation == "Get" {
							// fmt.Printf("Value for key %s is %s\n", operationMsg.Key, kv.kvMap[operationMsg.Key])
						} else if operationMsg.Operation == "Put" {
							_, ok := kv.kvMap[shardIndex]
							if !ok {
								kv.kvMap[shardIndex] = make(map[string]string)
							}
							kv.kvMap[shardIndex][operationMsg.Key] = operationMsg.Value
							// fmt.Printf("Added key %s at shard %d\n", operationMsg.Key, shardIndex)
							// fmt.Printf("Updated value for key %s is %s\n", operationMsg.Key, kv.kvMap[operationMsg.Key])
						} else {
							_, ok := kv.kvMap[shardIndex]
							if !ok {
								kv.kvMap[shardIndex] = make(map[string]string)
							}
							kv.kvMap[shardIndex][operationMsg.Key] += operationMsg.Value
							// fmt.Printf("Updated value for key %s is %s\n", operationMsg.Key, kv.kvMap[operationMsg.Key])
						}
						_, ok := kv.clerkRequestMap[shardIndex]
						if !ok {
							kv.clerkRequestMap[shardIndex] = make(map[int64]int)
						}
						kv.clerkRequestMap[shardIndex][operationMsg.ClerkId] = operationMsg.OperationId
					}

	//				if kv.maxraftstate != -1 {
	//					go kv.persist(applyMsg.CommandIndex)
	//				}

					// Send the response on waiting channel
					logEntryReplyCh, ok := kv.logEntryReplyChMap[applyMsg.CommandIndex]
					if ok {
						logEntryReplyCh <- operationMsg
					}
					kv.mu.Unlock()

				default:
					fmt.Printf("Unknown type found\n")
				}
			}
		}
	}
}

func isSame(op1 Op, op2 Op) bool {

	return op1.ClerkId == op2.ClerkId &&
		op1.OperationId == op2.OperationId &&
		op1.Operation == op2.Operation &&
		op1.Key == op2.Key &&
		op1.Value == op2.Value
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	shardIndex := key2shard(args.Key)

	kv.mu.Lock()
	// fmt.Printf("%d %d\n", kv.incomingShardCount, kv.outgoingShardCount)
	if kv.incomingShardCount != 0 || kv.outgoingShardCount != 0 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	_, ok := kv.handleShards[shardIndex]
	if !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{args.ClerkId, args.OperationId, "Get", args.Key, ""}
	startIndex, startTerm, startLeader := kv.rf.Start(op)

	// Check if server is leader or not
	if startLeader == false {
		reply.WrongLeader = true
		return
	}

	// Allocate a channel waiting on the raft response
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.logEntryReplyChMap[startIndex] = ch
	kv.mu.Unlock()
	duration, _ := time.ParseDuration("600ms")
	waitTimer := time.NewTimer(duration)
	select {
	case opReply := <-ch:
		currentTerm, _ := kv.rf.GetState()
		// Check if the current server is still a leader
		if currentTerm == startTerm && isSame(op, opReply) {
			reply.WrongLeader = false
			kv.mu.Lock()
			value, ok := kv.kvMap[shardIndex][opReply.Key]
			if ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
	case <-waitTimer.C:
		reply.WrongLeader = true
	}

	kv.mu.Lock()
	delete(kv.logEntryReplyChMap, startIndex)
	kv.mu.Unlock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	shardIndex := key2shard(args.Key)

	kv.mu.Lock()
	// fmt.Printf("%d %d\n", kv.incomingShardCount, kv.outgoingShardCount)
	if kv.incomingShardCount != 0 || kv.outgoingShardCount != 0 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	_, ok := kv.handleShards[shardIndex]
	if !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{args.ClerkId, args.OperationId, args.Op, args.Key, args.Value}
	startIndex, startTerm, startLeader := kv.rf.Start(op)

	// Check if server is leader or not
	if startLeader == false {
		reply.WrongLeader = true
		return
	}

	// Allocate a channel waiting on the raft response
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.logEntryReplyChMap[startIndex] = ch
	kv.mu.Unlock()
	duration, _ := time.ParseDuration("600ms")
	waitTimer := time.NewTimer(duration)
	select {
	case opReply := <-ch:
		currentTerm, _ := kv.rf.GetState()
		// Check if the current server is still a leader
		if currentTerm == startTerm && isSame(op, opReply) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-waitTimer.C:
		reply.WrongLeader = true
	}

	kv.mu.Lock()
	delete(kv.logEntryReplyChMap, startIndex)
	kv.mu.Unlock()

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	kv.isAlive = false
	kv.mu.Unlock()
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.kvMap = make(map[int]map[string]string)
	kv.clerkRequestMap = make(map[int]map[int64]int)
	kv.logEntryReplyChMap = make(map[int]chan Op)
	kv.persister = persister
	kv.handleShards = make(map[int]bool)

	kv.lastConfig.Num = 0
	kv.lastConfig.Groups = map[int][]string{}

	// Use something like this to talk to the shardmaster:
	kv.masterClerk = shardmaster.MakeClerk(kv.masters)

	kv.incomingShardCount = 0
	kv.outgoingShardCount = 0
	kv.isAlive = true

//	kv.readPersist(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.runPollMaster()
	go kv.runMainLoop()

	return kv
}
