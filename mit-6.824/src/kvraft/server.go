package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // {"Get", "Put", "Append"}
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store      map[string]string // underlying storage
	applyIndex int               // the lastest applied index
	applyTerm  int               // the lastest applied term
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if Debug == 1 {
		DPrintf("[server Get()] get key=%s", args.Key)
	}

	command := Op{"Get", args.Key, ""}
	curIndex, curTerm, isLeader := kv.rf.Start(command)

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "Not a leader"
		reply.Value = ""
		return
	}

	for {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		rfTerm, isStillLeader := kv.rf.GetState()
		// leadership has changed
		if rfTerm != curTerm || isStillLeader == false {
			reply.WrongLeader = true
			reply.Err = "Leader has changed"
			reply.Value = ""
			return
		}
		// leadership not change
		// current command has been applied to underlying store
		if curIndex <= kv.applyIndex && curTerm <= kv.applyTerm {
			reply.WrongLeader = false
			value, isExist := kv.store[args.Key]
			if Debug == 1 {
				DPrintf("[server Get()] SUCCESS get key=%s, isExist=%t", args.Key, isExist)
			}
			if !isExist {
				reply.Err = "NotExist"
				reply.Value = ""
				return
			}
			reply.Err = ""
			reply.Value = value
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	command := Op{args.Op, args.Key, args.Value}
	curIndex, curTerm, isLeader := kv.rf.Start(command)

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "FAIL_NOT_LEADER"
		return
	}

	for {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()

		rfTerm, isStillLeader := kv.rf.GetState()
		// leadership has changed
		if rfTerm != curTerm || isStillLeader == false {
			reply.WrongLeader = true
			reply.Err = "FAIL_LEADER_CHANGED"
			return
		}
		// leadership not change
		// current command has been applied to underlying store
		if curIndex <= kv.applyIndex && curTerm <= kv.applyTerm {
			reply.WrongLeader = false
			reply.Err = "SUCCESS_PUTAPPEND"
			if Debug == 1 {
				DPrintf("[server.go PutAppend() index_term] me=%d, curIndex=%d applyIndex=%d, curTerm=%d applyTerm=%d",
					kv.me, curIndex, kv.applyIndex, curTerm, kv.applyTerm)
			}
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	DPrintf("=================== kill KV server ===================")
	// Your code here, if desired.
}

//
// receive applied command from raft,
// and apply this command to store engine.
//
func (kv *KVServer) ReceiveAndApplyCommand() {
	for {
		appliedMsg := <-kv.applyCh

		kv.mu.Lock()

		command := appliedMsg.Command.(Op)

		if command.Type != "Get" {
			if command.Type == "Put" {
				kv.store[command.Key] = command.Value
			} else {
				if value, is_exist := kv.store[command.Key]; !is_exist {
					kv.store[command.Key] = command.Value
				} else {
					kv.store[command.Key] = value + command.Value
				}
			}
		}
		// now, the client can see the data in the storage engine
		kv.applyIndex = appliedMsg.CommandIndex
		kv.applyTerm = appliedMsg.CommandTerm
		if Debug == 1 {
			DPrintf("[server.go ReceiveAndApplyCommand()] receive a applied command from raft, me=%d, key=%s value=%s op=%s, applyIndex=%d applyTerm=%d",
				kv.me, command.Key, command.Value, command.Type, kv.applyIndex, kv.applyTerm)
		}

		kv.mu.Unlock()
	}
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
	kv.store = make(map[string]string)
	kv.applyIndex = -1
	kv.applyTerm = -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ReceiveAndApplyCommand()

	return kv
}
