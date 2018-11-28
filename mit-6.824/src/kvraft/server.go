package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

/*
* Big Picture of a kv server:
*
*   -------------             -------------
*   | kv server |--|       |--| kv server |
*   -------------  |       |  -------------
*                  |       |
*    -----------   |       |   -----------
*    |         | <--[1]    --> |         |
*    |   log   | <-----------> |   log   |
*    |         |    [2]        |         |
*    -----------               -----------
* KV server can write to it's log [1],
* and other log can also write to this log [2].
* [1] and [2] are serial write to log.(because of Lock)
*/

/*
* When a server receive a request from client,
* What the server must consider contains:
* 1. the server is not a leader.  // RPC Return WrongLeader==true
* 2. the server is a leader:
*    2.1 not leader anymore.  // RPC Return WrongLeader==true
*    2.2 still is leader, and term not changed:
*        2.2.1 leader's raft can not commit in time. (may be partitioned)  // Do not return for RPC
*        2.2.2 leader apply the cmd successfully.  // RPC Return ok==true
*    2.3 still is leader, but term changed:
*        2.3.1 cmd has been discard or overwrite.  // RPC Return WrongLeader==true --> can be optimazed
*        2.3.2 cmd has not been overwrite:
*              2.3.2.1 leader's raft can not commit in time. (may be partitioned)  // Do not return for RPC
*              2.3.2.2 leader apply the cmd successfully.  // RPC Return ok==true
*/

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
	store    map[string]string // underlying storage
	cmtIndex int               // the lastest commited index
	cmtTerm  int               // the lastest commited term
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if Debug == 1 {
		DPrintf("[server Get()] get key=%s", args.Key)
	}

	command := Op{"Get", args.Key, ""}
	reqIndex, reqTerm, isLeader := kv.rf.Start(command)

	// the server is not leader
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

		kv.rf.Lock()  // make sure raft's state unchanged

		curTerm, isStillLeader := kv.rf.GetState()
		// the server is not leader anymore
		if isStillLeader == false {
			reply.WrongLeader = true
			reply.Err = "Not a leader, anymore"
			reply.Value = ""
			kv.rf.Unlock()
			return
		}
		// the server is still leader and the term unchanged
		if curTerm == reqTerm {
			if kv.cmtIndex >= reqIndex {  // has been commited
				reply.WrongLeader = false
				reply.Err = ""
				reply.Value = kv.store[args.Key]
				kv.rf.Unlock()
				return
			} else { // has not been commited
				// pass, may be cause the RPC of client Timeout
			}
		}
		// the server still is leader, but term changed
		if curTerm > reqTerm {
			// the log entry has been discard or overwrite
			if kv.rf.GetLogLen() <= reqIndex || kv.rf.GetLogEntryAt(reqIndex).Term != reqTerm {
				reply.WrongLeader = true
				reply.Err = "Not a leader, log entry discard or overwrite"
				reply.Value = ""
				kv.rf.Unlock()
				return
			}
			// the log entry has not been overwrite
			if kv.rf.GetLogEntryAt(reqIndex).Term == reqTerm {
				if kv.cmtIndex >= reqIndex {  // has been commited
					reply.WrongLeader = false
					reply.Err = ""
					reply.Value = kv.store[args.Key]
					kv.rf.Unlock()
					return
				} else { // has not been commited
					// pass, may be cause the RPC of client Timeout
				}
			}
		}

		kv.rf.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	command := Op{args.Op, args.Key, args.Value}
	reqIndex, reqTerm, isLeader := kv.rf.Start(command)

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "Not a leader"
		return
	}

	if Debug == 1 {
		DPrintf("[Server %d PutAppend 2] receive client request: value=%s", kv.me, args.Value)
	}

	for {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()

		kv.rf.Lock()

		curTerm, isStillLeader := kv.rf.GetState()
		// the server is not leader anymore
		if isStillLeader == false {
			reply.WrongLeader = true
			reply.Err = "Not a leader, anymore"
			kv.rf.Unlock()
			return
		}
		// the server is still leader and the term unchanged
		if curTerm == reqTerm {
			if kv.cmtIndex >= reqIndex {  // has been commited
				reply.WrongLeader = false
				reply.Err = ""
				kv.rf.Unlock()
				return
			} else { // has not been commited
				// pass, may be cause the RPC of client Timeout
			}
		}
		// the server still is leader, but term changed
		if curTerm > reqTerm {
			// the log entry has been discard or overwrite
			if kv.rf.GetLogLen() <= reqIndex || kv.rf.GetLogEntryAt(reqIndex).Term != reqTerm {
				reply.WrongLeader = true
				reply.Err = "Not a leader, log entry discard or overwrite"
				kv.rf.Unlock()
				return
			}
			// the log entry has not been overwrite
			if kv.rf.GetLogEntryAt(reqIndex).Term == reqTerm {
				if kv.cmtIndex >= reqIndex {  // has been commited
					reply.WrongLeader = false
					reply.Err = ""
					kv.rf.Unlock()
					return
				} else { // has not been commited
					// pass, may be cause the RPC of client Timeout
				}
			}
		}

		kv.rf.Unlock()
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
		cmtMsg := <-kv.applyCh

		kv.mu.Lock()

		command := cmtMsg.Command.(Op)

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
		kv.cmtIndex = cmtMsg.CommandIndex
		kv.cmtTerm = cmtMsg.CommandTerm
		if Debug == 1 {
			DPrintf("[server.go ReceiveAndApplyCommand()] receive a applied command from raft, me=%d, key=%s value=%s op=%s, cmtIndex=%d cmtTerm=%d",
				kv.me, command.Key, command.Value, command.Type, kv.cmtIndex, kv.cmtTerm)
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
	kv.cmtIndex = -1
	kv.cmtTerm = -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ReceiveAndApplyCommand()

	return kv
}
