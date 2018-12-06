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
* What the server must consider:
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

/*
* Duplicate Command From Client:
*   client send command to a leader,
*   but the leader already apply this command.
*   (may be client send a command twice to a leader,
*    or leader received this command from old leader.)
* How to cope with duplicate command from same client?
*    For each command, clientID and commandID should be a part of this command.
*    Each kv server maintains a map, which contains all commands that have been applied.
*    So, when the server receive a committed command from raft:
*      1. this command has not been applied.
*      2. this command has been applied, just discard it.
*/

/*
* How to deal with raft log become too large:
*    If raft log become too large, we need do snapshot.
*
*    For the leader, after snapshot, the follower may lag too much,
*    so the leader also need to send InatllSnapshot() RPC.
*
*    All these operation will be done in a background goroutine,
*    and snapshot operation(bg goroutine) will Stop The World (STW)!
*/

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type  string // {"Get", "Put", "Append"}
	Key   string
	Value string
	ClientID  int64
	CommandID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// apply the committed command
	store    map[string]string // store engine
	applyIndex int             // the lastest applied index, use it for cutting the log during snapshot
	applyTerm  int             // the lastest applied term

	// used for filtering duplicate command
	seen map[int64]int64 // map[clientID]commandID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	command := Op{"Get", args.Key, "", args.ClientID, args.CommandID}
	reqIndex, reqTerm, isLeader := kv.rf.Start(command)

	// the server is not leader
	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "Not a leader"
		reply.Value = ""
		return
	}

	if Debug == 1 {
		log.Printf("[server:%v Get()] argv=%v\n", kv.me, args)
	}

	defer kv.UnlockRaftSecondly()

	for {
		time.Sleep(10 * time.Millisecond)

		kv.LockRaftFirstly()

		curTerm, isStillLeader := kv.rf.GetState()
		// the server is not leader anymore
		if isStillLeader == false {
			reply.WrongLeader = true
			reply.Err = "Not a leader, anymore"
			reply.Value = ""
			return
		}
		// the server is still leader and the term unchanged
		if curTerm == reqTerm {
			if kv.applyIndex >= reqIndex {  // has been commited
				reply.WrongLeader = false
				reply.Err = "SUCCESS"
				reply.Value = kv.store[args.Key]
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
				return
			}
			// the log entry has not been overwrite
			if kv.rf.GetLogEntryAt(reqIndex).Term == reqTerm {
				if kv.applyIndex >= reqIndex {  // has been commited
					reply.WrongLeader = false
					reply.Err = "SUCCESS"
					reply.Value = kv.store[args.Key]
					return
				} else { // has not been commited
					// pass, may be cause the RPC of client Timeout
				}
			}
		}

		kv.UnlockRaftSecondly()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	command := Op{args.Op, args.Key, args.Value, args.ClientID, args.CommandID}
	reqIndex, reqTerm, isLeader := kv.rf.Start(command)

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "Not a leader"
		return
	}

	if Debug == 1 {
		log.Printf("[server:%v PutAppend()] argv=%v\n", kv.me, args)
	}

	defer kv.UnlockRaftSecondly()

	for {
		time.Sleep(10 * time.Millisecond)

		kv.LockRaftFirstly()

		curTerm, isStillLeader := kv.rf.GetState()
		// the server is not leader anymore
		if isStillLeader == false {
			reply.WrongLeader = true
			reply.Err = "Not a leader, anymore"
			return
		}
		// the server is still leader and the term unchanged
		if curTerm == reqTerm {
			if kv.applyIndex >= reqIndex {  // has been commited
				reply.WrongLeader = false
				reply.Err = "SUCCESS"
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
				return
			}
			// the log entry has not been overwrite
			if kv.rf.GetLogEntryAt(reqIndex).Term == reqTerm {
				if kv.applyIndex >= reqIndex {  // has been commited
					reply.WrongLeader = false
					reply.Err = "SUCCESS"
					return
				} else { // has not been commited
					// pass, may be cause the RPC of client Timeout
				}
			}
		}

		kv.UnlockRaftSecondly()
	}
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	log.Printf("=================== kill KV server ===================")
}

//
// receive applied command from raft,
// and apply this command to store engine.
//
func (kv *KVServer) ReceiveAndApplyCommand() {
	for {
		cmtMsg := <-kv.applyCh

		kv.Lock()

		command := cmtMsg.Command.(Op)
		
		clientID := command.ClientID
		commandID := command.CommandID
		oldCommandID, ok := kv.seen[clientID]

		// this is an command that never been applied
		if !ok || commandID != oldCommandID {
			if command.Type != "Get" {
				if command.Type == "Put" {
					kv.store[command.Key] = command.Value
				} else {
					if value, ok := kv.store[command.Key]; !ok {
						kv.store[command.Key] = command.Value
					} else {
						kv.store[command.Key] = value + command.Value
					}
				}
			}
			// this command has been seen
			kv.seen[clientID] = commandID
		} else {
			log.Printf("[server:%v Apply()] Find duplicate command=%v\n", kv.me, command)
		}
		// now, the client can see the data in the storage engine
		kv.applyIndex = cmtMsg.CommandIndex
		kv.applyTerm = cmtMsg.CommandTerm
		if Debug == 1 {
			log.Printf("[server:%v Apply()] command=%v\n", kv.me, command)
		}

		kv.Unlock()
	}
}

/*
func (kv *KVServer) DoSnapshot() {
	for {
		time.Sleep(10 * time.Millisecond)
		if kv.maxraftstate < 0 {
			continue
		}

		kv.LockRaftFirstly()

		if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
			kv.rf.DoSnapshot(kv)
		}

		kv.UnlockRaftSecondly()
	}
}
*/

// init
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.store = make(map[string]string)
	kv.applyIndex = -1
	kv.applyTerm = -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.seen = make(map[int64]int64)

	go kv.ReceiveAndApplyCommand()

	// set log
	log.SetFlags(log.Lshortfile)

	return kv
}

// Have to lock raft firstly in order 
// to avoiding dead lock.
func (kv *KVServer) LockRaftFirstly() {
	kv.rf.Lock()
	kv.Lock()
}

func (kv *KVServer) UnlockRaftSecondly() {
	kv.Unlock()
	kv.rf.Unlock()
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv * KVServer) Unlock() {
	kv.mu.Unlock()
}