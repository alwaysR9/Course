package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
	"fmt"
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
*    Because the log has been applied by kv,
*    it no need to keep them in memory,
*    we can just discard the log before the last applied by kv. 
*
*    For the leader, after discard old log, log matching may fail, 
*    when this happen, the leader need to Inatll Snapshot to follower.
*
*    Discard old log will be done in a background goroutine,
*    cause this method is much simpler than discard log when add entry to log.
*
*    Install Snapshot also will be done in a background grountine in Raft,
*    when Install Snapshot detect some follower lag behind the leader's last applied index,
*    it will Install Snapshot to those follower.
*/

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
	applyCh  chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// kv
	store    map[string]string // store engine

	// used for filtering duplicate command
	seen map[int64]int64 // map[clientID]commandID

	applyIndex int             // the lastest applied index, use it for cutting the log during snapshot
	applyTerm  int             // the lastest applied term

	isKilled bool
}

func (kv *KVServer) Log(logString string) {
	log.Printf("[KVServer] server %v applyIndex:%v applyTerm:%v seen:%v %s",
		kv.me, kv.applyIndex, kv.applyTerm, kv.seen, logString)
}

// init
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	//kv.maxraftstate = 30 * 256

	kv.DecodeSnapshot(persister.ReadSnapshot())
	if kv.store == nil {
		kv.store = make(map[string]string)
		kv.seen = make(map[int64]int64)
		kv.applyIndex = 0
		kv.applyTerm = 0
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.isKilled = false

	go kv.ReceiveAndApplyCommandLoop()
	go kv.SnapshotAndDiscardOldLogLoop()

	// set log
	log.SetFlags(log.Lshortfile)

	return kv
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

	kv.Lock()
	kv.Log(fmt.Sprintf("[Get]: argv:%v", args))
	kv.Unlock()

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
		// the server still is leader, but term changed
		if curTerm > reqTerm {
			reply.WrongLeader = true
			reply.Err = "Leader term has changed"
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

		if kv.isKilled {
			return
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

	kv.Lock()
	kv.Log(fmt.Sprintf("[Put]: argv:%v", args))
	kv.Unlock()

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
		// the server still is leader, but term changed
		if curTerm > reqTerm {
			reply.WrongLeader = true
			reply.Err = "Leader term has changed"
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

		if kv.isKilled {
			return
		}

		kv.UnlockRaftSecondly()
	}
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()

	kv.Lock()
	kv.isKilled = true
	kv.Unlock()
	log.Printf("=================== kill KV server ===================")
}

//
// receive applied command from raft,
// and apply this command to store engine.
//
func (kv *KVServer) ReceiveAndApplyCommandLoop() {
	for {
		cmtMsg := <-kv.applyCh
		kv.Lock()

		if cmtMsg.ApplySnapshot {
			kv.DecodeSnapshot(cmtMsg.Snapshot)
			kv.Log(fmt.Sprintf("[ApplyCommand BGThread]: apply snapshot"))
		} else {
			command := cmtMsg.Command.(Op)

			clientID := command.ClientID
			commandID := command.CommandID
			oldCommandID, ok := kv.seen[clientID]

			// this is an command that never been applied
			if !ok || commandID > oldCommandID {
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
				kv.Log(fmt.Sprintf("[ApplyCommand BGThread]: find duplicate command:%v", command))
			}
			// now, the client can see the data in the storage engine
			if cmtMsg.CommandIndex > kv.applyIndex {
				kv.applyIndex = cmtMsg.CommandIndex
				kv.applyTerm = cmtMsg.CommandTerm
			}
			kv.Log(fmt.Sprintf("[ApplyCommand BGThread]: apply command:%v successfully", command))
		}

		if kv.isKilled {
			return
		}

		kv.Unlock()
	}
}

func (kv *KVServer) SnapshotAndDiscardOldLogLoop() {
	// because need operate the log and kv engine,
	// so we need lock Raft and KV server
	for {
		time.Sleep(10 * time.Millisecond)
		if kv.maxraftstate < 0 {
			continue
		}

		kv.LockRaftFirstly()

		if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
			kv.Log(fmt.Sprintf("[SnapshotAndDiscardOldLogLoop BGThread]: begin to snapshot, raftStateSize:%v", kv.rf.GetRaftStateSize()))
			snapshot := kv.EncodeSnapshot()
			kv.rf.SaveSnapshotWithRaft(snapshot, kv.applyIndex)
			kv.rf.DiscardOldLog(kv.applyIndex)
			kv.Log(fmt.Sprintf("[SnapshotAndDiscardOldLogLoop BGThread]: end snapshot"))
		}

		if kv.isKilled {
			return
		}

		kv.UnlockRaftSecondly()
	}
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

func (kv *KVServer) EncodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.seen)
	e.Encode(kv.applyIndex)
	e.Encode(kv.applyTerm)
	return w.Bytes()
}

func (kv *KVServer) DecodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store  map[string]string
	var seen   map[int64]int64
	var applyIndex int
	var applyTerm  int
	if d.Decode(&store) != nil ||
		d.Decode(&seen) != nil ||
		d.Decode(&applyIndex) != nil ||
		d.Decode(&applyTerm) != nil {
		kv.Log(fmt.Sprintf("[DecodeSnapshot]: Fail ReadSnapshot"))
	} else {
		kv.store = store
		kv.seen = seen
		kv.applyIndex = applyIndex
		kv.applyTerm = applyTerm
		kv.Log(fmt.Sprintf("[DecodeSnapshot]: SUCCESS ReadSnapshot"))
	}
}