package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

/*
* What the client must consider contains:
* 1. client request can not reach the server.  // RPC Timeout labrpc.go:line 218
* 2. the server is not a leader.  // RPC Return WrongLeader==true
* 3. the server is a leader:
*    3.1.1 leader has shutdown before apply the cmd.  // RPC Timeout, labrpc.go:line 262
*    3.1.3 leader state changed before apply the cmd.  // RPC Return WrongLeader==true
*    3.1.3 leader's raft can not commit in time. (may be partitioned)  // RPC Timeout, labrpc.go:line[280~293]
*    3.1.4 leader apply the cmd successfully.  // RPC Return ok==true
*/

/*
* How client identification duplicate command:
*    (clientID, commandID) pair
*/

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	// leader ID
	leaderID int

	// client ID
	clientID int64 // rand int64
}

func (ck *Clerk) GetCommandID() int64 {
	return time.Now().UnixNano()
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
	ck.leaderID = -1
	ck.clientID = nrand()
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
	commandID := nrand()
	args := GetArgs{key, ck.clientID, commandID}
	reply := GetReply{false, "", ""}

	if Debug == 1 {
		DPrintf("[clientID:%v Get()] argv=%v\n", ck.clientID, args)
	}

	if ck.leaderID != -1 {
		if ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply); ok {
			if reply.WrongLeader == true {
				ck.leaderID = -1
			} else {
				if reply.Err == "NotExist" {
					return ""		
				}
				return reply.Value
			}
		} else {
			// RPC Timeout: try other servers
			ck.leaderID = -1
		}
	}

	for {
		if ck.leaderID != -1 {
			break
		}
		for i := 0; i < len(ck.servers); i++ {
			reply = GetReply{false, "", ""}
			if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok {
				if reply.WrongLeader == true {
					continue
				}
				ck.leaderID = i
				break
			} else {
				// RPC Timeout: try other servers
				ck.leaderID = -1
			}
		}
	}

	if Debug == 1 {
		DPrintf("[clientID:%v Get()] SUCCESS argv=%v, reply=%v\n", ck.clientID, args, reply)
	}

	if reply.Err == "NotExist" {
		return ""		
	}
	return reply.Value
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
	commandID := nrand()
	args := PutAppendArgs{key, value, op, ck.clientID, commandID}
	reply := PutAppendReply{false, ""}

	if Debug == 1 {
		DPrintf("[clientID:%v PutAppend()] argv=%v\n", ck.clientID, args)
	}

	if ck.leaderID != -1 {
		if ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.WrongLeader == true {
				ck.leaderID = -1
			} else {
				return // PutAppend() success
			}
		} else {
			// RPC Timeout: try other servers
			ck.leaderID = -1
		}
	}

	for {
		if ck.leaderID != -1 {
			break
		}
		for i := 0; i < len(ck.servers); i++ {
			reply = PutAppendReply{false, ""}
			if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok {
				if reply.WrongLeader == true {
					continue
				}
				ck.leaderID = i
				break
			} else {
				// RPC Timeout: try other servers
				ck.leaderID = -1
			}
		}
	}

	if Debug == 1 {
		DPrintf("[clientID:%v PutAppend()] SUCCESS argv=%v, reply=%v\n", ck.clientID, args, reply)
	}

	return // PutAppend() success
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
