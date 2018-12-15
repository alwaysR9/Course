package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"log"
	"fmt"
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

	// client ID
	clientID int64 // rand int64

	// leader ID
	leaderID int

	// command ID
	commandID int64 // self increasing
}

func (ck *Clerk) Log(logString string) {
	log.Printf("[KVClient] clientID:%v leaderID:%v %s",
		ck.clientID, ck.leaderID, logString)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) GetCommandID() int64 {
	r := ck.commandID
	ck.commandID ++
	return r
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderID = -1
	ck.clientID = nrand()
	ck.commandID = 0
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
	commandID := ck.GetCommandID()
	args := GetArgs{key, ck.clientID, commandID}
	reply := GetReply{false, "", ""}

	ck.Log(fmt.Sprintf("[Get]: begin to get argv:%v", args))

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

	ck.Log(fmt.Sprintf("[Get]: get argv:%v reply:%v successfully", args, reply))

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
	commandID := ck.GetCommandID()
	args := PutAppendArgs{key, value, op, ck.clientID, commandID}
	reply := PutAppendReply{false, ""}

	ck.Log(fmt.Sprintf("[PutAppend]: begin to put argv:%v", args))

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

	ck.Log(fmt.Sprintf("[PutAppend]: put argv:%v reply:%v successfully", args, reply))

	return // PutAppend() success
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
