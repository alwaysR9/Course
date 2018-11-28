package raftkv

import (
	"crypto/rand"
	"fmt"
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

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.

	// leader ID
	leaderID int

	// client ID
	clientID int64 // rand int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// add by zfz
func (ck *Clerk) GetUniqueOpFlag() string {
	ts := time.Now().Unix()
	return fmt.Sprintf("%d%d", ck.clientID, ts)
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
	// You will have to modify this function.
	args := GetArgs{key}
	reply := GetReply{false, "", ""}

	if Debug == 1 {
		DPrintf("[client PutAppend()] get key=%s", key)
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
		DPrintf("[client Get()] SUCCESS get key=%s, value=%s", key, reply.Value)
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
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op}
	reply := PutAppendReply{false, ""}

	if Debug == 1 {
		DPrintf("[client PutAppend() 1] %s key=%s, value=%s, current leaderID=%d, reply=%t", op, key, value, ck.leaderID, reply.WrongLeader)
	}

	if ck.leaderID != -1 {
		if ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.WrongLeader == true {
				ck.leaderID = -1
			} else {
				if Debug == 1 {
					DPrintf("[client PutAppend()] SUCCESS leaderID=%d, already find leader, key=%s, value=%s",
						ck.leaderID, key, value)
				}
				return // PutAppend() success
			}
		} else {
			// RPC Timeout: try other servers
			ck.leaderID = -1
			if Debug == 1 {
				DPrintf("[client PutAppend() x] %s key=%s, value=%s, current leaderID=%d, reply=%t, timeout!", op, key, value, ck.leaderID, reply.WrongLeader)
			}
		}
	}

	if Debug == 1 {
		DPrintf("[client PutAppend() 2] %s key=%s, value=%s, current leaderID=%d, reply=%t", op, key, value, ck.leaderID, reply.WrongLeader)
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
				if Debug == 1 {
					DPrintf("[client PutAppend()] SUCCESS leaderID=%d, find a new leader, key=%s, value=%s",
						ck.leaderID, key, value)
				}
				break
			} else {
				// RPC Timeout: try other servers
				ck.leaderID = -1
				if Debug == 1 {
					DPrintf("[client PutAppend() xx] %s key=%s, value=%s, current leaderID=%d, reply=%t, timeout!", op, key, value, ck.leaderID, reply.WrongLeader)
				}
			}
		}
	}

	if Debug == 1 {
		DPrintf("[client PutAppend() 3] %s key=%s, value=%s, current leaderID=%d, reply=%t", op, key, value, ck.leaderID, reply.WrongLeader)
	}

	return // PutAppend() success
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
