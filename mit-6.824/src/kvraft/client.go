package raftkv

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"time"
)

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
	reply := GetReply{}

	if Debug == 1 {
		DPrintf("[client PutAppend()] get key=%s", key)
	}

	if ck.leaderID != -1 {
		if ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply); ok {
			if reply.WrongLeader == true {
				ck.leaderID = -1
			}
		}
	}

	for {
		if ck.leaderID != -1 {
			break
		}
		for i := 0; i < len(ck.servers); i++ {
			reply = GetReply{}
			if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok {
				if reply.WrongLeader == true {
					continue
				}
				ck.leaderID = i
				break
			}
		}
	}

	if Debug == 1 {
		DPrintf("[client Get()] SUCCESS get key=%s, value=%s", key, reply.Value)
	}

	if reply.Err != "NotExist" {
		return reply.Value
	}
	return ""
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
	reply := PutAppendReply{}

	if Debug == 1 {
		DPrintf("[client PutAppend()] %s key=%s, value=%s", op, key, value)
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
			}
		}
	}

	if Debug == 1 {
		DPrintf("[client PutAppend()] SUCCESS leaderID=%d, key=%s, value=%s",
			ck.leaderID, key, value)
	}
	return // PutAppend() success
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
