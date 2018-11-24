package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "time"
import "sort"
import "math/rand"

import "bytes"
import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

// TODO 2A zfz
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// TODO 2A zfz
	currentTerm int
	voteFor     int // -1 if not vote in currentTerm
	log         []LogEntry
	state       string // {"follower", "candidate", "leader"}

	elecTimeout    int // init when state turn to follower
	elecBeginTime  int // init when state turn to follower
	heartBeginTime int // init when state turn to leader

	commitIndex int // volatile when state changed
	lastApplied int // can not volatile when state changed

	nextIndex  []int // valid just for leader
	matchIndex []int // valid just for leader

	heartInterval   int // 100ms
	lowElecInterval int // 1000ms

	is_killed bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// TODO zfz
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// TODO by zfz
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("server %v: SUCCESS persist()", rf.me)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// TODO by zfz
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var state string
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&state) != nil {
		//DPrintf("server %v: Fail readPersist()", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.state = state
		//DPrintf("server %v: SUCCESS readPersist()", rf.me)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	DPrintf("server %v: (follower) receive requestVote() from server %v, term: %v/%v, my voteFor: %v; log len: %v/%v, log last term: %v/%v",
		rf.me, args.CandidateID, rf.currentTerm, args.Term, rf.voteFor, len(rf.log), args.LastLogIndex+1, rf.log[len(rf.log)-1].Term, args.LastLogTerm)

	is_should_persist := false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if is_should_persist == true {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.TurnToFollower(args.Term)
		is_should_persist = true
	}

	if rf.voteFor != -1 { // args.Term == rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
			args.LastLogIndex >= len(rf.log)-1 {

		reply.VoteGranted = true
		rf.voteFor = args.CandidateID

		is_should_persist = true
		DPrintf("server %v: (follower) return requestVote() from server %v SUCCESS, request.Term/Term: %v/%v",
			rf.me, args.CandidateID, args.Term, rf.currentTerm)
		return
	}
	reply.VoteGranted = false
	return
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool

	// if Success==false,
	// these are the first enrty's index and term
	// the same term with entry that do not match
	FirstIndex int
	FirstTerm  int
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	is_should_persist := false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if is_should_persist == true {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term || rf.state == "candidate" {
		rf.TurnToFollower(args.Term)
		is_should_persist = true
	}

	if rf.state == "leader" {
		DPrintf("Exception find two leader")
		reply.Success = false
		return
	}

	// reset election timeout
	rf.ResetElecTimeout()

	if len(rf.log)-1 < args.PrevLogIndex { // len(follow's log) < len(leader's log)
		DPrintf("server %v: receive heartbeat from %v, but i am too far away from leader's log, log len: %v/%v",
			rf.me, args.LeaderID, len(rf.log)-1, args.PrevLogIndex)

		// optimization when log do not match
		reply.FirstIndex = len(rf.log) // FirstIndex >= 1
		reply.FirstTerm = -1

		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // len(follwer's log) >= len(leader's log)

		// optimization when log do not match
		// find first log entry in follow that
		// has the same Term with log entry do not matched
		firstIndex := args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				firstIndex = i + 1
				break
			}
		}
		reply.FirstIndex = firstIndex // FirstIndex: [1, args.PrevLogIndex]
		reply.FirstTerm = rf.log[firstIndex].Term

		reply.Success = false
		return
	}

	// log match success
	// copy leader's log to follow's log
	if len(args.Entries) == 0 {
		reply.Success = true
		rf.commitIndex = args.LeaderCommit
		return
	}

	leaderLogLen := args.PrevLogIndex + 1 + len(args.Entries)

	// network is unstable, package can reordered or repeated
	// so old message should not cover new message
	if len(rf.log) > args.PrevLogIndex+1 &&
		rf.log[args.PrevLogIndex+1].Term == args.Entries[0].Term &&
		leaderLogLen <= len(rf.log) {

		DPrintf("-------------------------")
		DPrintf("server %d: SUCCESS heartbeat from leader %v", rf.me, args.LeaderID)
		DPrintf("server %d: But, this appendEntries() request is not up to date, leader PrevLogIndex:%v, Term:%v/%v, leaderLen/curLogLen=%v/%v, so drop it",
			rf.me, args.PrevLogIndex, rf.log[args.PrevLogIndex+1].Term, args.Entries[0].Term, leaderLogLen, len(rf.log))

		DPrintf("server %v: log entry num %v, log entry's last term %v", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
		DPrintf("-------------------------")

		reply.Success = true
		return
	}

	// now this is a new message, just do log copying
	DPrintf("-------------------------")
	if len(args.Entries) > 0 {
		is_should_persist = true
	}

	if leaderLogLen < len(rf.log) {
		rf.log = append(rf.log[:leaderLogLen])
		DPrintf("server %v: SUCCESS cut my log", rf.me)
	}

	for i, e := range args.Entries {
		i_next := args.PrevLogIndex + 1 + i
		if i_next < len(rf.log) {
			rf.log[i_next].Term = e.Term
			rf.log[i_next].Command = e.Command
		} else {
			rf.log = append(rf.log, LogEntry{e.Term, e.Command})
		}
	}

	DPrintf("server %d: SUCCESS heartbeat from server %v", rf.me, args.LeaderID)
	DPrintf("server %v: log entry num %v/%v, log entry's last term %v/%v",
		rf.me, len(rf.log), leaderLogLen, rf.log[len(rf.log)-1].Term, args.Entries[len(args.Entries)-1].Term)
	DPrintf("-------------------------")

	// set commitIndex
	rf.commitIndex = args.LeaderCommit

	reply.Success = true
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// TODO zfz
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return index, term, false
	}

	// add to log
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.matchIndex[rf.me] = len(rf.log) - 1

	// start sending log to followers,
	// by setting heartbeat timeout
	rf.ResetHeartbeatTimeout(0)

	index = len(rf.log) - 1
	term = rf.currentTerm
	isLeader = true

	rf.persist()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// TODO zfz
	rf.mu.Lock()
	rf.is_killed = true
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// TODO zfz
	rf.is_killed = false

	if len(rf.state) == 0 {
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.log = append(rf.log, LogEntry{0, ""})
		rf.state = "follower"
	}

	if rf.state == "leader" {
		rf.InitFollowerLog()
	}

	rf.commitIndex = 0 // term 0 has been submitted, update to 0 when state change
	rf.lastApplied = 0 // term 0 has been applied, do not update when state change

	rf.heartInterval = 150
	rf.lowElecInterval = 1000

	rand.Seed(int64(Now()))

	go rf.ElecLoop()
	go rf.AppendEntriesLoop()
	go rf.UpdateCommitIndexLoop(applyCh)

	return rf
}

// TODO zfz
func (rf *Raft) ElecLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetElecTimeout()
	n_vote_for_me := 0

	for {
		if rf.is_killed == true {
			DPrintf("server %d: (%s term %d) be killed",
				rf.me, rf.state, rf.currentTerm)
			break
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		elapse := Now() - rf.elecBeginTime

		if rf.state == "leader" {
			continue
		}

		if elapse >= rf.elecTimeout {
			// go into next term
			// state become candidate
			// kick off new leader election, add n_vote_for_me if success
			rf.TurnToCandidate()
			n_vote_for_me = 1
			rf.persist()

			DPrintf("server %d: (%s term %d) need request vote, timeout %d, elapse %d",
				rf.me, rf.state, rf.currentTerm, rf.elecTimeout, elapse)

			paramTerm := rf.currentTerm
			paramLastLogIndex := len(rf.log) - 1
			paramLastLogTerm := rf.log[paramLastLogIndex].Term

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i_peer, sendTerm, sendLastLogIndex, sendLastLogTerm int) { // send VoteRequests in Parallel
					rf.mu.Lock()
					defer rf.mu.Unlock()
					req := RequestVoteArgs{}
					req.Term = sendTerm
					req.CandidateID = rf.me
					req.LastLogIndex = sendLastLogIndex
					req.LastLogTerm = sendLastLogTerm
					rep := RequestVoteReply{}

					rf.mu.Unlock()
					res := rf.sendRequestVote(i_peer, &req, &rep)
					rf.mu.Lock()

					if rf.currentTerm != sendTerm || rf.state != "candidate" { // candidate can become follower or leader without change it's term
						DPrintf("==========================")
						DPrintf("server %v: <out date vote request from server %v> (candidate/%v) sendTerm/currentTerm: %v/%v",
							rf.me, i_peer, rf.state, sendTerm, rf.currentTerm)
						DPrintf("==========================")
						return
					}

					if res == true { // reply from peer
						DPrintf("==========================")
						DPrintf("server %v: [receive vote from server %v]: sendTerm: %v",
							rf.me, i_peer, sendTerm)
						if rep.VoteGranted == false {
							DPrintf("server %v: <but server %v reject this vote request>: sendTerm/reply.term: %v/%v",
								rf.me, i_peer, sendTerm, rep.Term)
							if rep.Term > sendTerm {
								rf.TurnToFollower(rep.Term)
								rf.persist()
							}
						} else {
							n_vote_for_me += 1
							DPrintf("server %v: [server %v voteFor this vote request]: sendTerm/reply.term: %v/%v, n_vote_for_me: %v",
								rf.me, i_peer, sendTerm, rep.Term, n_vote_for_me)
							if n_vote_for_me >= HalfPlusOne(len(rf.peers)) { // become leader
								DPrintf("server %v: half plus one (%v) votes have been received, i will be leader, sendTerm/reply.term: %v/%v",
									rf.me, n_vote_for_me, sendTerm, rep.Term)
								rf.InitFollowerLog()
								rf.TurnToLeader()
								rf.persist()
							}
						}
						DPrintf("==========================")
					} else { // no reply from peer
						DPrintf("==========================")
						DPrintf("server %v: <no reply from server %v, for this vote request>: sendTerm: %v",
							rf.me, i_peer, sendTerm)
						DPrintf("==========================")
						return
					}
				}(i, paramTerm, paramLastLogIndex, paramLastLogTerm)
			}

			// reset elecTimeout
			rf.ResetElecTimeout()
		}
	} // for
}

func (rf *Raft) AppendEntriesLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetHeartbeatTimeout(Now())

	for {
		if rf.is_killed == true {
			DPrintf("server %d: (%s term %d) be killed",
				rf.me, rf.state, rf.currentTerm)
			break
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		elapse := Now() - rf.heartBeginTime

		if elapse < rf.heartInterval {
			continue
		}
		if rf.state != "leader" {
			continue
		}

		DPrintf("server %d: (%s, term %d, log len %d) need to send heartbeat, elapse %d",
			rf.me, rf.state, rf.currentTerm, len(rf.log), elapse)

		// heartbeat timeout for leader
		// send heartbeat and appendEntries for followers
		paramTerm := rf.currentTerm
		paramCommitIndex := rf.commitIndex
		paramLogLen := len(rf.log)

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i_peer, sendTerm, sendLogLen, sendCommitIndex int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				i_next := rf.nextIndex[i_peer]
				if i_next > sendLogLen {
					DPrintf("i_next > sendLogLen, return send AppendEntries()")
					return
				}
				args.Term = sendTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = i_next - 1
				args.PrevLogTerm = rf.log[i_next-1].Term
				args.Entries = rf.log[i_next:sendLogLen] // new added entries
				args.LeaderCommit = sendCommitIndex

				rf.mu.Unlock()
				res := rf.sendAppendEntries(i_peer, &args, &reply)
				rf.mu.Lock()

				if sendTerm != rf.currentTerm || rf.state != "leader" {
					return
				}

				if res == false { // no reply from peer
					return
				}

				if reply.Success == true {
					// for heartbeat
					//   do nothing
					// for appendEntries
					//   update nextIndex, matchIndex
					//   Notice, just can commit with self Term
					rf.nextIndex[i_peer] = sendLogLen
					if sendTerm == rf.log[sendLogLen-1].Term { // can't submit previous leader's log
						rf.matchIndex[i_peer] = sendLogLen - 1
					}
				} else if reply.Term > rf.currentTerm {
					rf.TurnToFollower(reply.Term)
					rf.persist()
				} else { // log do not match
					// optimization for find nextIndex
					// when leader's log do not match follower's log.
					// there are two case
					// the first case: is len(follower's log) < len(leader's log)
					// the second case: is len(follower's log) >= len(leader's log)
					if rf.nextIndex[i_peer] > 1 {
						DPrintf("server %v, (leader) nextIndex[%v]/i_next begin is: %v/%v, reply.FirstIndex: %v, reply.FirstTerm: %v",
							rf.me, i_peer, rf.nextIndex[i_peer], i_next, reply.FirstIndex, reply.FirstTerm)
						//rf.nextIndex[i_peer] --
						if reply.FirstTerm == -1 { // first case
							rf.nextIndex[i_peer] = reply.FirstIndex
						} else { // second case
							idx := reply.FirstIndex - 1
							for i := args.PrevLogIndex; i >= reply.FirstIndex; i-- {
								if rf.log[i].Term == reply.FirstTerm {
									idx = i
									break
								}
							}
							if idx < 0 {
								DPrintf("Exception: nextIndex < 0")
							}
							rf.nextIndex[i_peer] = idx + 1
						}
						DPrintf("server %v, (leader) nextIndex[%v]/i_next after is: %v/%v, reply.FirstIndex: %v, reply.FirstTerm: %v",
							rf.me, i_peer, rf.nextIndex[i_peer], i_next, reply.FirstIndex, reply.FirstTerm)
					}
				}
			}(i, paramTerm, paramLogLen, paramCommitIndex)
		}

		// reset heartbeat timeout
		rf.ResetHeartbeatTimeout(Now())
	} // for
}

func (rf *Raft) UpdateCommitIndexLoop(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for {
		if rf.is_killed == true {
			DPrintf("server %d: (%s term %d) be killed",
				rf.me, rf.state, rf.currentTerm)
			break
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.state == "leader" {
			sorted_matchIndex := make([]int, len(rf.matchIndex))
			copy(sorted_matchIndex, rf.matchIndex)
			sort.Slice(sorted_matchIndex, func(i, j int) bool {
				return sorted_matchIndex[i] < sorted_matchIndex[j]
			})
			rf.commitIndex = sorted_matchIndex[len(rf.matchIndex)/2]
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.log[i].Command
			msg.CommandIndex = i
			msg.CommandTerm = rf.log[i].Term
			applyCh <- msg
			rf.lastApplied++
		}
	}
}

func Now() int {
	return int(time.Now().UnixNano() / 1000000)
}

func HalfPlusOne(n int) int {
	return n/2 + 1
}

func (rf *Raft) TurnToCandidate() {
	rf.currentTerm += 1
	rf.state = "candidate"
	rf.voteFor = rf.me
	rf.commitIndex = 0
}

func (rf *Raft) TurnToFollower(term int) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.ResetElecTimeout()
	DPrintf("server %d: (become follower, term %d, elecTimeout %v, elecBeginTime %v)",
		rf.me, rf.currentTerm, rf.elecTimeout, rf.elecBeginTime)
}

func (rf *Raft) TurnToLeader() {
	rf.state = "leader"
	rf.commitIndex = 0
	rf.ResetHeartbeatTimeout(0)
	DPrintf("server %d: (become leader, term %d)", rf.me, rf.currentTerm)
}

func (rf *Raft) ResetElecTimeout() {
	rf.elecTimeout = rand.Int()%600 + rf.lowElecInterval // [1000ms, 1600ms]
	rf.elecBeginTime = Now()
}

func (rf *Raft) ResetHeartbeatTimeout(cur int) {
	rf.heartBeginTime = cur
}

func (rf *Raft) InitFollowerLog() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}
