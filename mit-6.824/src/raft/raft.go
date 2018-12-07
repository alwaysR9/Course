package raft

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

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	voteFor     int // -1 if not vote in currentTerm
	state       string // {"follower", "candidate", "leader"}

	// log data structure
	// ----------------------
	// |base| | | | | | | | |
	// ----------------------
	// Invariant:
	//   Base is the last applied log entry by this kv server.
	//   So the real index of an entry log[i] = (baseIndex + i)
	log         []LogEntry
	baseIndex   int

	// commit and apply
	commitIndex int // volatile when state changed
	lastApplied int // volatile when state changed, 
	                // it is only used for record the index of entry that put into chanel.
	                // It can not be used for cutting the log during snapshot.
	                // Please use kv.applyIndex for cutting the log.

	// log match and copy
	// the leader will use there variables
	matchIndex []int // where the last matched
	nextIndex  []int // where the copy begin

	// election
	elecTimeout     int // init when state turn to follower
	elecBeginTime   int // init when state turn to follower
	lowElecInterval int // the smallest election timeout

	// heartbeat
	heartBeginTime  int // init when state turn to leader
	heartInterval   int // heartbeat interval

	isKilled bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.state)
	e.Encode(rf.log)
	e.Encode(rf.baseIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor     int
	var state       string
	var log         []LogEntry
	var baseIndex   int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&baseIndex) != nil {
		DPrintf("server %v: Fail readPersist()", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.state = state
		rf.log = log
		rf.baseIndex = baseIndex
		DPrintf("server %v: SUCCESS readPersist()", rf.me)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RPC handler from candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// if candidate.Term < me.Term:
	//     refuse to vote
	// else if candidate.Term == me.Term:
	//     if I have already vote for someone:
	//         refuse to vote
	//     else:
	//         vote for this candidate
	// else if candidate.Term > me.Term:
	//     turn me to follower
	//     if candidate.log newer than me.log:
	//         vote for this candidate
	//     else:
	//         refuse to vote

	DPrintf("server %v: (%v) receive requestVote() from server %v, term: %v/%v, my voteFor: %v; log len: %v/%v, log last term: %v/%v",
		rf.me, rf.state, args.CandidateID, rf.currentTerm, args.Term, rf.voteFor, rf.GetLogLen(), args.LastLogIndex+1, rf.GetLastEntryTerm(), args.LastLogTerm)

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
	} else if args.Term == rf.currentTerm {
		if rf.voteFor != -1 {
			reply.VoteGranted = false
			return
		} else {
			// pass, to check other.log whether newer than me
		}
	} else {
		// do not reset election timeout here
		// because If I have the newer log than candidate,
		// I will not vote for the candidate,
		// the election may fail.
		// If reset my election timeout, may lead me do not have
		// opportunity to become a candidate, So the election
		// will fail forever.
		rf.TurnToFollower(args.Term, false) 
		is_should_persist = true
		// to check other.log whether newer than me 
	}

	// other.term > me.term or
	// other.term == me.term and me.voteFor is -1
	// check whether other.log newer than me
	if args.LastLogTerm > rf.GetLastEntryTerm() ||
		args.LastLogTerm == rf.GetLastEntryTerm() &&
		args.LastLogIndex >= rf.GetLastEntryIndex() {

		reply.VoteGranted = true
		rf.voteFor = args.CandidateID
		rf.ResetElecTimeout()

		is_should_persist = true
		DPrintf("server %v: (follower) return requestVote() from server %v SUCCESS, request.Term/Term: %v/%v",
			rf.me, args.CandidateID, args.Term, rf.currentTerm)
		return
	}
	reply.VoteGranted = false
	return
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	// the entry where to match
	PrevLogIndex int
	PrevLogTerm  int
	// the log entries need to copy
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// if Success==false,
	// these are the first enrty's index and term
	// the same term with entry that do not match
	FirstIndex int
	FirstTerm  int
}

// RPC handler from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// state checking:
	// if leader.Term < me.Term:
	//     refuse heartbeat
	// else if leader.Term == me.Term:
	//     if I am the leader:
	//         (Exception: two leader)
	//     else if I am the candidate:
	//         turn me to follower
	//         do log copy
	//     else if I am the follower:
	//         if I vote for another candidate:
	//             turn me to this leader's follower
	//         else:
	//             do log copy
	// else if leader.Term > me.Term:
	//     turn me to follower
	//     do log copy

	// log matching and copy:
	// if me.LastLogEntryIndex < leader.preLogIndex:
	//     fail to copy (log matching fail)
	// else if me.LastLogEntryTerm != leader.preLogTerm:
	//     if me.LastLogEntryTerm > leader.preLogTerm:
	//         (Exception: follower log newer than leader)
	//     else:
	//         fail to copy (log matching fail)
	// else:
	//     copying the log

	//DPrintf("server %v: receive heartbeat from %v\n", rf.me, args.LeaderID)

	is_should_persist := false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if is_should_persist == true {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm

	// state checking
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == "leader" {
			DPrintf("Assert Exception find two leader")
			reply.Success = false
			return
		} else if rf.state == "candidate" {
			rf.TurnToFollowerAndVote(args.Term, args.LeaderID)
			is_should_persist = true
			// to log matching and copy
		} else { // me.state == 'follower'
			if rf.voteFor != args.LeaderID {
				rf.TurnToFollowerAndVote(args.Term, args.LeaderID)
				is_should_persist = true
			} else {
				// do nothing
			}
			// to log matching and copy
		}
	} else {
		rf.TurnToFollowerAndVote(args.Term, args.LeaderID)
		is_should_persist = true
		// to log matching and copy
	}

	// reset election timeout
	rf.ResetElecTimeout()

	// log matching and copy
	if rf.GetLastEntryIndex() < args.PrevLogIndex { // len(follow's log) < len(leader's log)
		DPrintf("server %v: receive heartbeat from %v, but i am too far away from leader's log, log len: %v/%v",
			rf.me, args.LeaderID, rf.GetLastEntryIndex(), args.PrevLogIndex)

		// optimization when log do not match
		reply.FirstIndex = rf.GetLogLen() // FirstIndex >= 1
		reply.FirstTerm = -1

		reply.Success = false
		return
	} else if rf.GetLogEntryAt(args.PrevLogIndex).Term != args.PrevLogTerm { // len(follwer's log) >= len(leader's log)
		// optimization when log do not match
		// find first log entry in follow that
		// has the same Term with log entry do not matched
		firstIndex := args.PrevLogIndex
		for firstIndex > rf.baseIndex {
			if rf.GetLogEntryAt(firstIndex).Term != rf.GetLogEntryAt(args.PrevLogIndex).Term {
				break
			}
			firstIndex -= 1
		}
		reply.FirstIndex = firstIndex + 1 // FirstIndex: [1, args.PrevLogIndex]
		reply.FirstTerm = rf.GetLogEntryAt(reply.FirstIndex).Term

		reply.Success = false
		return
	} else {
		// log match success
		leaderLogLen := args.PrevLogIndex + 1 + len(args.Entries)

		// copying log
		if len(args.Entries) == 0 {
			reply.Success = true
			rf.commitIndex = args.LeaderCommit
			return
		} else if rf.GetLogLen() > args.PrevLogIndex + 1 &&
			rf.GetLogEntryAt(args.PrevLogIndex + 1).Term == args.Entries[0].Term &&
			leaderLogLen <= rf.GetLogLen() {
			// this heartbeat is not up to date
			// because network is unstable, package can reordered or repeated
			// so old message should not cover new message
			DPrintf("-------------------------")
			DPrintf("server %d: SUCCESS heartbeat from leader %v", rf.me, args.LeaderID)
			DPrintf("server %d: But, this appendEntries() request is not up to date, leader PrevLogIndex:%v, Term:%v/%v, leaderLen/curLogLen=%v/%v, so drop it",
				rf.me, args.PrevLogIndex, rf.GetLogEntryAt(args.PrevLogIndex+1).Term, args.Entries[0].Term, leaderLogLen, rf.GetLogLen())
			DPrintf("-------------------------")

			reply.Success = true
			return
		} else {
			// now this is a new message, just do log copying
			DPrintf("-------------------------")
			is_should_persist = true

			if leaderLogLen < rf.GetLogLen() {
				rf.log = append(rf.log[:leaderLogLen-rf.baseIndex])
				DPrintf("server %v: SUCCESS cut my log", rf.me)
			}

			for i, e := range args.Entries {
				i_next := args.PrevLogIndex + 1 + i // logic index
				if i_next < rf.GetLogLen() {
					rf.SetLogEntryAt(i_next, e)
				} else {
					rf.log = append(rf.log, e)
				}
			}

			DPrintf("server %v: SUCCESS heartbeat from server %v", rf.me, args.LeaderID)
			DPrintf("server %v: log entry num %v/%v, log entry's last term %v/%v",
				rf.me, rf.GetLogLen(), leaderLogLen, rf.GetLastEntryTerm(), args.Entries[len(args.Entries)-1].Term)
			DPrintf("-------------------------")

			// set commitIndex
			rf.commitIndex = args.LeaderCommit

			reply.Success = true
			return
		}
	}
}

// RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// interface
// caller should not add rf.Lock around this interface
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return -1, -1, false
	}

	// add to log
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.matchIndex[rf.me] = rf.GetLastEntryIndex()

	index := rf.GetLastEntryIndex()
	term := rf.currentTerm
	isLeader := true

	rf.persist()

	rf.ResetHeartbeatTimeout(0)  // sending it to followers
	return index, term, isLeader
}

// interface
// caller should not add rf.Lock around this interface
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.isKilled = true
	rf.mu.Unlock()
}

// init Raft
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.readPersist(persister.ReadRaftState())

	rf.isKilled = false

	if len(rf.state) == 0 {
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.state = "follower"
		rf.log = append(rf.log, LogEntry{0, ""})
		rf.baseIndex = 0
	}

	if rf.state == "leader" {
		rf.InitFollowerLog()
	}

	rf.commitIndex = 0 // term 0(base) has been submitted, update to 0 when state change
	rf.lastApplied = 0 // term 0(base) has been applied, do not update when state change

	rf.heartInterval = 150
	rf.lowElecInterval = 1000

	rand.Seed(int64(Now()))

	go rf.ElecLoop()
	go rf.AppendEntriesLoop()
	go rf.UpdateCommitIndexLoop(applyCh)

	return rf
}

// election thread
func (rf *Raft) ElecLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetElecTimeout()
	n_vote_for_me := 0

	for {
		if rf.isKilled == true {
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
			paramLastLogIndex := rf.GetLastEntryIndex()
			paramLastLogTerm := rf.GetLastEntryTerm()

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i_peer, sendTerm, sendLastLogIndex, sendLastLogTerm int) { // send VoteRequests in Parallel
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.currentTerm != sendTerm || rf.state != "candidate" { // candidate can become follower or leader without change it's term
						DPrintf("==========================")
						DPrintf("server %v: (%v) my state have changed, no need to send heartbeat. sendTerm/currentTerm: %v/%v, sendLogLen/curLogLen: %v/%v",
							rf.me, rf.state, sendTerm, rf.currentTerm, sendLastLogIndex+1, rf.GetLogLen())
						DPrintf("==========================")
						return
					}

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
								rf.TurnToFollower(rep.Term, false)
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

// heartbeat thread
func (rf *Raft) AppendEntriesLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetHeartbeatTimeout(Now())

	for {
		if rf.isKilled == true {
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
			rf.me, rf.state, rf.currentTerm, rf.GetLogLen(), elapse)

		// heartbeat timeout for leader
		// send heartbeat and appendEntries for followers
		paramTerm := rf.currentTerm
		paramCommitIndex := rf.commitIndex
		paramLogLen := rf.GetLogLen()

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i_peer, sendTerm, sendLogLen, sendCommitIndex int) {
				// simulate time delay
				//r := rand.Int() % 200
				//time.Sleep(time.Duration(r) * time.Millisecond)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if sendTerm != rf.currentTerm || rf.state != "leader" {
					DPrintf("==========================")
					DPrintf("server %v: (%v) my state have changed, no need to send heartbeat. sendTerm/currentTerm: %v/%v, sendLogLen/curLogLen: %v/%v",
						rf.me, rf.state, sendTerm, rf.currentTerm, sendLogLen, rf.GetLogLen())
					DPrintf("==========================")
					return
				}

				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				i_next := rf.nextIndex[i_peer]

				if i_next > sendLogLen {
					DPrintf("server %v: (%v) i_next > sendLogLen, return send AppendEntries(). i_next/sendLogLen/curLogLen: %v/%v/%v",
						rf.me, rf.state, i_next, sendLogLen, rf.GetLogLen())
					return
				}
				args.Term = sendTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = i_next - 1
				args.PrevLogTerm = rf.GetLogEntryAt(i_next-1).Term
				args.Entries = rf.log[i_next-rf.baseIndex:sendLogLen-rf.baseIndex] // new added entries
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
					if sendTerm == rf.GetLogEntryAt(sendLogLen-1).Term { // can't submit previous leader's log
						rf.matchIndex[i_peer] = sendLogLen - 1
					}
				} else if reply.Term > rf.currentTerm {
					rf.TurnToFollower(reply.Term, false)
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
								DPrintf("server %v, (leader) log[%v].Term=%v, reply.FirstTerm=%v\n", rf.me, i, rf.GetLogEntryAt(i).Term, reply.FirstTerm)
								if rf.GetLogEntryAt(i).Term == reply.FirstTerm {
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

// update commit pointer thread
func (rf *Raft) UpdateCommitIndexLoop(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for {
		if rf.isKilled == true {
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
			msg.Command = rf.GetLogEntryAt(i).Command
			msg.CommandIndex = i
			msg.CommandTerm = rf.GetLogEntryAt(i).Term
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

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) GetLastEntryIndex() int {
	return rf.GetLogLen() - 1
}

func (rf *Raft) GetLastEntryTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) GetLogEntryAt(logicIndex int) LogEntry {
	return rf.log[logicIndex - rf.baseIndex]
}

func (rf *Raft) SetLogEntryAt(logicIndex int, entry LogEntry) {
	rf.log[logicIndex - rf.baseIndex] = entry
}

func (rf *Raft) GetLogLen() int {
	return rf.baseIndex + len(rf.log)
}

func (rf *Raft) GetlogicIndex(physicalIndex int) int {
	return rf.baseIndex + physicalIndex
}

func (rf *Raft) GetPhysicalIndex(logicIndex int) int {
	return logicIndex - rf.baseIndex
}

/*func (rf *Raft) GetRaftStateSize() {
	return rf.persister.RaftStateSize()
}*/

func (rf *Raft) TurnToCandidate() {
	rf.currentTerm += 1
	rf.state = "candidate"
	rf.voteFor = rf.me
	rf.commitIndex = 0
}

func (rf *Raft) TurnToFollower(term int, isResetElecTimeout bool) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.voteFor = -1
	rf.commitIndex = 0
	if isResetElecTimeout {
		rf.ResetElecTimeout()
	}
	DPrintf("server %d: (become follower, term %d, elecTimeout %v, elecBeginTime %v)",
		rf.me, rf.currentTerm, rf.elecTimeout, rf.elecBeginTime)
}

func (rf *Raft) TurnToFollowerAndVote(term int, voteFor int) {
	rf.TurnToFollower(term, true)
	rf.voteFor = voteFor
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
		rf.nextIndex[i] = rf.GetLogLen()
		rf.matchIndex[i] = 0
	}
}
