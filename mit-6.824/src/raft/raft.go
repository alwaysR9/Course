package raft

import "sync"
import "labrpc"

import "fmt"
import "time"
import "sort"
import "math/rand"

import "bytes"
import "labgob"
import "log"


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
	log         []LogEntry // the log is not in snapshot
	baseIndex   int // the last log entry in snapshot, which must be applied

	// commit and apply
	// Invariant: commitIndex and lastApplied >= baseIndex
	commitIndex int
	lastApplied int

	// log match and copy
	// Invariant: matchIndex[i] and nextIndex[i] >= baseIndex
	matchIndex []int // where the last matched
	nextIndex  []int // where the copy begin

	// snapshot
	needSnapshot []bool // whether follower need snapshot

	// channel for sending message to kv
	applyCh chan ApplyMsg

	// election
	elecTimeout     int // init when state turn to follower
	elecBeginTime   int // init when state turn to follower
	lowElecInterval int // the smallest election timeout

	// heartbeat
	heartBeginTime  int // init when state turn to leader
	heartInterval   int // heartbeat interval

	isKilled bool
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	ApplySnapshot bool
	Snapshot []byte
}

//=========== RPC argv and reply ============//
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
	NextIndex int
	NextTerm  int
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderID int
	LastIncludedIndex int // last log index in snapshot
	LastIncludedTerm  int // last log term in snapshot
	Data     []byte // snapshot's data
}

type InstallSnapshotReply struct {
	Term int
}

//============ thread context ============//
type StateContext struct {
	state   string
	term    int
	voteFor int
}

func (c *StateContext) Compare(other *StateContext) bool {
	return (c.state == other.state &&
			c.term == other.term &&
			c.voteFor == other.voteFor)
}

type LogContext struct {
	lastEntryIndex int
	lastEntryTerm  int
}

type CopyContext struct {
	lastCopiedEntryIndex int
}

type MatchContext struct {
	nextIndex int
	matchIndex int
}

func (c *MatchContext) Compare(other *MatchContext) bool {
	return (c.nextIndex == other.nextIndex &&
			c.matchIndex == other.matchIndex)
}

//============= interface =============//
func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

// create a Raft level
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

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

	rf.commitIndex = rf.baseIndex
	rf.lastApplied = rf.baseIndex

	rf.heartInterval = 150
	rf.lowElecInterval = 1000

	rand.Seed(int64(Now()))

	go rf.ElecLoop()
	go rf.AppendEntriesLoop()
	go rf.UpdateCommitIndexLoop()
	go rf.InstallSnapshotLoop()

	return rf
}

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

// caller should not add rf.Lock around this interface
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.isKilled = true
	rf.mu.Unlock()
}

// return currentTerm and whether this server
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

func (rf *Raft) SaveSnapshotWithRaft(snapshot []byte, lastAppliedIndex int) {
	// lastAppliedIndex: the last index applied by State Machine
	// lastAppliedIndex must <= rf.GetLastEntryIndex()

	if lastAppliedIndex <= rf.GetLastEntryIndex() {
		raftState := rf.EncodeRaftBeginWith(lastAppliedIndex)
		rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	} else {
		rf.Log(fmt.Sprintf("[SaveSnapshotWithRaft]: Exception lastAppliedIndex > rf.GetLastEntryIndex(): %v/%v",
				 lastAppliedIndex, rf.GetLastEntryIndex()))
	}
}

func (rf *Raft) DiscardOldLog(lastAppliedIndex int) {
	// lastAppliedIndex: the last index applied by State Machine
	// lastAppliedIndex must <= rf.GetLastEntryIndex()

	if lastAppliedIndex <= rf.GetLastEntryIndex() {
		physicalIndex := rf.GetPhysicalIndex(lastAppliedIndex)
		rf.log = append(rf.log[physicalIndex:])
		rf.baseIndex = lastAppliedIndex

		if rf.state == "leader" {
			for i, _ := range rf.matchIndex {
				if rf.matchIndex[i] < rf.baseIndex {
					rf.Log(fmt.Sprintf("[DiscardOldLog]: server %v need InstallSnapshot", i))
					rf.needSnapshot[i] = true
					rf.nextIndex[i] = rf.baseIndex + 1
					rf.matchIndex[i] = rf.baseIndex
				}
			}
		}
		rf.Log(fmt.Sprintf("[DiscardOldLog]: new baseIndex: %v", rf.baseIndex))
	} else {
		rf.Log(fmt.Sprintf("[DiscardOldLog]: Exception lastAppliedIndex > rf.GetLastEntryIndex(): %v/%v",
			lastAppliedIndex, rf.GetLastEntryIndex()))
	}
}

//============= persist =============//
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

func (rf *Raft) EncodeRaftBeginWith(logicIndex int) []byte {
	physicalIndex := rf.GetPhysicalIndex(logicIndex)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.state)
	e.Encode(rf.log[physicalIndex:])
	e.Encode(logicIndex)
	return w.Bytes()
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
	var _log        []LogEntry
	var baseIndex   int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&_log) != nil ||
		d.Decode(&baseIndex) != nil {
		rf.Log(fmt.Sprintf("[readPersist]: Fail readPersist()"))
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.state = state
		rf.log = _log
		rf.baseIndex = baseIndex
		rf.Log(fmt.Sprintf("[readPersist]: SUCCESS readPersist()"))
	}
}

//============== RPC Handler ==============//
// RPC handler (this RPC comes from candidate)
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

	rf.Log(fmt.Sprintf("[RequestVote Handler] candidate %v, term:%v, lastIndex:%v, lastTerm:%v",
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm))

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
		rf.Log(fmt.Sprintf("[RequestVote Handler]: reject candidate %v", args.CandidateID))
		return
	} else if args.Term == rf.currentTerm {
		if rf.voteFor != -1 {
			reply.VoteGranted = false
			rf.Log(fmt.Sprintf("[RequestVote Handler]: reject candidate %v", args.CandidateID))
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
		rf.Log(fmt.Sprintf("[RequestVote Handler]: vote for candidate %v", args.CandidateID))
		return
	}
	reply.VoteGranted = false
	rf.Log(fmt.Sprintf("[RequestVote Handler]: reject candidate %v", args.CandidateID))
	return
}

// RPC handler (this RPC comes from leader)
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

	//rf.Log("server %v: receive heartbeat from %v\n", rf.me, args.LeaderID)
	log.Println("-----------------------------------------")
	rf.Log(fmt.Sprintf("[AppendEntries Handler]: receive heartbeat from leader %v, matchPointIndex:%v matchPointTerm:%v",
		args.LeaderID, args.PrevLogIndex, args.PrevLogTerm))

	is_should_persist := false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if is_should_persist == true {
			rf.persist()
		}
		log.Println("-----------------------------------------")
	}()

	reply.Term = rf.currentTerm

	// state checking
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.Log(fmt.Sprintf("[AppendEntries Handler]: reject heartbeat from leader %v", args.LeaderID))
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == "leader" {
			rf.Log(fmt.Sprintf("[AppendEntries Handler]: Exception two leaders"))
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
	if args.PrevLogIndex >= rf.baseIndex {
		if args.PrevLogIndex <= rf.GetLastEntryIndex() &&
		   rf.GetLogEntryAt(args.PrevLogIndex).Term == args.PrevLogTerm { // log match success

			leaderLogLen := args.PrevLogIndex + 1 + len(args.Entries)
			if len(args.Entries) == 0 {
				reply.Success = true
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = args.LeaderCommit
				}
				rf.Log(fmt.Sprintf("[AppendEntries Handler]: empty heartbeat from leader %v", args.LeaderID))
				return
			} else if rf.GetLogLen() > args.PrevLogIndex + 1 &&
				rf.GetLogEntryAt(args.PrevLogIndex + 1).Term == args.Entries[0].Term &&
				leaderLogLen <= rf.GetLogLen() {
				// this heartbeat is not up to date
				// because network is unstable, package can reordered or repeated
				// so old message should not cover new message
				reply.Success = true
				rf.Log(fmt.Sprintf("[AppendEntries Handler]: Drop, out of date heartbeat from %v", args.LeaderID))
				return
			} else {
				// now this is a new message, just do log copying
				is_should_persist = true

				if leaderLogLen < rf.GetLogLen() {
					rf.log = append(rf.log[:leaderLogLen-rf.baseIndex])
					rf.Log(fmt.Sprintf("[AppendEntries Handler]: Cut my log"))
				}

				for i, e := range args.Entries {
					i_next := args.PrevLogIndex + 1 + i // logic index
					if i_next < rf.GetLogLen() {
						rf.SetLogEntryAt(i_next, e)
					} else {
						rf.log = append(rf.log, e)
					}
				}
				rf.Log(fmt.Sprintf("[AppendEntries Handler]: SUCCESS log copying from %v", args.LeaderID))

				// set commitIndex
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = args.LeaderCommit
				}

				reply.Success = true
				return
			}
		} else if args.PrevLogIndex <= rf.GetLastEntryIndex() &&
			 rf.GetLogEntryAt(args.PrevLogIndex).Term != args.PrevLogTerm { // len(follwer's log) >= len(leader's log)
			// args.PrevLogIndex must > rf.baseIndex
			// optimization when log do not match
			// nextIndex = [rf.baseIndex+1, args.PrevLogIndex]
			nextIndex := args.PrevLogIndex
			for nextIndex > rf.baseIndex {
				if rf.GetLogEntryAt(nextIndex).Term != rf.GetLogEntryAt(args.PrevLogIndex).Term {
					break
				}
				nextIndex--
			}
			reply.NextIndex = nextIndex + 1 // nextIndex: [baseIndex + 1, args.PrevLogIndex]
			reply.NextTerm = rf.GetLogEntryAt(reply.NextIndex).Term
			reply.Success = false
			rf.Log(fmt.Sprintf("[AppendEntries Handler]: log matching fail, NextIndex:%v NextTerm:%v",
				reply.NextIndex, reply.NextTerm))
			return
		} else if rf.GetLastEntryIndex() < args.PrevLogIndex { // len(follow's log) < len(leader's log)
			// optimization when log do not match
			reply.NextIndex = rf.GetLogLen() // NextIndex >= 1
			reply.NextTerm = -1
			reply.Success = false
			rf.Log(fmt.Sprintf("[AppendEntries Handler]: log matching fail, i am too far away from leader's log"))
			return
		}
	} else {
		// this is the case which should never happen
		rf.Log(fmt.Sprintf("[AppendEntries Handler]: Exception matchPointIndex:%v < baseIndex:%v",
			args.PrevLogIndex, rf.baseIndex))
		reply.NextIndex = -2
		reply.NextTerm = -2
		reply.Success = false
		return
	}
}

// RPC Handler (this RPC comes from leader)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.TurnToFollowerAndVote(args.Term, args.LeaderID)
		rf.persist()
	} else if rf.state == "candidate" {
		rf.TurnToFollowerAndVote(args.Term, args.LeaderID)
		rf.persist()
	} else if rf.state == "leader" {
		rf.Log(fmt.Sprintf("[InstallSnapshot handler]: Exception two leaders"))
		return
	} else {
		// pass
	}

	rf.ResetElecTimeout()

	// filter duplicate or old RPC
	if rf.baseIndex >= args.LastIncludedIndex {
		return
	}

	// install log
	rf.log = make([]LogEntry, 1)
	rf.baseIndex = args.LastIncludedIndex
	rf.log[0].Term = args.LastIncludedTerm
	rf.commitIndex = rf.baseIndex
	rf.lastApplied = rf.baseIndex

	// persist
	rf.SaveSnapshotWithRaft(args.Data, rf.baseIndex)
	rf.ResetElecTimeout()

	// update kv
	data := ApplyMsg{}
	data.ApplySnapshot = true
	data.Snapshot = args.Data
	rf.applyCh <- data
	rf.Log(fmt.Sprintf("[InstallSnapshot handler]: InstallSnapshot finish"))
	rf.ResetElecTimeout()
	return
}

//=============== Background Thread ================//
// election thread
func (rf *Raft) ElecLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetElecTimeout()
	n_vote_for_me := 0

	for !rf.isKilled {
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

			rf.Log(fmt.Sprintf("[ElecLoop BGThread]: need request vote, timeout %d, elapse %d", rf.elecTimeout, elapse))

			// save context
			sendContext := StateContext{rf.state, rf.currentTerm, rf.voteFor}
			logContext := LogContext{rf.GetLastEntryIndex(), rf.GetLastEntryTerm()}

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i_peer int) { // send VoteRequests in Parallel
					// simulate time delay
					//r := rand.Int() % 200
					//time.Sleep(time.Duration(r) * time.Millisecond)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					// check state
					curContext := StateContext{rf.state, rf.currentTerm, rf.voteFor}
					if !sendContext.Compare(&curContext) {
						rf.Log(fmt.Sprintf("[ElecLoop BGThread]: state has changed before RPC, old StateContext:%v", sendContext))
						return
					}

					req := RequestVoteArgs{}
					req.Term = rf.currentTerm
					req.CandidateID = rf.me
					req.LastLogIndex = logContext.lastEntryIndex
					req.LastLogTerm = logContext.lastEntryTerm
					rep := RequestVoteReply{}

					rf.mu.Unlock()
					res := rf.sendRequestVote(i_peer, &req, &rep)
					rf.mu.Lock()

					// check state
					curContext = StateContext{rf.state, rf.currentTerm, rf.voteFor}
					if !sendContext.Compare(&curContext) {
						rf.Log(fmt.Sprintf("[ElecLoop BGThread]: state has changed after RPC, old StateContext:%v", sendContext))
						return
					}

					// no need to check log,
					// because if the log has changed, the state must changed already

					if res == true { // reply from peer
						log.Println("=========================================")
						rf.Log(fmt.Sprintf("[ElecLoop BGThread]: receive vote from server %v, reply:%v", i_peer, rep))
						if rep.VoteGranted == false {
							rf.Log(fmt.Sprintf("[ElecLoop BGThread]: but server %v reject this vote request, reply:%v", i_peer, rep))
							if rep.Term > rf.currentTerm {
								rf.TurnToFollower(rep.Term, true)
								rf.persist()
							}
						} else {
							n_vote_for_me += 1
							rf.Log(fmt.Sprintf("[ElecLoop BGThread]: server %v voteFor this vote request, reply:%v, nVoteForMe:%v", i_peer, rep, n_vote_for_me))
							if n_vote_for_me >= HalfPlusOne(len(rf.peers)) { // become leader
								rf.Log(fmt.Sprintf("[ElecLoop BGThread]: half plus one (%v) votes have been received, i will be leader", n_vote_for_me))
								rf.InitFollowerLog()
								rf.TurnToLeader()
								rf.persist()
							}
						}
						log.Println("=========================================")
					} else { // no reply from peer
						rf.Log(fmt.Sprintf("[ElecLoop GBThread]: RPC Timeout to server %v", i_peer))
						return
					}
				}(i)
			}

			// reset elecTimeout
			rf.ResetElecTimeout()
		}
	} // for
}

// three things to do in this background thread:
// (1) heartbeat
// (2) log matching
// (3) log copying
//
// just send RPC for those follower that
// keep up with the leader
func (rf *Raft) AppendEntriesLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetHeartbeatTimeout(Now())

	for !rf.isKilled {
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

		rf.Log(fmt.Sprintf("[heartbeat GBThread]: need to send heartbeat, elapse: %d", elapse))

		// save context
		sendContext := StateContext{rf.state, rf.currentTerm, rf.voteFor}

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i_peer int) {
				// simulate time delay
				//r := rand.Int() % 200
				//time.Sleep(time.Duration(r) * time.Millisecond)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.Log(fmt.Sprintf("[heartbeat GBThread]: begin heartbeat to server %v", i_peer))

				// this follower do not keep up with the leader
				if rf.needSnapshot[i_peer] {
					rf.Log(fmt.Sprintf("[heartbeat GBThread]: server %v should InstallSnapshot, not send heartbeat", i_peer))
					return
				}

				// check state context
				curContext := StateContext{rf.state, rf.currentTerm, rf.voteFor}
				if !sendContext.Compare(&curContext) {
					rf.Log(fmt.Sprintf("[heartbeat GBThread]: state has changed before RPC, old StateContext:%v", sendContext))
					return
				}

				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				i_next := rf.nextIndex[i_peer]

				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = i_next - 1
				args.PrevLogTerm = rf.GetLogEntryAt(i_next - 1).Term
				args.Entries = rf.log[i_next - rf.baseIndex : rf.GetLogLen() - rf.baseIndex]
				args.LeaderCommit = rf.commitIndex

				// save log context
				copyContext := CopyContext{rf.GetLastEntryIndex()}
				// save match context
				oldMatchContext := MatchContext{i_next, rf.matchIndex[i_peer]}

				rf.mu.Unlock()
				res := rf.sendAppendEntries(i_peer, &args, &reply)
				rf.mu.Lock()

				// check state context
				curContext = StateContext{rf.state, rf.currentTerm, rf.voteFor}
				newMatchContext := MatchContext{rf.nextIndex[i_peer], rf.matchIndex[i_peer]}
				if !sendContext.Compare(&curContext) {
					rf.Log(fmt.Sprintf("[heartbeat GBThread]: state has changed after RPC, old StateContext:%v", sendContext))
					return
				}
				if !newMatchContext.Compare(&oldMatchContext) {
					rf.Log(fmt.Sprintf("[heartbeat GBThread]: match state has changed after RPC, old MatchStateContext:%v", oldMatchContext))
					return
				}

				if res == false { // no reply from peer
					rf.Log(fmt.Sprintf("[heartbeat GBThread]: RPC Timeout to server %v", i_peer))
					return
				}

				if reply.Success == true { // heartbeat and log copy successfully
					lastMatchedEntryIndex := copyContext.lastCopiedEntryIndex
					if len(args.Entries) > 0 {
						if lastMatchedEntryIndex + 1 > rf.nextIndex[i_peer] {
							rf.nextIndex[i_peer] = lastMatchedEntryIndex + 1
						}
					}
					// can't submit previous leader's log
					// please look at figure 8 in <<In search of an Understandable Consensus Algorithm (Extended Version)>>
					if rf.currentTerm == rf.GetLogEntryAt(lastMatchedEntryIndex).Term &&
						lastMatchedEntryIndex > rf.matchIndex[i_peer] {
						rf.matchIndex[i_peer] = lastMatchedEntryIndex
					}
				} else if reply.Term > rf.currentTerm {
					rf.TurnToFollower(reply.Term, true)
					rf.persist()
				} else { // log do not match
					// the first case: len(follower's log) < len(leader's log)
					// the second case: len(follower's log) >= len(leader's log)
					if rf.nextIndex[i_peer] > rf.baseIndex {
						if reply.NextTerm == -2 {
							// pass
						} else if reply.NextTerm == -1 { // first case
							if reply.NextIndex <= rf.baseIndex { // follower lags too much
								rf.Log(fmt.Sprintf("[heartbeat GBThread]: server %v need InstallSnapshot", i_peer))
								rf.needSnapshot[i_peer] = true
								rf.nextIndex[i_peer] = rf.baseIndex + 1
								rf.matchIndex[i_peer] = rf.baseIndex
								return
							}
							rf.nextIndex[i_peer] = reply.NextIndex
						} else { // second case
							next := rf.baseIndex
							for i := args.PrevLogIndex; i >= rf.baseIndex; i-- {
								if i < reply.NextIndex {
									next = reply.NextIndex
									break
								}
								if rf.GetLogEntryAt(i).Term == reply.NextTerm {
									next = i + 1
									break
								}
							}
							if next == rf.baseIndex {
								rf.Log(fmt.Sprintf("[heartbeat GBThread]: server %v need InstallSnapshot", i_peer))
								rf.needSnapshot[i_peer] = true
								rf.nextIndex[i_peer] = rf.baseIndex + 1
								rf.matchIndex[i_peer] = rf.baseIndex
							} else {
								rf.nextIndex[i_peer] = next
							}
						}
					}
				}
				rf.Log(fmt.Sprintf("[heartbeat GBThread]: end heartbeat to server %v", i_peer))
			}(i)
		}

		// reset heartbeat timeout
		rf.ResetHeartbeatTimeout(Now())
	} // for
}

// update commit pointer thread
func (rf *Raft) UpdateCommitIndexLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.isKilled {
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

		if rf.lastApplied < rf.commitIndex {
			rf.Log(fmt.Sprintf("[UpdateCommitIndexLoop GBThread]: commitIndex:%v, lastApplied:%v",
				rf.commitIndex, rf.lastApplied))
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.GetLogEntryAt(i).Command
			msg.CommandIndex = i
			msg.CommandTerm = rf.GetLogEntryAt(i).Term
			rf.applyCh <- msg
			rf.lastApplied++
		}
	}
}

// Install Snapshot thread
// only set needSnapshot[i]=false when
// InstallSnapshot RPC return,
// keep needSnapshot[i]=true if
// InstallSnapshot RPC timeout
func (rf *Raft) InstallSnapshotLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.isKilled {
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.state != "leader" {
			continue
		}

		sendContext := &StateContext{rf.state, rf.currentTerm, rf.voteFor}
		for i, _ := range rf.needSnapshot {
			if !rf.needSnapshot[i] {
				continue
			}

			go func(i_peer int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.Log(fmt.Sprintf("[InstallSnapshotLoop GBThread]: begin InstallSnapshot to server %v", i_peer))

				// check context
				curContext := &StateContext{rf.state, rf.currentTerm, rf.voteFor}
				if !sendContext.Compare(curContext) {
					rf.Log(fmt.Sprintf("[InstallSnapshotLoop GBThread]: state has changed before RPC, old StateContext:%v", sendContext))
					return
				}

				// params for RPC
				args := InstallSnapshotArgs{}
				reply := InstallSnapshotReply{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.LastIncludedIndex = rf.baseIndex
				args.LastIncludedTerm = rf.log[0].Term
				args.Data = rf.persister.ReadSnapshot()

				// call RPC
				rf.mu.Unlock()
				res := rf.sendInstallSnapshot(i_peer, &args, &reply)
				rf.mu.Lock()

				// check context
				curContext = &StateContext{rf.state, rf.currentTerm, rf.voteFor}
				if !sendContext.Compare(curContext) {
					rf.Log(fmt.Sprintf("[InstallSnapshotLoop GBThread]: state has changed after RPC, old StateContext:%v", sendContext))
					return
				}

				if res == false { // no reply from peer
					rf.Log(fmt.Sprintf("[InstallSnapshotLoop GBThread]: RPC Timeout to server %v", i_peer))
					return
				}

				if reply.Term > rf.currentTerm {
					rf.TurnToFollower(reply.Term, true)
					rf.persist()
				} else if reply.Term == rf.currentTerm {
					rf.needSnapshot[i_peer] = false
					rf.Log(fmt.Sprintf("[InstallSnapshotLoop GBThread]: end InstallSnapshot to server %v successfully", i_peer))
				} else {
					// pass
				}
			}(i)
		}
	}
}

//=============== private func ================//
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

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) TurnToCandidate() {
	rf.currentTerm += 1
	rf.state = "candidate"
	rf.voteFor = rf.me
	rf.commitIndex = rf.baseIndex
	rf.Log(fmt.Sprintf("[TurnToCandidate] elecTimeout:%v heartbeatTimeout:%v",
		Now() - rf.elecBeginTime,
		Now() - rf.heartBeginTime))
}

func (rf *Raft) TurnToFollower(term int, isResetElecTimeout bool) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.voteFor = -1
	rf.commitIndex = rf.baseIndex
	if isResetElecTimeout {
		rf.ResetElecTimeout()
	}
	rf.Log(fmt.Sprintf("[TurnToFollower] elecTimeout:%v heartbeatTimeout:%v",
		Now() - rf.elecBeginTime,
		Now() - rf.heartBeginTime))
}

func (rf *Raft) TurnToFollowerAndVote(term int, voteFor int) {
	rf.TurnToFollower(term, true)
	rf.voteFor = voteFor
}

func (rf *Raft) TurnToLeader() {
	rf.state = "leader"
	rf.commitIndex = rf.baseIndex
	rf.ResetHeartbeatTimeout(0)
	rf.Log(fmt.Sprintf("[TurnToLeader] elecTimeout:%v heartbeatTimeout:%v",
		Now() - rf.elecBeginTime,
		Now() - rf.heartBeginTime))
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
	rf.needSnapshot = make([]bool , len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.GetLogLen()
		rf.matchIndex[i] = rf.baseIndex
		rf.needSnapshot[i] = false
	}
}

func (rf *Raft) Log(logString string) {
	if rf.state == "leader" {
		log.Printf("[Raft] server %v %v term:%v voteFor:%v logicLogLen:%v phyLogLen:%v " +
			"commitIndex:%v lastApplied:%v baseIndex:%v baseTerm:%v " +
			"lastIndex:%v lastTerm:%v matchIndex:%v nextIndex:%v %s",
			rf.me, rf.state, rf.currentTerm, rf.voteFor, rf.GetLogLen(), len(rf.log),
			rf.commitIndex, rf.lastApplied, rf.baseIndex, rf.GetLogEntryAt(rf.baseIndex).Term,
			rf.GetLastEntryIndex(), rf.GetLastEntryTerm(), rf.matchIndex, rf.nextIndex, logString)
	} else {
		log.Printf("[Raft] server %v %v term:%v voteFor:%v logicLogLen:%v phyLogLen:%v " +
			"commitIndex:%v lastApplied:%v baseIndex:%v baseTerm:%v " +
			"lastIndex:%v lastTerm:%v %s",
			rf.me, rf.state, rf.currentTerm, rf.voteFor, rf.GetLogLen(), len(rf.log),
			rf.commitIndex, rf.lastApplied, rf.baseIndex, rf.GetLogEntryAt(rf.baseIndex).Term,
			rf.GetLastEntryIndex(), rf.GetLastEntryTerm(), logString)
	}
}

//=============== RPC Wrapper ===============//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//================ util func =================//
func Now() int {
	return int(time.Now().UnixNano() / 1000000)
}

func HalfPlusOne(n int) int {
	return n/2 + 1
}

func max(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}