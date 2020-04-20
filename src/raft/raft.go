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

import (
	"6.824/src/labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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
}

type LogEntity struct {
	Command interface{}
	term    int
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
	currentTerm int

	votedFor int

	electionTimeout time.Duration

	heartbeatChan chan bool

	leaderChan chan bool

	voteChan chan bool

	state int //1.follower 2.candidate 3.leader

	voted int //voted number

	leader int

	log []LogEntity

	commitIndex int

	lastApplied int

	nextIndex []int

	matchIndex []int

	stop bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 3 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int

	CandidateId int

	//todo 请求投票时日志对比
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int

	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		rf.turnFollower(args.Term, args.CandidateId)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		notify(rf.voteChan)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	log.Printf("server id:%d voted server:%d success:%t", rf.me, args.CandidateId, reply.VoteGranted)
	rf.mu.Unlock()
}

func notify(c chan bool) {
	go func() {
		c <- true
	}()
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
	log.Printf("sendRequestVote serverId: %d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term           int
	LeaderId       int
	PreLogIndex    int
	PreLogTerm     int
	Entries        []LogEntity
	LeaderCommitId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		rf.turnFollower(args.Term, args.LeaderId)
	}
	notify(rf.heartbeatChan)
	if args.Entries != nil {
		//有日志同步
		if args.PreLogIndex != -1 {
			if args.PreLogIndex > len(rf.log) { //没有匹配
				reply.Term = rf.currentTerm
				reply.Success = false
				rf.mu.Unlock()
				return
			} else if args.PreLogTerm != rf.log[args.PreLogIndex].term { //任期不对
				reply.Term = rf.currentTerm
				reply.Success = false
				rf.mu.Unlock()
				return
			}
		}
		if args.PreLogIndex == -1 {
			rf.log = append(rf.log, args.Entries...)
		} else {
			rf.log = append(rf.log[:args.PreLogIndex], args.Entries...)
		}
		if len(rf.log) < args.LeaderCommitId {
			rf.commitIndex = len(rf.log)
		} else {
			rf.commitIndex = args.LeaderCommitId
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("sendRequestAppendEntries serverId: %d", server)
	return rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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
	if rf.state != 3 {
		isLeader = false
		return index, term, isLeader
	}
	logEntity := &LogEntity{command, rf.currentTerm}
	rf.mu.Lock()
	rf.log = append(rf.log, *logEntity)
	index = len(rf.log)
	rf.lastApplied = index
	rf.mu.Unlock()
	term = rf.currentTerm
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
	rf.mu.Lock()
	rf.stop = true
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
	rf.votedFor = -1
	rf.voted = 0
	rf.currentTerm = 0
	rf.state = 1
	rf.stop = false
	rf.leaderChan = make(chan bool)
	rf.heartbeatChan = make(chan bool)
	rf.log = make([]LogEntity, 0)

	// Your initialization code here (2A, 2B, 2C).
	log.Printf("server id %d starting", me)
	rf.resetElectTimeout()
	go rf.servers()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) servers() {
	for {
		if rf.stop {
			return
		}
		log.Printf("node:%d state :%d", rf.me, rf.state)
		if rf.state == 1 {
			rf.followerProc()
		} else if rf.state == 2 {
			rf.candidateProc()
		} else if rf.state == 3 {
			rf.leaderProc()
		}
	}
}

func (rf *Raft) followerProc() {
	select {
	case <-time.Tick(rf.electionTimeout):
		rf.mu.Lock()
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.heartbeatChan:

	case <-rf.voteChan:

	}
}

func (rf *Raft) candidateProc() {
	rf.broadcastRequestVote()
	select {
	case <-time.Tick(rf.electionTimeout):
		rf.mu.Lock()
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.heartbeatChan:

	case <-rf.leaderChan:
		log.Printf("leaderchan ok, serverid:%d", rf.me)
	}
}

func (rf *Raft) leaderProc() {
	rf.broadcastAppendEntries()
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) turnCandidate() {
	rf.state = 2
	rf.votedFor = rf.me
	rf.voted = 1
	rf.currentTerm++
	rf.resetElectTimeout()
}

func (rf *Raft) turnFollower(term int, leaderId int) {
	rf.state = 1
	rf.votedFor = -1
	rf.voted = 0
	rf.currentTerm = term
	rf.leader = leaderId
}

func (rf *Raft) resetElectTimeout() {
	rf.electionTimeout = time.Duration(500+rand.Int63n(350)) * time.Millisecond
	log.Printf("serverId:%d, term:%d, electionTimeout:%d", rf.me, rf.currentTerm, rf.electionTimeout)
}

func (rf *Raft) turnLeader() {
	log.Printf("server_id:%d to leader", rf.me)
	rf.state = 3
	entityLen := len(rf.log)
	if rf.nextIndex == nil {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
	} else {
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = entityLen
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go func(serverId int) {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			reply := &AppendEntriesReply{}
			if len(rf.log) > rf.nextIndex[serverId] {
				//需要同步
				args.Entries = rf.log[rf.nextIndex[serverId]:]
				if rf.nextIndex[serverId] == 0 {
					args.PreLogIndex = -1
					args.PreLogTerm = rf.currentTerm
				} else {
					args.PreLogIndex = rf.nextIndex[serverId] - 1
					args.PreLogTerm = rf.log[rf.nextIndex[serverId]-1].term
				}
				args.LeaderCommitId = rf.commitIndex
				ok := rf.sendRequestAppendEntries(serverId, args, reply)
				if ok {
					rf.mu.Lock()
					if reply.Success {
						//成功
						rf.nextIndex[serverId] = rf.nextIndex[serverId] + len(args.Entries)
						rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
						count := 0
						if rf.matchIndex[serverId] <= rf.commitIndex {
							return
						}
						for j := 0; j < len(rf.peers); j++ {
							if rf.matchIndex[j] == rf.matchIndex[serverId] {
								count++
								if count > len(rf.peers)/2 {
									//可以提交
									rf.commitIndex = rf.matchIndex[serverId]
								}
							}
						}
					} else if reply.Term > rf.currentTerm {
						//任期已过期
						rf.turnFollower(reply.Term, serverId)
					} else {
						rf.nextIndex[serverId]--
					}
					rf.mu.Unlock()
				}
			} else {
				//心跳消息
				ok := rf.sendRequestAppendEntries(serverId, args, reply)
				if ok && reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.turnFollower(reply.Term, i)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

func (rf *Raft) broadcastRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go func(serverId int) {
			voteArgs := &RequestVoteArgs{rf.currentTerm, rf.me}
			voteReply := &RequestVoteReply{}
			if rf.sendRequestVote(serverId, voteArgs, voteReply) && voteReply.VoteGranted {
				rf.mu.Lock()
				if rf.state == 2 {
					rf.voted = rf.voted + 1
					//log.Printf("server :%d voted:%t,votedNum:%d", serverId, voteReply.VoteGranted,rf.voted)
					if rf.voted > len(rf.peers)/2 {
						log.Printf("server:%d success", serverId)
						rf.turnLeader()
						notify(rf.leaderChan)
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}
