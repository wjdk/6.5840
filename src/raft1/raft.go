package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var debug = false
func DebugPrintf(format string, a ...interface{}) {
    if debug {
        fmt.Printf(format, a...)
    }
}

type Entry struct {
	Term    int        
	Command interface{}
}
type RaftState int
const (
	Follower  RaftState = iota
	Candidate
	Leader
)
type AppendEntriesArgs struct {
	Term 		 int
	LeaderId 	 int
	PrevLogIndex int 
	PrevLogTerm  int
	Entries 	 []Entry
	LeaderCommit int

}
type AppendEntriesReply struct {
	Term	int
	Success	bool
}
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	currentTerm int
	votedFor 	int 
	log 		[]Entry
	commitIndex int
	lastApplied int
	state RaftState
	lastHeartbeatTime time.Time   
	electionTimeout time.Duration
	commitCh chan int
	nextIndex  []int //for each server,index of the next log entry to send to that server
	// matchIndex []int //for each server,index of highest log entry known to be replicated on server, matchIndex = nextIndex-1
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term 
		rf.votedFor = -1
		if rf.state != Follower {
			DebugPrintf("%d change to follower, term: %d\n",rf.me,rf.currentTerm)
		}
		rf.state = Follower
	}
}
func (rf *Raft) ChangeToLeader() {
	if rf.state != Leader {
		rf.state = Leader
		DebugPrintf("leader: %d\n",rf.me)
		rf.nextIndex = make([]int,len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = 1
		}
		go rf.leaderTicker()
		go rf.appendChecker()
		go rf.commitChecker()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DebugPrintf("%d: Old leader!\n",rf.me)
		reply.Success = false
		return 
	}
	rf.lastHeartbeatTime = time.Now()
	rf.updateTerm(args.Term)
	if rf.state != Follower {
		DebugPrintf("%d change to follower, term: %d\n",rf.me,rf.currentTerm)
	}
	rf.state = Follower
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//DebugPrintf("%d: PrevLog wrong!\n",rf.me)
		reply.Success = false 
		return 
	}
	if len(args.Entries) >= 1 {
		DebugPrintf("%d append entries: %v\n",rf.me,args.Entries)
		// DebugPrintf("prevlogindex: %d\n",args.PrevLogIndex)
	}
	for i,entry := range args.Entries {
		logIndex := args.PrevLogIndex+1+i 
		if logIndex < len(rf.log) {
			if rf.log[logIndex].Term != entry.Term {
				rf.log = rf.log[:logIndex]
				rf.log = append(rf.log,entry)
			}
			
		} else{
			rf.log = append(rf.log,entry)
		}	
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex { 
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		//DebugPrintf("%d update commitIndex %d\n",rf.me,rf.commitIndex)
	}
	if len(args.Entries)>0 {
		//DebugPrintf("%d update log: %v\n",rf.me,rf.log)
	}
	return 

}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int 
	LastLogTerm int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false 
		return 
	} 
	rf.updateTerm(args.Term)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastlogterm := rf.log[len(rf.log)-1].Term
		if args.LastLogTerm > lastlogterm || (args.LastLogTerm == lastlogterm && args.LastLogIndex >= len(rf.log)-1) {
			rf.lastHeartbeatTime = time.Now()
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			//DebugPrintf("%d voted for %d\n",rf.me,rf.votedFor)
			return 
		}
	} 
	reply.VoteGranted = false
	return 

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state==Leader
	killed := rf.killed()
	rf.mu.Unlock()
	if killed || !isLeader  {
		return index, term, isLeader
	} 
	DebugPrintf("%d get command %v\n",rf.me,command)
	rf.mu.Lock()
	rf.log=append(rf.log,Entry{Term: rf.currentTerm, Command: command})
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	//DebugPrintf("%d start election: term %d, %v\n",rf.me,rf.currentTerm,time.Now().Sub(rf.lastHeartbeatTime))
	rf.state = Candidate
	rf.votedFor = rf.me 
	cnt := 1 //vote for self
	rf.lastHeartbeatTime = time.Now() //reset election timer
	rf.currentTerm = rf.currentTerm + 1 
	me := rf.me
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log)-1,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	for id,server := range(rf.peers) {
		if id == me {
			continue
		}
		go func(id int,server *labrpc.ClientEnd){
			reply := &RequestVoteReply{}
			ok := server.Call("Raft.RequestVote", args, reply)
			if !ok {
				//DebugPrintf("%d requeset vote %d fail! args:%v,reply:%v\n",rf.me,id,args,reply)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.updateTerm(reply.Term)
			if rf.state == Follower {
				return 
			}
			if reply.VoteGranted {
				cnt += 1
				if cnt >= (len(rf.peers)+1)/2 {
					rf.ChangeToLeader()
				}
			}
		}(id,server)
	}
	return 
}
func (rf *Raft) appendChecker() {
	for rf.killed() == false {
		for id,_ := range(rf.peers) {
			if id == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return 
			}
			rf.mu.Unlock()
			go func(serverId int){
				rf.mu.Lock()
				pos := rf.nextIndex[serverId]
				if pos > len(rf.log)-1{
					rf.mu.Unlock()
					return 
				}
				var entries []Entry
				for i := pos ; i <= len(rf.log)-1 ; i++ {
					entries = append(entries,rf.log[i])
				}
				args := AppendEntriesArgs{
					Term: 		  rf.currentTerm,
					LeaderId: 	  rf.me,
					PrevLogIndex: rf.nextIndex[serverId]-1,
					PrevLogTerm:  rf.log[rf.nextIndex[serverId]-1].Term,
					Entries: 	  entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				ok := rf.peers[serverId].Call("Raft.AppendEntries", &args, &reply)
				rf.mu.Lock()
				if !ok {
					DebugPrintf("%d to %d append entry failed!\n",rf.me,id)
					if rf.nextIndex[serverId] > 1 {
						rf.nextIndex[serverId] -= 1
					}
					rf.mu.Unlock()
					return 
				}
				if reply.Success {
					rf.nextIndex[serverId] = rf.nextIndex[serverId] + len(entries)
				} else{
					if rf.nextIndex[serverId] > 1 {
						rf.nextIndex[serverId] -= 1
					}
				}
				rf.updateTerm(reply.Term)
				if rf.state == Follower {
					rf.mu.Unlock()
					return 
				}
				rf.mu.Unlock()
			}(id)
			
		}
		time.Sleep(time.Duration(205) * time.Millisecond)
	}
}
func (rf *Raft) commitChecker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return 
		}
		cnt := 1
		for id,_ := range(rf.peers) {
			if id == rf.me {
				continue
			}
			if rf.nextIndex[id]-1 >= rf.commitIndex+1{
				cnt++
			}
		}
		if rf.commitIndex < len(rf.log)-1 && cnt*2 >= len(rf.peers){
			rf.commitIndex++
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}
func (rf *Raft) leaderTicker() {//给follower发送心跳信息
	for rf.killed() == false {
		for id,server := range(rf.peers) {
			//DebugPrintf("%d heartbeat to %d\n",rf.me,id)
			if id == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				DebugPrintf("%d is not leader!\n",rf.me)
				return 
			}
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: len(rf.log)-1,
				PrevLogTerm: rf.log[len(rf.log)-1].Term,
				Entries: []Entry{},
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			go func(targetServer *labrpc.ClientEnd, callArgs *AppendEntriesArgs) { 
                reply := &AppendEntriesReply{}
                ok := targetServer.Call("Raft.AppendEntries", callArgs, reply) 
				if !ok {
					//DebugPrintf("%d to %d append entry failed!\n",rf.me,id)
					return 
				}
				rf.mu.Lock()
                rf.updateTerm(reply.Term) 
				rf.mu.Unlock()
            }(server, args) 
			
		}
		time.Sleep(time.Duration(205) * time.Millisecond)
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		lastHeartbeatTime := rf.lastHeartbeatTime
		electionTimeout := rf.electionTimeout 
		rf.mu.Unlock()
		switch state {
		case Follower,Candidate:
			if time.Now().Sub(lastHeartbeatTime) > electionTimeout {
				go rf.startElection()
			}
		case Leader:
			
		}
		time.Sleep(time.Duration(10) * time.Millisecond) //每过很短的时间检查一次是否选举超时
	}
}
func (rf *Raft) apply(applyCh chan raftapi.ApplyMsg){
	// Check if a log entry can be commited.
	lastCommit := 0
	for rf.killed() == false {
		rf.mu.Lock()
		nowCommit := rf.commitIndex
		rf.mu.Unlock()
		for i := lastCommit+1; i<=nowCommit; i++ {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}			
			applyCh <- msg
			DebugPrintf("%d apply %d\n",rf.me,i)
		}
		lastCommit = nowCommit
		time.Sleep(time.Duration(50) * time.Millisecond) //每过很短的时间检查是否需要commit
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0 
	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{Term: 0, Command: nil} 
	minTimeout := 500 * time.Millisecond
	rf.electionTimeout = minTimeout + time.Duration(rand.Int63n(500)) * time.Millisecond
	DebugPrintf("%d timeout: %v\n",rf.me,rf.electionTimeout)
	rf.commitCh = make(chan int)   
	// fmt.Println(rf.electionTimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply(applyCh)
	return rf
}
