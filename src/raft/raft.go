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
	//	"bytes"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
)

type machineIdentity int

// type voteResult int
type AppendEntriesResult int
type appendEntriesRejectedReason int

const earlyWakeUp time.Duration = 1 * time.Millisecond

const (
	follower machineIdentity = iota
	candidate
	leader
)

// const (
//
//	voteSuccess voteResult = iota
//	voteFail
//	voteTimeout
//	voteNewLeaderFound
//
// )
const (
	appendEntriesSuccess AppendEntriesResult = iota
	appendEntriesFailNetwork
	appendEntriesFailRejected
)
const (
	termBehind appendEntriesRejectedReason = iota
	unmatchedPrevLogIndex
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntriesArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int
	Entries        []*LogEntry
	LeaderCommit   int
	HeartbeatIndex int
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	RejectedReason appendEntriesRejectedReason
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}

	ReplyCh  chan AppendEntriesResult
	StopCh   chan struct{}
	CommitCh chan bool
}

type appendEntriesRPCSender struct {
	peerId            int
	notificationCh    chan struct{}
	identityChangedCh chan machineIdentity
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	meIdentity        machineIdentity      // current identity of the server
	currTerm          int                  // latest term server has seen
	votedFor          int                  // candidateId that received vote in current term
	log               []*LogEntry          // log entries
	commitIndex       int                  // index of highest log entry known to be committed
	lastApplied       int                  // index of highest log entry applied to state machine
	nextWakeUp        time.Time            // next time to wake up
	identityChangedCh chan machineIdentity // channel to notify identity change

	// volatile state on leaders
	nextIndex                  []int                     // for each server, index of the next log entry to send to that server
	matchIndex                 []int                     // for each server, index of highest log entry known to be replicated on server
	applyCh                    chan ApplyMsg             // channel to send ApplyMsg to client
	processedIndex             int                       // index of highest log entry that has been sent to followers
	clientCh                   chan interface{}          // channel to send log entries to main loop
	appendEntriesRPCSenders    []*appendEntriesRPCSender // senders for appendEntries RPC
	heartbeatIndex             int                       // index of next heartbeat to send
	receiveReplyNotificationCh chan struct{}             // channel to notify receive reply from followers

	// volatile state on followers
	receivedHeartbeatCh chan struct{} // channel to check if received heartbeat from leader
}

func getNextHeartbeatTimeout() time.Duration {
	// pause for a random amount of time between 50 and 300
	// milliseconds.
	ms := 50 + (rand.Int63() % 250)
	return (time.Duration(ms) * time.Millisecond)
}

func getNextFollowerTimeout() time.Duration {
	// pause for a random amount of time between 300 and 450
	// milliseconds.
	ms := 300 + (rand.Int63() % 150)
	return (time.Duration(ms) * time.Millisecond)
}

func getNextWaitForReplyTimeout() time.Duration {
	// pause for a random amount of time 400
	// milliseconds.
	ms := 400
	return (time.Duration(ms) * time.Millisecond)
}

func getNextElectionTimeout() time.Duration {
	// pause for a random amount of time between 150 and 300
	// milliseconds.
	ms := 150 + (rand.Int63() % 150)
	return (time.Duration(ms) * time.Millisecond)
}

func getRetryTimeout() time.Duration {
	ms := 50
	return (time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) checkIfLeaderDisconnectedFromTheNetwork(identityChangedCh chan machineIdentity, receiveReplyNotificationCh chan struct{}) {
	disconnectTk := time.NewTicker(getNextWaitForReplyTimeout())
	defer disconnectTk.Stop()

	for {
		select {

		case <-identityChangedCh:
			return

		default:
			select {

			case <-identityChangedCh:
				return

			case <-disconnectTk.C:
				rf.mu.Lock()
				// change identity to follower
				rf.meIdentity = follower
				log.Println("Leader ", rf.me, " disconnect the network, change to follower")
				rf.mu.Unlock()

				close(identityChangedCh)

				return

			case <-receiveReplyNotificationCh:
				disconnectTk.Reset(getNextWaitForReplyTimeout())

			}
		}
	}
}

func (rf *Raft) startAppendEntriesSender(identityChangedCh chan machineIdentity, receiveReplyNotificationCh chan struct{}, sender *appendEntriesRPCSender) {
	// send heartbeat immediately
	senderTk := time.NewTicker(1 * time.Nanosecond)
	defer senderTk.Stop()

	for {
		select {

		case <-identityChangedCh:
			return

		default:
			select {

			case <-identityChangedCh:
				return

			case <-senderTk.C:
				// send heartbeat
				rf.mu.Lock()

				// construct args and reply
				// todo: 检查是否需要发送落后的entries
				var args = AppendEntriesArgs{
					Term:           rf.currTerm,
					LeaderId:       rf.me,
					PrevLogIndex:   rf.matchIndex[sender.peerId],
					PrevLogTerm:    rf.log[rf.matchIndex[sender.peerId]].Term,
					Entries:        nil,
					LeaderCommit:   rf.commitIndex,
					HeartbeatIndex: rf.heartbeatIndex,
				}
				var reply = AppendEntriesReply{}

				rf.mu.Unlock()

				// send AppendEntries RPC
				log.Println("Leader ", rf.me, " prepares to send AppendEntries RPC heartbeat: ", args.HeartbeatIndex, " to follower: ", sender.peerId, " args.term: ", args.Term)
				ok := rf.peers[sender.peerId].Call("Raft.AppendEntries", &args, &reply)
				log.Println("Leader ", rf.me, " sent AppendEntries RPC heartbeat: ", args.HeartbeatIndex, " to follower: ", sender.peerId, " leader term: ", args.Term, " network ok: ", ok, " follower term: ", reply.Term, " success: ", reply.Success)

				if !ok {
					// network failure, retry after a short pause
					senderTk.Reset(getRetryTimeout())
					continue
				}

				// notify checkIfLeaderDisconnectedFromTheNetwork goroutine
				receiveReplyNotificationCh <- struct{}{}

				// handle reply
				needRetry := rf.handleAppendEntriesReply(sender, nil, &args, &reply)
				if needRetry {
					// retry after a short pause
					senderTk.Reset(getRetryTimeout())
					continue
				}
				// don't need to retry, just reset timer
				senderTk.Reset(time.Until(rf.nextWakeUp))

			case <-sender.notificationCh:
				// send AppendEntries RPC
				rf.mu.Lock()

				// get the largest index of log that can be sent
				currProcessedIndex := rf.processedIndex

				// get prevLogIndex of this follower
				var prevLogIndex int
				prevLogIndex = rf.nextIndex[sender.peerId] - 1

				// get prevLogTerm of this follower
				var prevLogTerm int
				prevLogTerm = rf.log[prevLogIndex].Term

				// get nextIndex
				var nextIndex int
				nextIndex = rf.nextIndex[sender.peerId]

				// get entries to send
				var entries []*LogEntry
				entries = rf.log[nextIndex : currProcessedIndex+1]

				// construct args and reply
				var args = AppendEntriesArgs{
					Term:           rf.currTerm,
					LeaderId:       rf.me,
					PrevLogIndex:   prevLogIndex,
					PrevLogTerm:    prevLogTerm,
					Entries:        entries,
					LeaderCommit:   rf.commitIndex,
					HeartbeatIndex: 0,
				}
				var reply = AppendEntriesReply{}

				rf.mu.Unlock()

				// send AppendEntries RPC
				ok := rf.peers[sender.peerId].Call("Raft.AppendEntries", &args, &reply)

				if !ok {
					// network failure, retry after a short pause
					senderTk.Reset(getRetryTimeout())
					continue
				}

				// notify checkIfLeaderDisconnectedFromTheNetwork goroutine
				receiveReplyNotificationCh <- struct{}{}

				// handle reply
				// get last log entry
				var lastLogEntry *LogEntry
				lastLogEntry = entries[len(entries)-1]
				needRetry := rf.handleAppendEntriesReply(sender, lastLogEntry, &args, &reply)

				if needRetry {
					// retry after a short pause
					senderTk.Reset(getRetryTimeout())
					continue
				}
				// don't need to retry, just reset timer
				senderTk.Reset(time.Until(rf.nextWakeUp))
			}
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(sender *appendEntriesRPCSender, lastLogEntry *LogEntry, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// success
	if reply.Success {
		if args.HeartbeatIndex != 0 {
			// heartbeat success, don't need to update nextIndex and matchIndex
			return false
		}
		// nextIndex and matchIndex
		nextIndex := lastLogEntry.Index + 1
		matchIndex := lastLogEntry.Index

		// check if need to reply to main loop
		select {
		case _, ok := <-lastLogEntry.StopCh:
			// stopCh was closed, main loop has stopped waiting for reply
			if !ok {
				// check if the last log entry is committed (check stopCh closed because of success or failure)
				if lastLogEntry.Index > rf.commitIndex {
					// not committed, failed
					// only update the committed index range
					rf.nextIndex[sender.peerId] = nextIndex - 1
					rf.matchIndex[sender.peerId] = matchIndex - 1
				} else {
					// committed, success
					// update nextIndex and matchIndex
					rf.nextIndex[sender.peerId] = nextIndex
					rf.matchIndex[sender.peerId] = matchIndex
				}
			}

			return false

		default:
			// stopCh is still open, main loop is still waiting for reply
			lastLogEntry.ReplyCh <- appendEntriesSuccess
			// update nextIndex and matchIndex
			rf.nextIndex[sender.peerId] = nextIndex
			rf.matchIndex[sender.peerId] = matchIndex

			return false
		}
	}

	// fail
	switch reply.RejectedReason {
	case termBehind:
		if reply.Term > rf.currTerm {
			// update term
			rf.currTerm = reply.Term
		}

		// term falls behind, return retry = true, continue next loop
		// if the leader is really out of date, the goroutine will hit the identity change signal in next loop
		// if the leader is not out of date, the goroutine will hit the retry timeout in next loop
		return true

	case unmatchedPrevLogIndex:
		// update nextIndex and matchIndex
		rf.nextIndex[sender.peerId]--
		if rf.nextIndex[sender.peerId] <= rf.matchIndex[sender.peerId] {
			rf.matchIndex[sender.peerId]--
		}

		// return retry = true, continue next loop
		return true
	}

	return false

}

// timeoutUpdater updates the nextWakeUp time of all goroutines
func (rf *Raft) timeoutUpdater(wg *sync.WaitGroup) {

	rf.mu.Lock()
	// hold the identity channel, prevent the original channel from being reassigned
	identityChangedCh := rf.identityChangedCh
	rf.mu.Unlock()

	nextWakeUp := time.Now().Add(getNextHeartbeatTimeout())
	// timeoutUpdater should wake up earlier to update the next timeout
	tk := time.NewTicker(time.Until(nextWakeUp.Add(-earlyWakeUp)))
	defer tk.Stop()
	// no need to lock here, as only one goroutine is updating the nextWakeUp
	rf.nextWakeUp = nextWakeUp
	wg.Done()

	for {
		select {
		case <-tk.C:
			nextWakeUp = time.Now().Add(getNextHeartbeatTimeout())
			// timeoutUpdater should wake up earlier to update the next timeout
			tk.Reset(time.Until(nextWakeUp.Add(-earlyWakeUp)))
			// no need to lock here, as only one goroutine is updating the nextWakeUp
			rf.nextWakeUp = nextWakeUp

		case <-identityChangedCh:
			return
		}
	}
}

// candidateLoop is the main loop for the candidate server
func (rf *Raft) candidateLoop() {
	rf.mu.Lock()
	rf.initLoop()

	// hold the channel, prevent the original channel from being reassigned
	identityChangedCh := rf.identityChangedCh
	// update term
	rf.currTerm++
	// vote for self
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	voteGrantedCount := 1
	voteFailCount := 0

	// send RequestVote RPC to all peers (async)
	voteCollectCh := make(chan RequestVoteReply, len(rf.peers))
	stopCh := make(chan struct{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, &RequestVoteReply{}, voteCollectCh, stopCh)
	}

	candidateTk := time.NewTicker(getNextElectionTimeout())
	defer candidateTk.Stop()

	for {
		select {

		case <-identityChangedCh:
			return

		default:
			select {

			case <-identityChangedCh:
				return
			case reply := <-voteCollectCh:
				if !reply.OK {
					// network failure, ignore the reply
					// if replies are not enough, will start next election
					continue
				}

				if reply.VoteGranted {
					voteGrantedCount++
					if voteGrantedCount > len(rf.peers)/2 {
						// become leader
						rf.mu.Lock()
						rf.meIdentity = leader
						log.Printf("Server %d becomes leader\n", rf.me)
						rf.mu.Unlock()

						close(stopCh)
						close(identityChangedCh)

						return
					}
				} else {
					voteFailCount++
					if reply.Term > rf.currTerm || voteFailCount > len(rf.peers)/2 {
						// election failed
						// no matter the reasons(term falls behind or incomplete logs), this server can't be leader in this term
						rf.mu.Lock()
						rf.meIdentity = follower
						// update term if failed because of term falls behind
						if reply.Term > rf.currTerm {
							rf.currTerm = reply.Term
						}
						rf.mu.Unlock()

						close(stopCh)
						close(identityChangedCh)

						return
					}
				}

			case <-candidateTk.C:
				// election timeout, start next election
				close(stopCh)
				return

			}
		}
	}

}

// followerLoop is the main loop for the follower server
func (rf *Raft) followerLoop() {

	rf.mu.Lock()
	rf.initLoop()

	// hold the channels, prevent the original channels from being reassigned
	identityChangedCh := rf.identityChangedCh
	receivedHeartbeatCh := rf.receivedHeartbeatCh

	rf.mu.Unlock()

	followerTk := time.NewTicker(getNextFollowerTimeout())
	defer followerTk.Stop()

	for {
		select {
		case <-followerTk.C:
			rf.mu.Lock()
			// timeout, update identity to candidate
			rf.meIdentity = candidate
			rf.mu.Unlock()

			log.Printf("Server %d timeout, change to candidate\n", rf.me)
			close(identityChangedCh)

			return

		case <-receivedHeartbeatCh:
			followerTk.Reset(getNextFollowerTimeout())
		}
	}
}

// leaderLoop is the main loop for the leader server
func (rf *Raft) leaderLoop() {

	rf.mu.Lock()
	rf.initLoop()

	// hold the identity channels, prevent the original channels from being reassigned
	identityChangedCh := rf.identityChangedCh
	receiveReplyNotificationCh := rf.receiveReplyNotificationCh
	rf.mu.Unlock()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go rf.timeoutUpdater(&wg)

	wg.Wait()

	leaderTk := time.NewTicker(time.Until(rf.nextWakeUp))
	defer leaderTk.Stop()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.startAppendEntriesSender(identityChangedCh, receiveReplyNotificationCh, rf.appendEntriesRPCSenders[i])
	}

	go rf.checkIfLeaderDisconnectedFromTheNetwork(identityChangedCh, receiveReplyNotificationCh)

	for {
		select {
		case <-identityChangedCh:
			return

		default:
			select {

			case <-identityChangedCh:
				return

			case <-leaderTk.C:
				// update heartbeat index
				rf.heartbeatIndex++

			case command := <-rf.clientCh:
				rf.mu.Lock()
				// append log entry to log
				logEntry := LogEntry{
					Term:    rf.currTerm,
					Index:   rf.commitIndex + 1,
					Command: command,

					ReplyCh:  make(chan AppendEntriesResult),
					StopCh:   make(chan struct{}),
					CommitCh: make(chan bool),
				}

				rf.log = append(rf.log, &logEntry)
				rf.processedIndex++

				// notify all AppendEntries RPC senders
				for _, sender := range rf.appendEntriesRPCSenders {
					sender.notificationCh <- struct{}{}
				}

				rf.mu.Unlock()

				// wait for replies
				// no need to start a new goroutine here, as the senders are not blocked
				rf.waitForAppendEntriesReply(identityChangedCh, &logEntry)

			}
		}
	}
}

func (rf *Raft) waitForAppendEntriesReply(identityChangedCh chan machineIdentity, logEntry *LogEntry) {
	commitCh := logEntry.CommitCh
	replyCh := logEntry.ReplyCh
	stopCh := logEntry.StopCh

	currSuccessReplyCount := 0
	currFailReplyRejectedCount := 0

	// if timeout, will change identity to follower
	replyTicker := time.NewTicker(getNextWaitForReplyTimeout())
	defer replyTicker.Stop()

	// CollectLogReply:
	for {
		select {

		case <-identityChangedCh:
			// identity changed
			close(stopCh)
			commitCh <- false
			return

		default:
			select {

			case <-identityChangedCh:
				// identity changed
				close(stopCh)
				commitCh <- false
				return

			case replyResult := <-replyCh:
				switch replyResult {
				case appendEntriesSuccess:
					currSuccessReplyCount++
					// received more than half of the success replies
					if currSuccessReplyCount > len(rf.peers)/2 {
						rf.mu.Lock()
						// update commitIndex
						rf.commitIndex = logEntry.Index
						rf.mu.Unlock()

						close(stopCh)
						commitCh <- true

						return
					}

				case appendEntriesFailRejected:
					currFailReplyRejectedCount++
					// received more than half of the rejected replies
					if currFailReplyRejectedCount > len(rf.peers)/2 {
						// don't change identity here, identity change from leader to follower should be done in AppendEntries RPC
						close(stopCh)
						commitCh <- false
						return
					}

				case appendEntriesFailNetwork:
					// do nothing
				}

			case <-replyTicker.C:
				// timeout
				rf.mu.Lock()
				// change identity to follower
				if rf.meIdentity == leader {
					rf.meIdentity = follower
					close(identityChangedCh)
				}

				rf.mu.Unlock()

				log.Println("Leader ", rf.me, " wait for AppendEntries reply timeout, change to follower")
				commitCh <- false
				close(stopCh)

				return
			}
		}
	}
}

func (rf *Raft) sendRequestVote(peerId int, args *RequestVoteArgs, reply *RequestVoteReply, voteCollectCh chan RequestVoteReply, stopCh chan struct{}) {
	ok := rf.peers[peerId].Call("Raft.RequestVote", args, reply)

	reply.OK = ok

	if ok {
		log.Println("Candidate ", rf.me, " electionTerm: ", args.Term, " received vote reply from ", peerId, "reply term: ", reply.Term, "voted: ", reply.VoteGranted)
	} else {
		log.Println("Candidate ", rf.me, " electionTerm: ", args.Term, " did not receive reply from ", peerId, "cause of network failure")
	}
	select {
	case <-stopCh:
		// stop signal received, ignore the reply
		return
	default:
		voteCollectCh <- *reply
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currTerm

	if args.Term < rf.currTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currTerm {
		rf.currTerm = args.Term
		rf.votedFor = -1
		if rf.meIdentity == leader || rf.meIdentity == candidate {
			rf.meIdentity = follower
			close(rf.identityChangedCh)
		}
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if the candidate's log is up-to-date
		selfLastLogEntry := rf.log[len(rf.log)-1]
		selfLastLogIndex := selfLastLogEntry.Index
		selfLastLogTerm := selfLastLogEntry.Term

		if args.LastLogTerm > selfLastLogTerm || (args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex) {
			// vote for this candidate
			rf.votedFor = args.CandidateId

			reply.VoteGranted = true
			return
		}
	}

	// rf.votedFor != -1 && rf.votedFor != args.CandidateId
	// voted for other candidate in this term
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	originalIdentity := rf.meIdentity

	if rf.meIdentity == leader {
		// check term
		if args.Term <= rf.currTerm {
			// ignore the request
			reply.Term = rf.currTerm
			reply.Success = false
			reply.RejectedReason = termBehind
			log.Println("Server ", rf.me, "self identity: ", rf.meIdentity, " self term: ", rf.currTerm, " received AppendEntries RPC from ", args.LeaderId, "args.term: ", args.Term, "args.heartbeat: ", args.HeartbeatIndex, " rejected because of term behind")
			return
		}

		rf.meIdentity = follower
		close(rf.identityChangedCh)
	} else if rf.meIdentity == candidate {
		// check term
		if args.Term < rf.currTerm {
			// ignore the request
			reply.Term = rf.currTerm
			reply.Success = false
			reply.RejectedReason = termBehind
			log.Println("Server ", rf.me, "self identity: ", rf.meIdentity, " self term: ", rf.currTerm, " received AppendEntries RPC from ", args.LeaderId, "args.term: ", args.Term, "args.heartbeat: ", args.HeartbeatIndex, " rejected because of term behind")
			return
		}

		rf.meIdentity = follower
		close(rf.identityChangedCh)
	} else if rf.meIdentity == follower {
		// check term
		if args.Term < rf.currTerm {
			// ignore the request
			reply.Term = rf.currTerm
			reply.Success = false
			reply.RejectedReason = termBehind
			log.Println("Server ", rf.me, "self identity: ", rf.meIdentity, " self term: ", rf.currTerm, " received AppendEntries RPC from ", args.LeaderId, "args.term: ", args.Term, "args.heartbeat: ", args.HeartbeatIndex, " rejected because of term behind")
			return
		}
	}

	// notify the follower loop
	if originalIdentity == follower {
		rf.receivedHeartbeatCh <- struct{}{}
	}

	// update follower info
	rf.currTerm = args.Term // assert args.Term >= rf.currTerm
	rf.votedFor = args.LeaderId

	// check prev log index
	largestReceivedIndex := rf.log[len(rf.log)-1].Index
	// if rf.log has not-committed log entry, wont hit the if condition, then the not-committed log entry will be covered
	if args.PrevLogIndex > largestReceivedIndex || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = rf.currTerm
		reply.Success = false
		reply.RejectedReason = unmatchedPrevLogIndex
		log.Println("Server ", rf.me, " received AppendEntries RPC from ", args.LeaderId, "args.term: ", args.Term, "args.heartbeat: ", args.HeartbeatIndex, " rejected because of unmatched prevLogIndex")
		return
	}

	log.Println("Server ", rf.me, " received AppendEntries RPC from ", args.LeaderId, "args.term: ", args.Term, "args.heartbeat: ", args.HeartbeatIndex, " success")

	// success
	reply.Term = rf.currTerm
	reply.Success = true

	// heartbeat
	if args.Entries == nil {
		return
	}

	// update log
	newEntries := []*LogEntry{}
	for i := 0; i < len(args.Entries); i++ {
		entryFromLeader := args.Entries[i]

		entry := LogEntry{
			Term:    entryFromLeader.Term,
			Index:   entryFromLeader.Index,
			Command: entryFromLeader.Command,
		}

		if entryFromLeader.Index > largestReceivedIndex {
			// new log entry, append to temp slice
			newEntries = append(newEntries, entryFromLeader)
		} else {
			// cover the old log entry
			rf.setLog(entry.Index, &entry)
		}
	}

	// append new log entries
	rf.log = append(rf.log, newEntries...)
	// update commit index
	rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log[len(rf.log)-1].Index)))

	// apply (todo: 是不是没有apply被覆盖的log)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getLog(i).Command,
			CommandIndex: rf.getLog(i).Index,
		}
		rf.applyCh <- msg
	}

	// update lastApplied
	rf.lastApplied = rf.commitIndex

}

func (rf *Raft) getLog(index int) *LogEntry {
	return rf.log[index]
}

func (rf *Raft) setLog(index int, entry *LogEntry) {
	rf.log[index] = entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currTerm
	isleader = rf.meIdentity == leader
	return term, isleader
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
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
	// OK for network failure
	OK bool
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

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		identity := ""
		switch rf.meIdentity {
		case leader:
			identity = "leader"
		case candidate:
			identity = "candidate"
		case follower:
			identity = "follower"
		}

		log.Println("Server ", rf.me, " new identity: ", identity, "currTerm: ", rf.currTerm)

		if rf.meIdentity == leader {
			rf.leaderLoop()
		} else if rf.meIdentity == candidate {
			rf.candidateLoop()
		} else if rf.meIdentity == follower {
			rf.followerLoop()
		}

	}
}

func (rf *Raft) initLoop() {
	switch rf.meIdentity {
	case leader:
		rf.clientCh = make(chan interface{})
		identityChangedCh := make(chan machineIdentity)
		rf.identityChangedCh = identityChangedCh
		rf.receivedHeartbeatCh = nil
		rf.heartbeatIndex = 1
		rf.receiveReplyNotificationCh = make(chan struct{}, len(rf.peers))

		rf.appendEntriesRPCSenders = make([]*appendEntriesRPCSender, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.appendEntriesRPCSenders[i] = &appendEntriesRPCSender{
				peerId:            i,
				notificationCh:    make(chan struct{}),
				identityChangedCh: identityChangedCh,
			}
		}

	case candidate:
		rf.clientCh = nil
		rf.appendEntriesRPCSenders = nil
		rf.receivedHeartbeatCh = nil
		rf.identityChangedCh = make(chan machineIdentity)
		rf.receiveReplyNotificationCh = nil

	case follower:
		rf.clientCh = nil
		rf.appendEntriesRPCSenders = nil
		rf.receivedHeartbeatCh = make(chan struct{}, 1)
		rf.identityChangedCh = make(chan machineIdentity)
		rf.receiveReplyNotificationCh = nil
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.meIdentity = follower
	rf.currTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{{Term: 0, Index: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	// rf.nextWakeUp = time.Now().Add(getNextFollowerTimeout())
	rf.identityChangedCh = make(chan machineIdentity)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.processedIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
