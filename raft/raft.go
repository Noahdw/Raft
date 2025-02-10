package raft

import (
	"fmt"
	"math/rand/v2"
	"net/rpc"
	"sync"
	"time"
)

type ElectionState int

const (
	Follower ElectionState = iota
	Candidate
	Leader
)

type log struct {
	entries []logEntry
}

func (l *log) lastTerm() int {
	len := len(l.entries)
	if len > 0 {
		return l.entries[len-1].term
	}
	return 0
}

func (l *log) lastIndex() int {
	return len(l.entries) - 1
}

func (l *log) termAtIndexMatches(index int, termToMatch int) bool {
	if len(l.entries) <= index {
		return false
	}
	return l.entries[index].term == termToMatch
}

type logEntry struct {
	term     int
	logIndex int
	command  interface{}
}

type State struct {
	electionState ElectionState
	heartbeat     time.Duration
	electionReset chan bool

	// Persistent state on all servers

	log         log // contains log entries
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term
	id          int // id for this node

	// Volatile state on all servers

	commitIndex int // index of highest log entry know to be commited
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leader

	// for each server, index of the next log entry to send to that server
	nextindex []int
	// for each server, index of highest log entry known to be replicated on server
	matchindex []int
}

func (r *raft) Start() {
	// Start heartbeat/election tickers
	go r.ticker()

	// Begin listening for RPC calls from other raft servers
	go r.serveRaft()
}

type raft struct {
	state      State
	rpcClient  []*rpc.Client
	numOfPeers int
}

type RaftConfig struct {
	// index into raftAddresses this node belongs to
	raftId int
	// addresses for raft, including own
	raftAddresses []string
}

func NewRaft(config RaftConfig) *raft {
	clients := createRpcClients(&config.raftAddresses, config.raftId)
	numOfPeers := len(clients)

	state := State{
		electionState: Follower,
		heartbeat:     time.Duration(150) * time.Millisecond,
		id:            config.raftId,
		nextindex:     make([]int, numOfPeers),
		matchindex:    make([]int, numOfPeers),
	}

	r := raft{
		state:      state,
		rpcClient:  clients,
		numOfPeers: numOfPeers,
	}
	return &r
}

func createRpcClients(raftAddresses *[]string, ownId int) []*rpc.Client {
	numPeers := len(*raftAddresses) - 1
	clients := make([]*rpc.Client, numPeers)
	index := 0
	for i, addr := range *raftAddresses {
		if i == ownId {
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			// FIXME do more
			fmt.Printf("Error in rcp Dial\n")
		} else {
			clients[index] = client
		}
		index++
	}
	return clients
}

func (r *raft) TryCommand(command interface{}) {
	if r.state.electionState == Leader {
		entry := logEntry{
			term:     r.state.currentTerm,
			command:  command,
			logIndex: len(r.state.log.entries),
		}
		r.state.log.entries = append(r.state.log.entries, entry)
		r.requestAppendEntries(
			r.state.currentTerm,
			r.state.id,
			r.state.log.lastIndex(),
			r.state.log.lastTerm(),
			r.state.commitIndex,
			[]logEntry{entry},
		)
	}
}

type appendEntriesArgs struct {
	leaderTerm   int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	leaderCommit int
	entries      []logEntry
}

type appendEntriesReply struct {
	term    int
	success bool
}

// FIXME: Current code assumes sequential command processing. Make async
func (r *raft) requestAppendEntries(
	leaderTerm int,
	leaderId int,
	prevLogIndex int,
	prevLogTerm int,
	leaderCommit int,
	entries []logEntry) {

	var wg sync.WaitGroup
	ch := make(chan appendEntriesReply, r.numOfPeers)
	wg.Add(r.numOfPeers)

	for i := 0; i < r.numOfPeers; i++ {
		go func(index int, curTerm int) {
			defer wg.Done()

			args := appendEntriesArgs{
				leaderTerm:   leaderTerm,
				leaderId:     leaderId,
				prevLogIndex: prevLogIndex,
				prevLogTerm:  prevLogTerm,
				leaderCommit: leaderCommit,
				entries:      entries,
			}

			reply := appendEntriesReply{
				term:    0,
				success: false,
			}
			for {
				err := r.rpcClient[i].Call("appendEntries", args, &reply)
				if err != nil {
					fmt.Printf("appendEntries error %s", err.Error())
				}
				if reply.success || reply.term > curTerm {
					break
				}
				r.state.nextindex[i] -= 1
				args.prevLogIndex = r.state.nextindex[i]
				args.entries = r.state.log.entries[r.state.nextindex[i]:]
			}

			ch <- reply
		}(i, r.state.currentTerm)
	}

	wg.Wait()

	for i := 0; i < r.numOfPeers; i++ {
		reply := <-ch
		if reply.term > r.state.currentTerm {
			// Found a higher term. Leader no more, become follower.
			r.state.currentTerm = reply.term
			r.state.electionState = Follower
		}
	}
}

func (r *raft) appendEntries(args *appendEntriesArgs, reply *appendEntriesReply) error {
	reply.term = r.state.currentTerm

	// Stale leader
	if args.leaderTerm < r.state.currentTerm {
		reply.success = false
		return nil
	}

	r.state.electionReset <- true
	r.state.electionState = Follower

	// Not in line with leader at previous entries
	// Leader will retry with index-1
	if !r.state.log.termAtIndexMatches(args.prevLogIndex, args.prevLogTerm) {
		r.persistState()
		reply.success = false
		return nil
	}

	// If we get here, we know args.prevLogIndex is the last known good
	// location for our log. We might have extras though that are not good.

	// If there are existing entries that do not match the term of the new entries,
	// delete all existing entries after the first bad one
	curLastIndex := r.state.log.lastIndex()
	amountToCheck := min(len(args.entries), curLastIndex-args.prevLogIndex)
	for i := 0; i < amountToCheck; i++ {
		checkIndex := args.prevLogIndex + i + 1
		newEntryTerm := args.entries[i]
		curEntryTerm := r.state.log.entries[checkIndex]
		if newEntryTerm != curEntryTerm {
			r.state.log.entries = r.state.log.entries[:checkIndex]
			r.state.log.entries = append(r.state.log.entries, args.entries[i:]...)
			break
		}

	}

	if args.leaderCommit > r.state.commitIndex {
		r.state.commitIndex = min(args.leaderCommit, r.state.log.lastIndex())
	}
	r.persistState()
	reply.success = true
	return nil
}

type requestVoteArgs struct {
	electionTerm int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

type requestVoteReply struct {
	term        int
	voteGranted bool
}

// Called by a candidate to request votes to become leader
// Initiates the RPC call to requestVote.
// Returns number of votes granted
func (r *raft) requestRequestVote(term int, candidateId int, lastLogIndex int, lastLogTerm int) int {
	args := requestVoteArgs{
		electionTerm: term,
		candidateId:  candidateId,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
	}

	var wg sync.WaitGroup
	wg.Add(r.numOfPeers)
	ch := make(chan (requestVoteReply), r.numOfPeers)
	defer close(ch)

	// Send requestVote to all clients
	// Gather num of votes received
	for i := 0; i < r.numOfPeers; i++ {
		go func(index int) {
			defer wg.Done()
			reply := requestVoteReply{
				term:        0,
				voteGranted: false,
			}
			// FIXME - need rpc timeout
			err := r.rpcClient[index].Call("requestVote", args, &reply)
			if err != nil {
				fmt.Printf("requestVote error %s", err.Error())
				return
			}
			ch <- reply

		}(i)
	}

	wg.Wait()

	votes := 0
	inElection := true
	for i := 0; i < r.numOfPeers; i++ {
		reply := <-ch
		if reply.term > r.state.currentTerm {
			// Become follower again if behind in term
			// Continue through all clients in case there is another greater term
			r.state.currentTerm = reply.term
			r.state.votedFor = 0
			r.state.electionState = Follower
			r.persistState()
			inElection = false
		} else if reply.voteGranted {
			votes += 1
		}
	}

	if !inElection {
		votes = 0
	}

	return votes
}

// Received by other nodes to respond to requests to become leader
// Not called directly. Only called via RPC
func (r *raft) requestVote(args *requestVoteArgs, reply *requestVoteReply) error {
	if args.electionTerm < r.state.currentTerm {
		// If our term is greater than the term of the request, update the requesters term and vote no.
		reply.term = r.state.currentTerm
		reply.voteGranted = false
		return nil
	}

	reply.term = args.electionTerm

	if args.electionTerm > r.state.currentTerm {
		r.state.currentTerm = args.electionTerm
		r.state.votedFor = 0
		r.state.electionState = Follower
		r.persistState()
	}

	newLogUpToDate := false
	if args.lastLogTerm > r.state.log.lastTerm() {
		newLogUpToDate = true
	} else if args.lastLogTerm == r.state.log.lastTerm() &&
		args.lastLogIndex >= r.state.log.lastIndex() {
		newLogUpToDate = true
	}

	// Otherwise grant vote if not yet voted in this term or if already voted for candidate
	if (r.state.votedFor == 0 || r.state.votedFor == args.candidateId) && newLogUpToDate {
		r.state.votedFor = args.candidateId
		r.persistState()
		reply.voteGranted = true
	} else {
		reply.voteGranted = false
	}
	return nil
}

func (r *raft) persistState() {

}
func (r *raft) ticker() {
	heartbeatTicker := time.NewTicker(r.state.heartbeat)
	electionTicker := time.NewTicker(getRandElectionTimeout())
	for {
		select {
		// Attempt to become leader
		case <-electionTicker.C:
			r.handleElectionTimeout()
		// Leader checks if other nodes are still available
		case <-heartbeatTicker.C:
			if r.state.electionState == Leader {
				r.sendHeatbeats()
			}
		case <-r.state.electionReset:
			electionTicker.Reset(getRandElectionTimeout())
		}
	}
}

func (r *raft) serveRaft() {

}

func (r *raft) handleElectionTimeout() {
	if (r.state.electionState == Follower && r.state.votedFor == 0) ||
		r.state.electionState == Candidate {

		r.state.currentTerm += 1      // Begin new election term
		r.state.votedFor = r.state.id // vote for self
		r.state.electionState = Candidate
		r.persistState()
		votes := r.requestRequestVote(
			r.state.currentTerm,
			r.state.id,
			r.state.lastApplied,
			r.state.log.lastTerm(),
		)
		if votes > r.numOfPeers/2-1 {
			// Election won, become leader
			r.state.electionState = Leader
			r.persistState()
			for i := range r.state.nextindex {
				r.state.nextindex[i] = r.state.log.lastIndex() + 1
			}
			r.sendHeatbeats()
		}
	}
}

// Send empty entries as a heartbeat,
// clients reset their election timeout on heartbeat
func (r *raft) sendHeatbeats() {

	r.requestAppendEntries(
		r.state.currentTerm,
		r.state.id,
		r.state.log.lastIndex(),
		r.state.log.lastTerm(),
		r.state.commitIndex,
		[]logEntry{})
}

func getRandElectionTimeout() time.Duration {
	return time.Duration(rand.IntN(100)+50) * time.Millisecond
}
