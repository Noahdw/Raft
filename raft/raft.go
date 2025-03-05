package raft

import (
	"fmt"
	"math/rand/v2"
	"net"
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

type electable interface {
	// Returns the state to transition into after run returns
	run() <-chan ElectionState
	// Stops the entity in order to transition into the election state
	stopAndBecome(ElectionState)

	electionState() ElectionState
}

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
	command  any
}

type State struct {
	electionState ElectionState
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

type RaftConfig struct {
	peerAddresses []string
	ownAdress     string
}

func NewRaft(config RaftConfig) *Raft {
	clients := createRpcClients(&config.peerAddresses)
	numOfPeers := len(clients)

	state := State{
		electionState: Follower,
		nextindex:     make([]int, numOfPeers),
		matchindex:    make([]int, numOfPeers),
	}

	r := Raft{
		state:      state,
		rpcClients: clients,
		numOfPeers: numOfPeers,
		ownAdress:  config.ownAdress,
		entity:     &follower{},
		rpcQueued:  make(chan struct{}),
	}
	return &r
}

func createRpcClients(raftAddresses *[]string) []*rpc.Client {
	numPeers := len(*raftAddresses)
	clients := make([]*rpc.Client, numPeers)
	index := 0
	for _, addr := range *raftAddresses {
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

type Raft struct {
	state        State
	ownAdress    string
	numOfPeers   int
	rpcClients   []*rpc.Client
	entity       electable
	rpcQueued    chan struct{}
	fnMu         sync.Mutex
	rpcTaskQueue []*rpcTask
}

func (r *Raft) persistState() {

}

func (r *Raft) serveRaft() {
	// Create the RPC server
	rpcServer := rpc.NewServer()

	// Register the RPC handlers

	err := rpcServer.RegisterName("Raft", r)
	if err != nil {
		fmt.Printf("Error registering RPC handlers: %v\n", err)
		return
	}

	// Listen for connections
	listener, err := net.Listen("tcp", r.ownAdress)
	if err != nil {
		fmt.Printf("Error starting RPC listener: %v\n", err)
		return
	}

	// Serve connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}

// Considered main goroutine
func (r *Raft) Start() {

	go r.serveRaft()

	for {
		select {
		case <-r.rpcQueued:
			r.processQueuedRPC()
		case newElectionState := <-r.entity.run():
			switch newElectionState {
			case Follower:
				r.entity = &follower{
					raft: r,
					exit: make(chan ElectionState),
				}
			case Candidate:
				r.entity = &candiate{
					raft: r,
					exit: make(chan ElectionState),
				}
			case Leader:
				r.entity = &leader{
					raft: r,
					exit: make(chan ElectionState),
				}
			}
		}

	}
}

func (r *Raft) processQueuedRPC() {
	r.fnMu.Lock()
	defer r.fnMu.Unlock()

	task := r.rpcTaskQueue[0]
	err := task.fn()
	task.fnResult <- err
}

// Either a RequestVote or AppendEntries
type rpcTask struct {
	fn       func() error
	fnResult chan error
}

func (r *Raft) queueFunc(fn func() error) *rpcTask {
	r.fnMu.Lock()
	defer r.fnMu.Unlock()
	task := &rpcTask{
		fn:       fn,
		fnResult: make(chan error),
	}
	r.rpcTaskQueue = append(r.rpcTaskQueue, task)
	r.rpcQueued <- struct{}{}
	return task
}

// Called via RPC
// We are in rpc goroutine
func (r *Raft) RequestVote(args *requestVoteArgs, reply *requestVoteReply) error {
	// Queue this onto the main goroutine
	fn := func() error {
		if r.entity.electionState() != Follower {
			if args.electionTerm > r.state.currentTerm {
				r.state.currentTerm = args.electionTerm
				r.state.votedFor = 0
				r.entity.stopAndBecome(Follower)
			}
			reply.term = r.state.currentTerm
			reply.voteGranted = false
			return nil
		}

		follower := r.entity.(*follower)
		return follower.requestVote(args, reply)
	}

	fnHolder := r.queueFunc(fn)
	err := <-fnHolder.fnResult
	return err
}

// Called via RPC
// We are in rpc goroutine
func (r *Raft) AppendEntries(args *appendEntriesArgs, reply *appendEntriesReply) error {

	// Stale leader
	if args.leaderTerm < r.state.currentTerm {
		reply.success = false
		return nil
	}

	if r.entity.electionState() != Follower {
		r.entity.stopAndBecome(Follower)
	}

	follower := r.entity.(*follower)
	return follower.appendEntries(args, reply)
}

// ========== LEADER ================
type leader struct {
	raft *Raft
	exit chan ElectionState
}

func (l *leader) run() <-chan ElectionState {
	ch := make(chan ElectionState)

	go func() {
		l.sendHeatbeats()

		heartbeatTicker := time.NewTicker(time.Duration(150) * time.Millisecond)
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				l.sendHeatbeats()
			case state := <-l.exit:
				ch <- state
				return
			}
		}
	}()
	return ch
}

func (c *leader) stopAndBecome(state ElectionState) {
	c.exit <- state
}

// Send empty entries as a heartbeat,
// clients reset their election timeout on heartbeat
func (l *leader) sendHeatbeats() {
	l.requestAppendEntries(
		l.raft.state.currentTerm,
		l.raft.state.id,
		l.raft.state.log.lastIndex(),
		l.raft.state.log.lastTerm(),
		l.raft.state.commitIndex,
		[]logEntry{})
}

// FIXME: Current code assumes sequential command processing. Make async
func (l *leader) requestAppendEntries(
	leaderTerm int,
	leaderId int,
	prevLogIndex int,
	prevLogTerm int,
	leaderCommit int,
	entries []logEntry) {

	var wg sync.WaitGroup
	ch := make(chan appendEntriesReply, l.raft.numOfPeers)
	wg.Add(l.raft.numOfPeers)

	for i := range l.raft.numOfPeers {
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
				err := l.raft.rpcClients[i].Call("Raft.AppendEntries", args, &reply)
				if err != nil {
					fmt.Printf("appendEntries error %s", err.Error())
				}
				if reply.success || reply.term > curTerm {
					break
				}
				l.raft.state.nextindex[i] -= 1
				args.prevLogIndex = l.raft.state.nextindex[i]
				args.entries = l.raft.state.log.entries[l.raft.state.nextindex[i]:]
			}

			ch <- reply
		}(i, l.raft.state.currentTerm)
	}

	wg.Wait()

	for range l.raft.numOfPeers {
		reply := <-ch
		if reply.term > l.raft.state.currentTerm {
			// Found a higher term. Leader no more, become follower.
			l.raft.state.currentTerm = reply.term
			l.raft.state.electionState = Follower
		}
	}
}

func (l *leader) TryCommand(command any) {
	entry := logEntry{
		term:     l.raft.state.currentTerm,
		command:  command,
		logIndex: len(l.raft.state.log.entries),
	}
	l.raft.state.log.entries = append(l.raft.state.log.entries, entry)
	l.requestAppendEntries(
		l.raft.state.currentTerm,
		l.raft.state.id,
		l.raft.state.log.lastIndex(),
		l.raft.state.log.lastTerm(),
		l.raft.state.commitIndex,
		[]logEntry{entry},
	)
}

func (l *leader) electionState() ElectionState {
	return Leader
}

// ========== CANDIDATE ================
type candiate struct {
	raft *Raft
	exit chan ElectionState
}

func (c *candiate) run() <-chan ElectionState {
	ch := make(chan ElectionState)

	go func() {
		c.attemptBecomeLeader()

		electionTicker := time.NewTicker(getRandElectionTimeout())
		defer electionTicker.Stop()
		for {
			select {
			case <-electionTicker.C:
				c.attemptBecomeLeader()
			case state := <-c.exit:
				ch <- state
				return
			}
		}
	}()
	return ch
}

// Called by a candidate to request votes to become leader
// Initiates the RPC call to requestVote which runs on a follower.
// Returns number of votes granted
func (c *candiate) requestRequestVote(term int, candidateId int, lastLogIndex int, lastLogTerm int) int {
	args := requestVoteArgs{
		electionTerm: term,
		candidateId:  candidateId,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
	}

	var wg sync.WaitGroup
	wg.Add(c.raft.numOfPeers)
	ch := make(chan (requestVoteReply), c.raft.numOfPeers)
	defer close(ch)

	// Send requestVote to all clients
	// Gather num of votes received
	for i := range c.raft.numOfPeers {
		go func(index int) {
			defer wg.Done()
			reply := requestVoteReply{
				term:        0,
				voteGranted: false,
			}
			// FIXME - need rpc timeout
			err := c.raft.rpcClients[index].Call("Raft.RequestVote", args, &reply)
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
	for i := 0; i < c.raft.numOfPeers; i++ {
		reply := <-ch
		if reply.term > c.raft.state.currentTerm {
			// Become follower again if behind in term
			// Continue through all clients in case there is another greater term
			c.raft.state.currentTerm = reply.term
			c.raft.state.votedFor = 0
			c.raft.state.electionState = Follower
			c.raft.persistState()
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

func (c *candiate) stopAndBecome(state ElectionState) {
	c.exit <- state
}

func (c *candiate) attemptBecomeLeader() {
	c.raft.state.currentTerm += 1           // Begin new election term
	c.raft.state.votedFor = c.raft.state.id // vote for self
	c.raft.state.electionState = Candidate
	c.raft.persistState()
	votes := c.requestRequestVote(
		c.raft.state.currentTerm,
		c.raft.state.id,
		c.raft.state.lastApplied,
		c.raft.state.log.lastTerm(),
	)
	if votes > c.raft.numOfPeers/2-1 {
		// Election won, become leader
		c.raft.state.electionState = Leader
		c.raft.persistState()
		for i := range c.raft.state.nextindex {
			c.raft.state.nextindex[i] = c.raft.state.log.lastIndex() + 1
		}
		c.stopAndBecome(Leader)
	}
}

func (c *candiate) electionState() ElectionState {
	return Candidate
}

// ========== FOLLOWER ================
type follower struct {
	raft *Raft
	exit chan ElectionState
}

func (f *follower) run() <-chan ElectionState {
	ch := make(chan ElectionState)

	go func() {
		electionTicker := time.NewTicker(getRandElectionTimeout())
		defer electionTicker.Stop()
		for {
			select {
			case <-electionTicker.C:
				f.stopAndBecome(Candidate)
			case state := <-f.exit:
				ch <- state
				return
			}
		}
	}()
	return ch
}

// Received by other nodes to respond to requests to become leader
// Not called directly. Only called via RPC
func (f *follower) requestVote(args *requestVoteArgs, reply *requestVoteReply) error {
	if args.electionTerm < f.raft.state.currentTerm {
		// If our term is greater than the term of the request, update the requesters term and vote no.
		reply.term = f.raft.state.currentTerm
		reply.voteGranted = false
		return nil
	}
	// Requester is in a higher term than us, reset our vote
	if args.electionTerm > f.raft.state.currentTerm {
		f.raft.state.currentTerm = args.electionTerm
		f.raft.state.votedFor = 0
	}

	reply.term = f.raft.state.currentTerm

	newLogUpToDate := false
	if args.lastLogTerm > f.raft.state.log.lastTerm() {
		newLogUpToDate = true
	} else if args.lastLogTerm == f.raft.state.log.lastTerm() &&
		args.lastLogIndex >= f.raft.state.log.lastIndex() {
		newLogUpToDate = true
	}

	// Otherwise grant vote if not yet voted in this term or if already voted for candidate
	if (f.raft.state.votedFor == 0 || f.raft.state.votedFor == args.candidateId) && newLogUpToDate {
		f.raft.state.votedFor = args.candidateId
		f.raft.persistState()
		reply.voteGranted = true
	} else {
		reply.voteGranted = false
	}
	return nil
}

func (f *follower) appendEntries(args *appendEntriesArgs, reply *appendEntriesReply) error {
	reply.term = f.raft.state.currentTerm

	f.raft.state.electionReset <- true
	f.raft.state.electionState = Follower

	// Not in line with leader at previous entries
	// Leader will retry with index-1
	if !f.raft.state.log.termAtIndexMatches(args.prevLogIndex, args.prevLogTerm) {
		f.raft.persistState()
		reply.success = false
		return nil
	}

	// If we get here, we know args.prevLogIndex is the last known good
	// location for our log. We might have extras though that are not good.

	// If there are existing entries that do not match the term of the new entries,
	// delete all existing entries after the first bad one
	curLastIndex := f.raft.state.log.lastIndex()
	amountToCheck := min(len(args.entries), curLastIndex-args.prevLogIndex)
	for i := range amountToCheck {
		checkIndex := args.prevLogIndex + i + 1
		newEntryTerm := args.entries[i]
		curEntryTerm := f.raft.state.log.entries[checkIndex]
		if newEntryTerm != curEntryTerm {
			f.raft.state.log.entries = f.raft.state.log.entries[:checkIndex]
			f.raft.state.log.entries = append(f.raft.state.log.entries, args.entries[i:]...)
			break
		}

	}

	if args.leaderCommit > f.raft.state.commitIndex {
		f.raft.state.commitIndex = min(args.leaderCommit, f.raft.state.log.lastIndex())
	}
	f.raft.persistState()
	reply.success = true
	return nil
}

func (f *follower) stopAndBecome(state ElectionState) {
	f.exit <- state
}

func (f *follower) electionState() ElectionState {
	return Follower
}

func getRandElectionTimeout() time.Duration {
	return time.Duration(rand.IntN(100)+50) * time.Millisecond
}
