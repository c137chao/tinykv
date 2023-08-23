// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rflog := newLog(c.Storage)

	hardstate, _, _ := rflog.storage.InitialState()

	r := Raft{
		id:               c.ID,
		Term:             hardstate.Term,
		Vote:             hardstate.Vote,
		RaftLog:          rflog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		// other default 0 or nil
	}
	rflog.committed = hardstate.Commit
	lastIndex := r.RaftLog.LastIndex()

	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{lastIndex + 1, 0}
	}
	r.printf(1, TEST, "---------------New Raft with entries %v", r.RaftLog.allEntries())

	return &r
}

// append entries to local log and send to others
func (r *Raft) proposeEntries(m pb.Message) {
	// push all entries to my local log
	for idx, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = r.RaftLog.LastIndex() + uint64(idx) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		r.printf(3, LEAD, "Propose append ets %v", *ent)
	}

	// update my match index and next index
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// if only one member in group, update commit index
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	// send to all other raft node
	for to := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// help for rawnode
func (r *Raft) softstate() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// help for rawnode
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed += 1
	r.heartbeatElapsed += 1 // on;y keep for leader

	if r.State != StateLeader && r.electionElapsed >= r.electionTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}

	// if r become leader, it doesn't clear heartbeatElapsedm it will send heartbeat next tick
	if r.State == StateLeader && r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.State = StateFollower
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.Vote = r.id
	r.State = StateCandidate
	// test need random election timeout(et) from [et, 2*et]
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	// init votes record
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

	r.printf(2, INFO, "Become Candidate")
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		panic("becomeLeader Error")
		// return // this case shouldn't occur
	}
	r.printf(1, LEAD, "Become Leader")
	r.State = StateLeader
	// r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	r.heartbeatElapsed = 0

	// init nextIndex and match Index
	initNextIndex := r.RaftLog.LastIndex() + 1
	for _, pr := range r.Prs {
		pr.Next = initNextIndex
		pr.Match = 0
	}

	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.followerStep(m)
	case StateCandidate:
		r.candidateStep(m)
	case StateLeader:
		r.leaderStep(m)
	}
	return nil
}

func (r *Raft) followerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.sendRequestVoteAll()

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgRequestVote:
		r.handlerRequestVote(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	default:
		return errors.New("follower doesn't handle message")
	}
	return nil
}

func (r *Raft) candidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.sendRequestVoteAll()

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgRequestVote:
		r.handlerRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	default:
		return errors.New("candidate doesn't handle message")
	}
	return nil
}

func (r *Raft) leaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// leader doesn't receive heartbeat reponse for a long time
		// it will become candidate and start election
		// r.becomeCandidate()

		// why leader ignore this msg ????
		// how to check leader offline
	case pb.MessageType_MsgBeat:
		r.sendHeartbeatAll()
	case pb.MessageType_MsgPropose:
		r.proposeEntries(m)

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)

	case pb.MessageType_MsgRequestVote:
		r.handlerRequestVote(m)

	case pb.MessageType_MsgSnapshot:
		panic("Unimplement handler on MsgSnapshot")

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)

	case pb.MessageType_MsgTransferLeader:
		panic("Unimplement handler on MsgTransferLeader")

	case pb.MessageType_MsgTimeoutNow:
		panic("Unimplement handler on MsgTimeoutNo")

	default:
		return errors.New("leader doesn't handle message")
	}
	return nil
}

func (r *Raft) sendRequestVoteAll() {
	if len(r.Prs) == 1 {
		// only one node
		r.becomeLeader()
		return
	}

	lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   r.RaftLog.LastIndex(),
	}
	for to := range r.Prs {
		if to != r.id {
			msg.To = to
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handlerRequestVote(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	response := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}

	votefor := m.Term >= r.Term && (r.Vote == None || r.Vote == m.From)

	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	up_to_date := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())

	if votefor && up_to_date {
		// candidate or leader keep vote to itself
		r.Vote = m.From
		response.Reject = false
		r.printf(2, VOTE, "vote for node %v", m.From)
	}

	r.msgs = append(r.msgs, response)
}

//
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	r.votes[m.From] = !m.Reject
	if len(r.votes) <= len(r.Prs)/2 {
		return
	}
	agree := 0
	rejec := 0

	for _, granted := range r.votes {
		if granted {
			agree += 1
		} else {
			rejec += 1
		}
	}

	if agree > len(r.Prs)/2 {
		r.becomeLeader()
	} else if rejec > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

//
func (r *Raft) sendHeartbeatAll() {
	for peer_id := range r.Prs {
		if peer_id != r.id {
			r.sendHeartbeat(peer_id)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State != StateLeader {
		panic("state change when send heartbeat")
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}

	response := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, response)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if r.RaftLog.committed > m.Commit {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
