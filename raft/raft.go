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

	"github.com/pingcap-incubator/tinykv/log"
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
	"FOLL",
	"CAND",
	"LEAD",
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

	// some other member add by Summer
	ticks uint64 // for debug

	snapTicks uint64

	// randElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	rflog := newLog(c.Storage)

	r := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          rflog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		// other default 0 or nil
	}

	peers := c.peers

	hardstate, confstate, err := c.Storage.InitialState()
	if err == nil {
		// in raft, commit index and last applied are volatitle state
		// but tinykv persist commit index,
		rflog.committed = hardstate.Commit
		r.Term = hardstate.Term
		r.Vote = hardstate.Vote
		if peers == nil {
			peers = confstate.Nodes
		}
	}

	// init nextIndex and matchIndex
	lastIndex := r.RaftLog.LastIndex()

	for _, peer := range peers {
		r.Prs[peer] = &Progress{lastIndex + 1, 0}
	}

	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	log.Infof("[T%v] R%v: new raft, stabled:%v, commit:%v applied:%v, stabled:%v", r.Term, r.id, r.RaftLog.stabled, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.stabled)

	return &r
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
	r.ticks += 1
	// Your Code Here (2A).
	r.electionElapsed += 1
	r.heartbeatElapsed += 1 // on;y keep for leader

	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	if r.snapTicks > 0 {
		r.snapTicks -= 1
	}

	// only non-leader can trigger election timeout
	// it mean if one old leader offline and restore after
	// the old leader will not break other nodes' work when old leader restore
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
	r.leadTransferee = None
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

	log.Infof("[T%v] R%v Become candidate: commit %v, apply %v, lastIndex %v", r.Term, r.id, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex())

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		panic("becomeLeader Error")
		// return // this case shouldn't occur
	}

	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	r.Lead = r.id
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.PendingConfIndex = None

	log.Infof("[T%v] R%v Become leader: commit %v, apply %v, lastIndex %v, pendingIndex %v",
		r.Term, r.id, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.PendingConfIndex)

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
		r.followerMsgHandle(m)

	case StateCandidate:
		r.candidateMsgHandle(m)

	case StateLeader:
		r.leaderMsgHandle(m)

	}
	return nil
}

func (r *Raft) followerMsgHandle(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgRequestVote:
		r.handlerRequestVote(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow()

	case pb.MessageType_MsgTransferLeader:
		if _, ok := r.Prs[r.id]; ok {
			log.Infof("[T%v] %v:R%v trans LeadTrans to %v", r.Term, r.State, r.id, r.Lead)
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	default:
		// ignore
	}
	return nil
}

func (r *Raft) candidateMsgHandle(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgRequestVote:
		r.handlerRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow()

	case pb.MessageType_MsgTransferLeader:
		r.transferMsgToLead(m)

	default:
		return errors.New("follower doesn't handle message")
	}
	return nil
}

func (r *Raft) leaderMsgHandle(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// ignore

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

	case pb.MessageType_MsgRequestVoteResponse:
		// ignore it

	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)

	case pb.MessageType_MsgTransferLeader:
		r.handleLeadTransfer(m)

	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow()

	default:
		panic("undefined message!!!")
	}
	return nil
}

func (r *Raft) transferMsgToLead(m pb.Message) {
	if _, ok := r.Prs[r.id]; ok {
		// log.Infof("[T%v] %v:R%v trans LeadTrans to %v", r.Term, r.State, r.id, r.Lead)
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	}
}

// sendHearts sends a heartbeat to all peers
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

	// commit used to help leader to decide whether send append entries to me
	response := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.LastIndex(),
	}

	r.msgs = append(r.msgs, response)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// current leader is a stale leader
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// if m.Commit > r.Prs[m.From].Match {
	// 	r.Prs[m.From].Match = m.Commit
	// }
	// need send some entres to follower
	if r.RaftLog.committed > m.Commit || r.RaftLog.LastIndex() > m.Index {
		r.sendAppend(m.From)
	}
}

// do nothing ....
func (r *Raft) abortLeadTransfer() {
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A)
	if _, ok := r.Prs[id]; ok {
		return
	}

	// snapshot should send to new peer
	// so next index set first index not last index
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.FirstIndex(),
	}

	log.Warnf("[%v] %v:R%v add Peer %v: %v", r.Term, r.State, r.id, id, r.Prs[id])
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	// skip if id doesn't exist
	if _, ok := r.Prs[id]; !ok {
		return
	}
	log.Infof("[%v] %v:R%v remove Peer %v", r.Term, r.State, r.id, id)

	delete(r.Prs, id)
	delete(r.votes, id)

	// try to update commit index
	if id != r.id && r.State == StateLeader && r.RaftLog.committed != r.RaftLog.LastIndex() {
		r.maybeCommit()
	}
}

// become candiate and start election right now
func (r *Raft) handleTimeoutNow() {
	log.Infof("[T%v] %v:R%v recv MsgTimeOut", r.Term, r.State, r.id)
	r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
}

//
func (r *Raft) handleLeadTransfer(m pb.Message) {
	// if raft will destroy or transfer target doesn't in prs, skip
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	leadTransferee := m.From

	log.Infof("[T%v] %v:R%v transfer leader to %v", r.Term, r.State, r.id, m.From)

	if r.leadTransferee != None {
		if r.leadTransferee == leadTransferee {
			return
		}
		if leadTransferee == r.id {
			r.leadTransferee = None
			r.Term += 1
		}
		r.abortLeadTransfer()
	}

	if leadTransferee == r.id {
		return
	}

	r.leadTransferee = leadTransferee

	if r.Prs[leadTransferee].Match == r.RaftLog.LastIndex() {
		log.Infof("[T%v] %v:R%v send msgTimeout to %v", r.Term, r.State, r.id, r.leadTransferee)
		msg := pb.Message{
			From:    r.id,
			To:      r.leadTransferee,
			MsgType: pb.MessageType_MsgTimeoutNow,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		// r.leadTransferee = None
		log.Infof("[T%v] %v:R%v transfer target %v not-up-to-date, match:%v, lastIndex:%v",
			r.Term, r.State, r.id, r.Prs[leadTransferee].Match, leadTransferee, r.RaftLog.LastIndex())
		r.sendAppend(r.leadTransferee)
	}

}
