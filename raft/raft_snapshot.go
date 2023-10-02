package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// lastIncludeIndex := max(m.Snapshot.Metadata.Index, m.Index-1)
	lastIncludeIndex := m.Snapshot.Metadata.Index
	lastIncludeTerm := m.Snapshot.Metadata.Term

	// filter stale message
	if m.Term < r.Term || lastIncludeIndex < r.RaftLog.FirstIndex() {
		return
	}

	// TestRestoreIgnoreSnapshot2C ignore snap that index <= commit index
	// it may be a vverdate snapshot
	if lastIncludeIndex <= r.RaftLog.committed {
		return
	}

	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	// clear raft logs
	if lastIncludeIndex >= r.RaftLog.LastIndex() {
		r.RaftLog.entries = make([]pb.Entry, 1)
		r.RaftLog.firstIndex = lastIncludeIndex
		r.RaftLog.entries[0].Term = lastIncludeTerm
		r.RaftLog.entries[0].Index = lastIncludeIndex
	}

	r.RaftLog.CutStartEntry(lastIncludeIndex)

	r.RaftLog.committed = lastIncludeIndex
	r.RaftLog.applied = lastIncludeIndex
	r.RaftLog.stabled = lastIncludeIndex

	// if raft has a pending snapshot, it must be older one
	// because if it is new, its commit index must be great than snaphot in message
	r.RaftLog.pendingSnapshot = m.Snapshot

	log.Warnf("[%v] %v:R%v recv Snapshot at <Index:%v, Term:%v>",
		r.Term, r.State, r.id, lastIncludeIndex, lastIncludeTerm)

	r.Prs = make(map[uint64]*Progress, 0)
	for _, id := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[id] = &Progress{} //
	}

	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    m.Term,
		Reject:  false,
		Index:   lastIncludeIndex,
		Commit:  lastIncludeIndex,
	}

	r.msgs = append(r.msgs, response)
}

func (r *Raft) sendSnapShotTo(to uint64) bool {
	if _, ok := r.Prs[r.id]; !ok {
		return false
	}

	var snapshot pb.Snapshot
	var err error

	if r.snapTicks != 0 {
		return false
	}

	if r.RaftLog.pendingSnapshot != nil {
		snapshot = *r.RaftLog.pendingSnapshot
	} else {
		snapshot, err = r.RaftLog.storage.Snapshot()
		r.snapTicks = 2
	}

	if err != nil {
		// first call Storage.Snapshot will error
		// next heart beat will send again
		return false
	}
	log.Warnf("[T%v] %v:R%v send snapshot <lastInclude: %v> to R%v, applied %v, first %v ",
		r.Term, r.State, r.id, snapshot.Metadata.Index, to, r.RaftLog.applied, r.RaftLog.FirstIndex())

	snapMsg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
		Commit:   r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, snapMsg)

	return true
}
