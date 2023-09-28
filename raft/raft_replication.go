package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// append entries to local log and send to others
func (r *Raft) proposeEntries(m pb.Message) {
	// pusunexpected raft log index all entries to my local log
	for idx, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = r.RaftLog.LastIndex() + uint64(idx) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
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

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	ets := make([]*pb.Entry, 0)
	prevLogIndex := r.Prs[to].Next - 1

	if prevLogIndex < r.RaftLog.FirstIndex()-1 {
		return r.sendSnapShotTo(to)
	}

	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	// next may be great than lastIndex
	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		ets = append(ets, r.RaftLog.getEntry(i))
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: ets,
		Commit:  r.RaftLog.committed,
	}

	// log.Infof("[T%v] %v:R%v sendappend to %v: prev <Idx %v, Term %v> len %v",
	// 	r.Term, r.State, r.id, msg.To, msg.Index, msg.LogTerm, len(ets))
	r.msgs = append(r.msgs, msg)
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}

	if m.Term == r.Term {
		// TODO: check log entries conflict
		prevLogTerm, _ := r.RaftLog.Term(m.Index)
		prevLogMatch := prevLogTerm == m.LogTerm

		response.Index = r.RaftLog.LastIndex()

		if prevLogMatch {
			r.appendEntriesToLog(m.Entries)
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			response.Commit = r.RaftLog.committed
			response.Index = m.Index + uint64(len(m.Entries))
			response.Reject = false
		}
		// log.Infof("[T%v] %v:R%v recv append: prev <Idx %v, Term %v> len %v reject %v",
		// 	r.Term, r.State, r.id, m.Index, m.LogTerm, len(m.Entries), response.Reject)
	}

	r.msgs = append(r.msgs, response)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// log.Infof("[T%v] %v:R%v recv appendResp From %v at %v",
	// 	r.Term, r.State, r.id, m.From, m.Index)

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Term < r.Term || r.Prs[m.From].Match > m.Index {
		return
	}

	// TO FIX: un-order msg maybe cause a bug:
	// leader request snapshot but it recv reject msg before in same term
	if m.Reject {
		// TODO: log catchup
		if r.Prs[m.From].Next >= r.RaftLog.FirstIndex()-1 {
			r.Prs[m.From].Next -= 1
			r.sendAppend(m.From)
		}

		return
	}

	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index

	if m.From == r.leadTransferee {
		r.leadTransferee = None
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgTransferLeader,
			From:    m.From,
			To:      r.id,
		})
		return
	}

	term, _ := r.RaftLog.Term(m.Index)
	if term == r.Term && m.Index > r.RaftLog.committed {
		r.tryAdvanceCommit(m.Index)
	}
}

func (r *Raft) appendEntriesToLog(entries []*pb.Entry) {
	for _, entry := range entries {
		if entry.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else {
			matchTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				log.Error(err)
			} else if entry.Term != matchTerm {
				log.Warnf("[T%v] R%v Conflict Entry at %v and my first index is %v", r.Term, r.id, entry.Index, r.RaftLog.FirstIndex())
				r.RaftLog.CutEndEntry(entry.Index)
				if r.RaftLog.stabled >= entry.Index {
					// TODO: maybe some problem
					r.RaftLog.stabled = entry.Index - 1
				}

				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}
	}
}

func (r *Raft) tryAdvanceCommit(newcommit uint64) bool {
	agreeCnt := 0
	for _, pr := range r.Prs {
		if pr.Match >= newcommit {
			agreeCnt += 1
		}
	}

	advance := agreeCnt > len(r.Prs)/2

	if advance {
		r.printf(1, CMIT, "advance commit from %v to %v", r.RaftLog.committed, newcommit)
		r.RaftLog.committed = newcommit
		r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose})
	}

	return advance
}
