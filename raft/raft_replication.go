package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// append entries to local log and send to others
func (r *Raft) proposeEntries(m pb.Message) {
	// push all entries to my local log
	for idx, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = r.RaftLog.LastIndex() + uint64(idx) + 1
		r.printf(3, LOG2, "Propose ets %v at %v", *ent, r.RaftLog.LastIndex()+1)
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
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	// TODO; if prevLogIndex < first Index, send snapshot

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

	r.printf(2, LEAD, "SendAppend to %v: prev <Idx %v, Term %v> len %v", msg.To, msg.Index, msg.LogTerm, len(ets))
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
		prevLogTerm, err := r.RaftLog.Term(m.Index)
		prevLogMatch := prevLogTerm == m.LogTerm

		r.printf(3, LOG1, "handleAppendEntires prev<Idx:%v, Term %v> len %v", m.Index, prevLogTerm, len(m.Entries))
		if err != nil {
			// prevLogTerm will 0, prevLogMatch must be false
			r.printf(1, ERRO, "Term %v err: %v", m.Index, err)
		}

		if prevLogMatch {
			r.printf(2, APED, "Append %v Entry at %v", len(m.Entries), r.RaftLog.LastIndex()+1)
			r.appendEntriesToLog(m.Entries)
			if m.Commit > r.RaftLog.committed {
				r.printf(1, CMIT, "advance commit from %v to %v", r.RaftLog.committed, m.Commit)
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			response.Commit = r.RaftLog.committed
			response.Index = m.Index + uint64(len(m.Entries))
			response.Reject = false
		}
	}

	r.msgs = append(r.msgs, response)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Term < r.Term || r.Prs[m.From].Match > m.Index {
		// a rpc from long long ago
		return
	}

	if m.Reject {
		// TODO: log catchup
		r.Prs[m.From].Next -= 1
		r.printf(1, LOG1, "S%v reject me, reduce next to %v", m.From, r.Prs[m.From].Next)
		r.sendAppend(m.From)

		return
	}

	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index
	r.printf(2, LEAD, "append success from S%v", m.From)

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
			matchTerm, _ := r.RaftLog.Term(entry.Index)
			if entry.Term != matchTerm {
				r.RaftLog.CutEndEntry(entry.Index)
				r.printf(1, APED, "Conflict Entry at %v", entry.Index)
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
	agreeNode := make([]uint64, 0)
	for id, pr := range r.Prs {
		if id != r.id && pr.Match >= newcommit {
			agreeNode = append(agreeNode, id)
		}
	}

	advance := len(agreeNode)+1 > len(r.Prs)/2

	if advance {
		r.printf(1, CMIT, "advance commit from %v to %v", r.RaftLog.committed, newcommit)
		r.RaftLog.committed = newcommit
		r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose})
	}

	return advance
}
