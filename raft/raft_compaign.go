package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// send request vote to all peers
func (r *Raft) sendRequestVoteAll() {
	log.Warnf("[T%v] R%v request votes to %v", r.Term, r.id, len(r.Prs))
	if len(r.Prs) == 1 {
		// only one node
		r.becomeLeader()
		return
	}

	for to := range r.Prs {
		if to != r.id {
			r.sendRequestVoteTo(to)
		}
	}
}

func (r *Raft) sendRequestVoteTo(to uint64) {
	lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   r.RaftLog.LastIndex(),
	}

	r.msgs = append(r.msgs, msg)
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

	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	canVoteFor := m.Term >= r.Term && (r.Vote == None || r.Vote == m.From) && r.Lead == None
	up_to_date := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())

	if canVoteFor && up_to_date {
		// candidate or leader keep vote to itself
		r.Vote = m.From
		response.Reject = false
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
	agreeCnt := 0
	rejecCnt := 0

	for _, granted := range r.votes {
		if granted {
			agreeCnt += 1
		} else {
			rejecCnt += 1
		}
	}

	if agreeCnt > len(r.Prs)/2 {
		r.becomeLeader()
	} else if rejecCnt > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}
