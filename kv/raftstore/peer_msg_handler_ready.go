package raftstore

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

// check is there any data in raft need to persistence
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}

	rd := d.RaftGroup.Ready()

	// save should before send message
	result, _ := d.peerStorage.SaveReadyState(&rd)

	// sync command to other peer
	d.Send(d.ctx.trans, rd.Messages)

	if result != nil {
		// ctx info should be lock, it is share by all peers
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.setRegion(result.Region, d.peer)
		d.ctx.storeMeta.Unlock()
		// log.Warnf("[trans region] Peer %v Change region: %v", d.PeerId(), d.Region())
	}

	// persist raft state and raft entries
	if len(rd.CommittedEntries) != 0 {
		kvWB := &engine_util.WriteBatch{}
		applyKey := meta.ApplyStateKey(d.regionId)
		var start uint64 = 0

		// if storage re-start, rafr applied will be the first entry index
		// it re-play all log entries
		if rd.CommittedEntries[0].Index < d.peerStorage.AppliedIndex() {
			start = d.peerStorage.AppliedIndex() - rd.CommittedEntries[0].Index
		}
		for _, entry := range rd.CommittedEntries[start:] {
			d.applyRaftEntry(&entry, kvWB)
			if d.stopped {
				return
			}
		}

		kvWB.SetMeta(applyKey, d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	if !d.stopped {
		d.RaftGroup.Advance(rd)
	}

}

// apply raft entry and new state to kvdb and raftdb
// entry canbe:
//    (1) confChange: change region member
//    (2) adminRequest: compact log or split regions
//    (3) normal opertions: put, get, delete, scan
//
func (d *peerMsgHandler) applyRaftEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		d.applyChangePeers(entry)
	} else {
		reqs := &raft_cmdpb.RaftCmdRequest{}
		reqs.Unmarshal(entry.Data)

		if reqs.AdminRequest != nil {
			d.applyAdminReq(entry, reqs)
		}

		if reqs.Requests != nil {
			d.applyRequests(entry, reqs)
		}
	}
}

// applyAdminReq
//  Admin Request may be compacklog and split region,
//  lead transfer and confChange won't arrive here
//
func (d *peerMsgHandler) applyAdminReq(entry *eraftpb.Entry, reqs *raft_cmdpb.RaftCmdRequest) {
	admin := reqs.AdminRequest

	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.applyCompactLog(entry, admin.CompactLog)
	case raft_cmdpb.AdminCmdType_Split:
		log.Fatalf("split doesn't implement")
	default:
		panic("unsupport admin type")
	}
}

// applyCompactLog
//  it will update trunc state and apply index, then compact the kvdb
//  compact log is also seen as a raft entry, it also increase applyindex
//  apply state will write to kvdb in HandleRaftReady
func (d *peerMsgHandler) applyCompactLog(entry *eraftpb.Entry, compLog *raft_cmdpb.CompactLogRequest) {
	if compLog.CompactIndex < d.peerStorage.truncatedIndex() {
		return
	}
	lastIncludeIndex := compLog.CompactIndex
	lastIncludeTerm := compLog.CompactTerm

	d.peerStorage.applyState.TruncatedState.Index = lastIncludeIndex
	d.peerStorage.applyState.TruncatedState.Term = lastIncludeTerm

	// becareful, don't set apply index to last include index, even it won't cause fault
	d.peerStorage.applyState.AppliedIndex = entry.Index

	// log.Infof("Peer %v Compact Log at index %v, entries first idx %v", d.PeerId(), lastIncludeIndex, d.RaftGroup.Raft.RaftLog.FirstIndex())

	// it's safe for follower
	// if one follower apply this request, it means all before entry has been applied
	d.ScheduleCompactLog(lastIncludeIndex)

}

// applyChangePeers
//  region memeber change, add or remove node from region of current peer
//
func (d *peerMsgHandler) applyChangePeers(entry *eraftpb.Entry) {
	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
	}

	// unmarshal data
	cc := eraftpb.ConfChange{}
	cc.Unmarshal(entry.GetData())

	changePeers := raft_cmdpb.ChangePeerRequest{}
	changePeers.Unmarshal(cc.Context)

	// apply to raft
	d.RaftGroup.ApplyConfChange(cc)

	wb := &engine_util.WriteBatch{}

	switch changePeers.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if util.FindPeer(d.Region(), changePeers.Peer.StoreId) == nil {
			d.Region().RegionEpoch.ConfVer += 1
			d.Region().Peers = append(d.Region().Peers, changePeers.Peer)
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)

			d.insertPeerCache(changePeers.Peer)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		if d.PeerId() == changePeers.Peer.Id {
			d.destroyPeer()
			return
		}
		if util.RemovePeer(d.Region(), changePeers.Peer.StoreId) != nil {
			d.Region().RegionEpoch.ConfVer += 1
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)

			d.removePeerCache(changePeers.Peer.GetId())
		}

	default:
		panic("unsupport conf change type")

	}

	log.Infof("%v [APPLY] confChange:%v confV:%v, peers %v", d.Tag, changePeers, d.Region().RegionEpoch.ConfVer, d.Region().GetPeers())
	d.CallBackPropose(entry, resp)

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) CallBackPropose(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 && (d.proposals[0].term < entry.Term || d.proposals[0].index < entry.Index) {
		// log.Warnf("[T%v] Peer %v Skip Stale Propose in T%v%v ", d.Term(), d.PeerId(), d.proposals[0].index, d.proposals[0].term)
		d.proposals[0].cb.Done(ErrResp(&util.ErrStaleCommand{}))
		d.proposals = d.proposals[1:]
	}

	// non-leader may be wake up client, it's ok
	if len(d.proposals) > 0 && d.proposals[0].term == entry.Term && d.proposals[0].index == entry.Index {
		// log.Infof("[%v] Peer %v Wakeup Propose %v for %v resps: %v", d.Term(), d.PeerId(), d.proposals[0].index, len(resps.Responses), resps.Responses[0])
		if d.proposals[0].cb.Txn == nil {
			d.proposals[0].cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		d.proposals[0].cb.Done(resp)
		d.proposals = d.proposals[1:]
	}
}

// apply request in entry to db storage
func (d *peerMsgHandler) applyRequests(entry *eraftpb.Entry, reqs *raft_cmdpb.RaftCmdRequest) {
	resps := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term(), Uuid: []byte(fmt.Sprintf("%v", d.PeerId()))},
	}

	for _, req := range reqs.Requests {
		if d.stopped {
			return
		}

		kvWB := &engine_util.WriteBatch{}
		resp := raft_cmdpb.Response{CmdType: req.CmdType}

		switch req.GetCmdType() {
		case raft_cmdpb.CmdType_Get:
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			resp.Get = &raft_cmdpb.GetResponse{Value: val}
			if err != nil {
				BindRespError(resps, &util.ErrKeyNotInRegion{Key: req.Get.Key, Region: d.Region()})
			}

		case raft_cmdpb.CmdType_Put:
			// engine_util.PutCF(kvdb, req.Put.Cf, req.Put.Key, req.Put.Value)
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			resp.Put = &raft_cmdpb.PutResponse{}

		case raft_cmdpb.CmdType_Delete:
			// engine_util.DeleteCF(kvdb, req.Delete.Cf, req.Delete.Key)
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			resp.Delete = &raft_cmdpb.DeleteResponse{}

		case raft_cmdpb.CmdType_Snap:
			resp.Snap = &raft_cmdpb.SnapResponse{Region: d.Region()}

		default:
			panic("Invalid Raft Command")
		}

		// log.Infof("[T%v] Peer %v apply %v at index %v", d.Term(), d.PeerId(), *req, entry.Index)
		resps.Responses = append(resps.Responses, &resp)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	// write back apply state and request
	d.peerStorage.applyState.AppliedIndex = entry.Index

	// skip all stale command, it may be caused by leader change (oldleader -> follower -> newleader) or log overwrite
	d.CallBackPropose(entry, resps)
}
