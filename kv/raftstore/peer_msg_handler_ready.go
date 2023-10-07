package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
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
		// region member or version may be change but region id won't change
		// because follower and leader must be in smae region

		// ctx info should be lock, it is share by all peers in this store
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.setRegion(result.Region, d.peer)
		d.ctx.storeMeta.Unlock()
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

		// if peer restart, it may try apply old version or confversion region change request
		err := util.CheckRegionEpoch(reqs, d.Region(), true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			resp := newCmdResp()
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index

			BindRespError(resp, errEpochNotMatching)
			d.CallBackPropose(entry, resp, false)

			return
		}

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
		d.applySplitRegion(entry, reqs)

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

	// apply to raft node
	d.RaftGroup.ApplyConfChange(cc)

	wb := &engine_util.WriteBatch{}

	switch changePeers.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if util.FindPeer(d.Region(), changePeers.Peer.StoreId) == nil {
			d.Region().RegionEpoch.ConfVer += 1
			d.Region().Peers = append(d.Region().Peers, changePeers.Peer)
			// it's important to update peer cache
			d.insertPeerCache(changePeers.Peer)

			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		if d.PeerId() == changePeers.Peer.Id {
			d.destroyPeer()
			return
		}
		if util.RemovePeer(d.Region(), changePeers.Peer.StoreId) != nil {
			d.Region().RegionEpoch.ConfVer += 1
			d.removePeerCache(changePeers.Peer.GetId())

			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)
		}

	default:
		panic("unsupport conf change type")

	}

	log.Infof("%v [APPLY] confChange:%v confV:%v, peers %v", d.Tag, changePeers, d.Region().RegionEpoch.ConfVer, d.Region().GetPeers())
	d.peerStorage.applyState.AppliedIndex = entry.Index
	d.CallBackPropose(entry, resp, false)
}

func (d *peerMsgHandler) applySplitRegion(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest) {
	// modify range and regionEpoch
	// other will create relevant meta infomation

	// newly-created region should be create by creater()
	// register to router.region and insert to regionRanges in ctx.StoreMeta
	region := d.Region()
	split := req.AdminRequest.Split

	if err := util.CheckKeyInRegion(split.SplitKey, region); err != nil {
		d.peerStorage.applyState.AppliedIndex = entry.Index
		return
	}

	peers := make([]*metapb.Peer, 0)
	for i, peer := range d.Region().Peers {
		peers = append(peers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peer.StoreId})
	}

	splitRegion := &metapb.Region{
		Id:          split.NewRegionId,
		StartKey:    split.SplitKey,
		EndKey:      region.EndKey,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       peers,
	}

	if d.IsLeader() {
		log.Warnf("%v [APPLy] split region\nregion [%s, %s) split with key %s, version:%v, my version %v",
			d.Tag, d.Region().StartKey, d.Region().EndKey, split.SplitKey, req.Header.RegionEpoch.Version, d.Region().RegionEpoch.Version)
	}

	splitPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.schedulerTaskSender, d.peerStorage.Engines, splitRegion)
	if err != nil {
		panic(err)
	}

	d.Region().EndKey = split.SplitKey
	d.Region().RegionEpoch.Version += 1

	d.ctx.router.register(splitPeer)
	d.ctx.router.send(split.NewRegionId, message.Msg{Type: message.MsgTypeStart, RegionID: split.NewRegionId})

	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: splitRegion})

	d.ctx.storeMeta.setRegion(splitRegion, splitPeer)

	d.peerStorage.applyState.AppliedIndex = entry.Index

	kvwb := &engine_util.WriteBatch{}
	meta.WriteRegionState(kvwb, region, rspb.PeerState_Normal)
	meta.WriteRegionState(kvwb, splitRegion, rspb.PeerState_Normal)
	kvwb.MustWriteToDB(d.peerStorage.Engines.Kv)

	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{splitRegion}},
	}
	d.CallBackPropose(entry, resp, false)
}

// apply request in entry to db storage
func (d *peerMsgHandler) applyRequests(entry *eraftpb.Entry, reqs *raft_cmdpb.RaftCmdRequest) {
	scan := false
	resps := newCmdResp()
	resps.Header.CurrentTerm = d.Term()

	for _, req := range reqs.Requests {
		if d.stopped {
			return
		}

		kvWB := &engine_util.WriteBatch{}
		resp := raft_cmdpb.Response{CmdType: req.CmdType}

		switch req.GetCmdType() {
		case raft_cmdpb.CmdType_Get:
			if err := util.CheckKeyInRegion(req.Get.Key, d.Region()); err != nil {
				BindRespError(resps, &util.ErrKeyNotInRegion{Key: req.Get.Key, Region: d.Region()})
			} else {
				val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				resp.Get = &raft_cmdpb.GetResponse{Value: val}
				if err != nil {
					BindRespError(resps, &util.ErrKeyNotInRegion{Key: req.Get.Key, Region: d.Region()})
				}
			}

		case raft_cmdpb.CmdType_Put:
			// engine_util.PutCF(kvdb, req.Put.Cf, req.Put.Key, req.Put.Value)
			if err := util.CheckKeyInRegion(req.Put.Key, d.Region()); err != nil {
				BindRespError(resps, &util.ErrKeyNotInRegion{Key: req.Put.Key, Region: d.Region()})
			} else {
				kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
				resp.Put = &raft_cmdpb.PutResponse{}
				d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))
			}

		case raft_cmdpb.CmdType_Delete:
			// engine_util.DeleteCF(kvdb, req.Delete.Cf, req.Delete.Key)
			if err := util.CheckKeyInRegion(req.Delete.Key, d.Region()); err != nil {
				BindRespError(resps, &util.ErrKeyNotInRegion{Key: req.Delete.Key, Region: d.Region()})
			} else {
				// TODO: delete may cause merge
				kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
				resp.Delete = &raft_cmdpb.DeleteResponse{}
			}

		case raft_cmdpb.CmdType_Snap:
			resp.Snap = &raft_cmdpb.SnapResponse{Region: d.Region()}
			scan = true

		default:
			panic("Invalid Raft Command")
		}

		// log.Infof("%v apply %v at index %v", d.Tag, *req, entry.Index)
		resps.Responses = append(resps.Responses, &resp)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	// write back apply state and request
	d.peerStorage.applyState.AppliedIndex = entry.Index

	// skip all stale command, it may be caused by leader change (oldleader -> follower -> newleader) or log overwrite
	d.CallBackPropose(entry, resps, scan)
}

// wakeup callback of proposal which term and index equal enty's term and index
// during apply this request, leader maybe change, so function should skip all old proposals before
func (d *peerMsgHandler) CallBackPropose(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse, scan bool) {
	for len(d.proposals) > 0 && (d.proposals[0].term < entry.Term || d.proposals[0].index < entry.Index) {
		// log.Warnf("[T%v] Peer %v Skip Stale Propose in T%v%v ", d.Term(), d.PeerId(), d.proposals[0].index, d.proposals[0].term)
		d.proposals[0].cb.Done(ErrResp(&util.ErrStaleCommand{}))
		d.proposals = d.proposals[1:]
	}

	// non-leader may be wake up client, it's ok
	if len(d.proposals) > 0 && d.proposals[0].term == entry.Term && d.proposals[0].index == entry.Index {
		// log.Infof("[%v] Peer %v Wakeup Propose %v for %v resps: %v", d.Term(), d.PeerId(), d.proposals[0].index, len(resps.Responses), resps.Responses[0])
		if scan {
			d.proposals[0].cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
		}
		d.proposals[0].cb.Done(resp)
		d.proposals = d.proposals[1:]
	}
}
