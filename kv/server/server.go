package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// KvGet reads a value from the database at a supplied timestamp.
// If the key to be read is locked by another transaction at the time of the KvGet request, then return an error.
// Otherwise, TinyKV must search the versions of the key to find the most recent, valid value.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.GetResponse{}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := mvccTxn.GetLock(req.GetKey())
	log.Infof("txn:%v getlock: lock %+v, err %v", req.Version, lock, err)
	if err != nil {
		if regionErr, ok := (err).(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// if key is locked by another txn
	if lock != nil && req.Version >= lock.Ts {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.GetKey(),
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}

	val, err := mvccTxn.GetValue(req.Key)
	log.Infof("txn:%v getlock: val %v, err %v", req.Version, val, err)

	if err != nil {
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
	}

	resp.Value = val

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.PrewriteResponse{}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// check conflict of WW
	keyErrs := make([]*kvrpcpb.KeyError, 0)
	for _, mutation := range req.Mutations {
		write, commitTS, err := mvccTxn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTS > req.StartVersion {
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTS,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
		}

	}
	if len(keyErrs) != 0 {
		resp.Errors = keyErrs
		return resp, nil
	}

	for _, mutation := range req.Mutations {
		lock, err := mvccTxn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts < req.StartVersion {
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
		}
	}

	if len(keyErrs) != 0 {
		resp.Errors = keyErrs
		return resp, nil
	}

	// acquire lock
	for _, mutation := range req.Mutations {
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKind(mutation.Op + 1),
		}
		mvccTxn.PutLock(mutation.Key, lock)
	}

	// Put Value
	for _, mutation := range req.Mutations {
		mvccTxn.PutValue(mutation.Key, mutation.Value)
	}

	if err := server.storage.Write(req.Context, mvccTxn.Writes()); err != nil {
		return nil, err
	}

	log.Infof("response: %+v", resp)

	return resp, nil
}

// commit phase, put write record to write column
// assume crash happen in crash, when server restore, if lock on key still hold by txn, it will commit again, else retry txn
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.CommitResponse{}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	writeKinds := make([]mvcc.WriteKind, 0)

	for _, key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			// txn has been commited or aborted
			if write, _, _ := mvccTxn.CurrentWrite(key); write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{
						Abort: "true",
					}
				}
			}
			// lock may timeout and acquire by other txn
			// the txn can guarantee isoluation, txn should try restart
			if lock != nil {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
			}
			return resp, nil
		}

		writeKinds = append(writeKinds, lock.Kind)
	}

	for i, key := range req.Keys {
		write := mvcc.Write{StartTS: req.StartVersion, Kind: writeKinds[i]}
		mvccTxn.PutWrite(key, req.CommitVersion, &write)
	}

	for _, key := range req.Keys {
		mvccTxn.DeleteLock(key)
	}

	if err := server.storage.Write(req.Context, mvccTxn.Writes()); err != nil {
		return nil, err
	}

	return resp, nil
}

// scan all key, value which commited at the time of some start timestamp
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	resp := &kvrpcpb.ScanResponse{}
	limit := req.Limit
	for i := 0; i < int(limit); i++ {
		key, val, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if val == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
	}

	return resp, nil
}

// KvCheckTxnStatus checks for timeouts, removes expired locks, and returns the status of the lock.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// 1. checks that if there is a lock and its ttl has expired, then it is rolled back.
	// 2. checks that if there is a lock and its ttl has not expired, then nothing changes.
	// 3. checking a key which has already been rolled back..
	// 4. checking a key which has already been committed.
	// 5. if there is no data for the key, then we get the right response.
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.Action_NoAction,
	}
	rollBackWrite := &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)

	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	write, commitTs, err := mvccTxn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	// write is not nil, it has been rollback or commit
	// delete lock if it doesn't release lock
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTs
		}
		// clear lock
		if lock != nil && lock.Ts == req.LockTs {
			mvccTxn.DeleteLock(req.PrimaryKey)
			server.storage.Write(req.Context, mvccTxn.Writes())
		}

		return resp, nil
	}

	// write is nil, txn doesn't commit or rollback
	// if it hold lock, just response it lock status and do nothing
	// else if lock is timeout, rollback
	if lock != nil {
		// no lock, primary key has been lock success
		resp.LockTtl = lock.Ttl

		// lock timeout
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			mvccTxn.DeleteLock(req.PrimaryKey)
			mvccTxn.DeleteValue(req.PrimaryKey)
			mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, rollBackWrite)
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			server.storage.Write(req.Context, mvccTxn.Writes())
		}

		return resp, nil
	}

	// write is nil and lock is nil, roll back
	resp.Action = kvrpcpb.Action_LockNotExistRollback
	mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, rollBackWrite)
	server.storage.Write(req.Context, mvccTxn.Writes())

	return resp, nil
}

// delete value and lock if exist
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.BatchRollbackResponse{}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	rollBackWrite := &mvcc.Write{
		StartTS: req.StartVersion,
		Kind:    mvcc.WriteKindRollback,
	}

	for _, key := range req.Keys {
		write, _, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := (err).(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			if regionErr, ok := (err).(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		// if write is not nil, it must has been commited or rollback
		// rollback commited key
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			break
		} else if write == nil {
			mvccTxn.DeleteValue(key)
			if lock != nil && lock.Ts == req.StartVersion {
				mvccTxn.DeleteLock(key)
			}

			mvccTxn.PutWrite(key, req.StartVersion, rollBackWrite)
		}

	}
	server.storage.Write(req.Context, mvccTxn.Writes())

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.ResolveLockResponse{}
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	var keys [][]byte

	// find all lock of current txn, get their keys
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.Key())
		}
	}
	// no lock
	if len(keys) == 0 {
		return resp, nil
	}

	// rollback if commit version equals 0
	// commit else
	if req.CommitVersion == 0 {
		rollbackResp, err := server.KvBatchRollback(context.TODO(), &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = rollbackResp.Error
		resp.RegionError = rollbackResp.RegionError

	} else {
		commitResp, err := server.KvCommit(context.TODO(), &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = commitResp.Error
		resp.RegionError = commitResp.RegionError
	}

	return resp, err

}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
