package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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

	resp := &kvrpcpb.GetResponse{}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := mvccTxn.GetLock(req.GetKey())
	log.Infof("txn:%v getlock: lock %+v, err %v", req.Version, lock, err)
	if err != nil {
		return nil, err
	}

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

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.CommitResponse{}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	writeKinds := make([]mvcc.WriteKind, 0)

	for _, key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			if write, _, _ := mvccTxn.CurrentWrite(key); write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{
						Abort: "true",
					}
				}
			}
			return resp, nil
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
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

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
