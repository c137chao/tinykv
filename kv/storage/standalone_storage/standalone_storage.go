package standalone_storage

import (
	"path"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	cf_storage *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kv_path := path.Join(conf.DBPath, "kv")
	raft_path := path.Join(conf.DBPath, "raft")

	kv_db := engine_util.CreateDB(kv_path, false)
	raft_db := engine_util.CreateDB(raft_path, conf.Raft)

	s := &StandAloneStorage{cf_storage: engine_util.NewEngines(kv_db, raft_db, kv_path, raft_path)}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// return s.cf_storage.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.cf_storage.Kv.NewTransaction(false)
	return storage.NewStorageReader(txn)
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error = nil

	for _, data := range batch {
		if _, ok := data.Data.(storage.Put); ok {
			err = engine_util.PutCF(s.cf_storage.Kv, data.Cf(), data.Key(), data.Value())
		} else if _, ok := data.Data.(storage.Delete); ok {
			err = engine_util.DeleteCF(s.cf_storage.Kv, data.Cf(), data.Key())
		} else {
			panic("Not a Write Modify")
		}

		if err != nil {
			break
		}
	}
	return err
}
