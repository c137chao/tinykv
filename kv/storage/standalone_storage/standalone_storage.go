package standalone_storage

import (
	"errors"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

var ErrStorageStopped = errors.New("operation on stopped storage")

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	stopped    bool
	config     *config.Config // not used now
	cf_storage *engine_util.Engines
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStorageReader(txn *badger.Txn) (*StandAloneStorageReader, error) {
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return val, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Commit()
	s.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kv_path := filepath.Join(conf.DBPath, "kv")
	raft_path := filepath.Join(conf.DBPath, "raft")

	// create DB will mkdir  for badger db
	kv_db := engine_util.CreateDB(kv_path, false)
	raft_db := engine_util.CreateDB(raft_path, true)

	return &StandAloneStorage{
		stopped:    false,
		config:     conf,
		cf_storage: engine_util.NewEngines(kv_db, raft_db, kv_path, raft_path),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.stopped = false
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.stopped = true
	if err := s.cf_storage.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.stopped {
		return nil, ErrStorageStopped
	}
	txn := s.cf_storage.Kv.NewTransaction(false)
	return NewStorageReader(txn)
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.stopped {
		return ErrStorageStopped
	}

	wb := &engine_util.WriteBatch{}

	for _, data := range batch {
		if _, ok := data.Data.(storage.Put); ok {
			wb.SetCF(data.Cf(), data.Key(), data.Value())
		} else if _, ok := data.Data.(storage.Delete); ok {
			wb.DeleteCF(data.Cf(), data.Key())

		} else {
			panic("unsupport modify type")
		}
	}

	if err := s.cf_storage.WriteKV(wb); err != nil {
		return err
	}

	return nil
}
