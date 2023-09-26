# Project 1 Document
[官方指导书传送门](https://github.com/talent-plan/tinykv/blob/course/doc/project1-StandaloneKV.md)

Reading [Big Table](https://www.cs.cornell.edu/courses/cs6464/2009sp/papers/bigtable.pdf) is helpful for you to understand Column Family and LSM Tree
Chinese Version : [DataBase Lab of Xiamen University](https://dblab.xmu.edu.cn/post/google-bigtable/)

### Design:
##### Part I
According documention, we know we should implement interface through badger(LSM-Tree).
However, badger doesn't support Column Family, we need use engine
```go
type Engines struct {
	// Data, including data which is committed (i.e., committed across other nodes) and un-committed (i.e., only present
	// locally).
	Kv     *badger.DB
	KvPath string
	// Metadata used by Raft.
	Raft     *badger.DB
	RaftPath string
}
```
KvPath can simple seen as Column Name
Some API given in [engines.go](./util/engines.go), but you can see arg of WriteKV is `WriteBatch`
I guess it used for writing raft log, may be improve performance through batch processing.

But in this progect, we just write a k-v pair (see more in `storage.Modify`) to badger.DB
So actually I use simplier API in [util.go](./util/engine_util/util.go).

Now, add implement to StandAloneStorage 

```go
type StandAloneStorage struct {
	// Your Data Here (1).
	cf_storage *engine_util.Engines
}
```
Implement storage API 

```go
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
    // conf.DBPath is temp path in linux, join format new path through args
	kv_path := path.Join(conf.DBPath, "kv")
	raft_path := path.Join(conf.DBPath, "raft")

	kv_db := engine_util.CreateDB(kv_path, false)
	raft_db := engine_util.CreateDB(raft_path, conf.Raft)

	s := &StandAloneStorage{cf_storage: engine_util.NewEngines(kv_db, raft_db, kv_path, raft_path)}

	return s
}
```

`Put` And `Delete` are both write operation
```go
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
```

Reader should return a `StorageReader` that supports key/value's point get and scan operations on a snapshot.
```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
}
```
We can find `StorageReader` just a interface without implementation
```go
type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
```
According hints, use `badger.Txn` to implement it.
```go
type MyP1StorageReader struct {
	txn *badger.Txn
}
```
`StorageReader` Create
```go
func NewStorageReader(txn *badger.Txn) (*MyP1StorageReader, error) {
	return &MyP1StorageReader{txn: txn}, nil
}
```

You will find GetCF can't return err when it doesn't found key through test.
```go
func (s *MyP1StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return val, err
}
```

```go
func (s *MyP1StorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}
```

```go
func (s *MyP1StorageReader) Close() {
	s.txn.Discard()
}
```

Now, complete Reader function.
```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
    txn := s.cf_storage.Kv.NewTransaction(false)
	return storage.NewStorageReader(txn)
}
```

##### Part II
This part is simple
```go

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)

	if err != nil {
		panic(err)
	}

	value, err := reader.GetCF(req.Cf, req.Key)

	response := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: len(value) == 0,
	}

	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put_data := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	modify := storage.Modify{Data: put_data}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	response := &kvrpcpb.RawPutResponse{} // need init anything ???

	return response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete_data := storage.Delete{Key: req.Key, Cf: req.Cf}
	modifies := []storage.Modify{{Data: delete_data}}

	err := server.storage.Write(req.Context, modifies)
	response := &kvrpcpb.RawDeleteResponse{}

	return response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)

	if err != nil {
		panic(err)
	}

	iter := reader.IterCF(req.Cf)
	// if you forget to seek, iter will be un-valid
	iter.Seek(req.StartKey)

	kvs := make([]*kvrpcpb.KvPair, 0, req.Limit)
	limit := req.Limit

	for iter.Valid() && limit > 0 {
		val, err := iter.Item().Value()
		// err can be not nil ???
		if err != nil {
			panic("storage RawScan: iterate a un-exist item ??? ")
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: iter.Item().Key(), Value: val})

		iter.Next()
		limit -= 1
	}

	response := &kvrpcpb.RawScanResponse{Kvs: kvs}

	return response, nil
}

```



