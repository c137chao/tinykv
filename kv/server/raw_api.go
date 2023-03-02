package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

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
