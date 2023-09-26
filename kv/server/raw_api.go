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
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	response := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}

	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}

	put_data := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	modify := storage.Modify{Data: put_data}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		response.Error = err.Error()
	}

	return response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}

	delete_data := storage.Delete{Key: req.Key, Cf: req.Cf}
	modifies := []storage.Modify{{Data: delete_data}}

	err := server.storage.Write(req.Context, modifies)
	if err != nil {
		response.Error = err.Error()
	}

	return response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)
	// Ini
	iter.Seek(req.StartKey)

	kvs := make([]*kvrpcpb.KvPair, 0, req.Limit)
	limit := req.Limit

	for ; iter.Valid() && limit > 0; iter.Next() {
		val, err := iter.Item().Value()
		if err != nil {
			panic("db item value error")
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: iter.Item().Key(), Value: val})
		limit -= 1
	}

	response := &kvrpcpb.RawScanResponse{Kvs: kvs}

	return response, nil
}
