package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	mvccTxn *MvccTxn
	nextTs  uint64
	nextKey []byte
	iter    engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scanner := &Scanner{
		mvccTxn: txn,
		nextKey: startKey,
		nextTs:  txn.StartTS,
		iter:    iter,
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	scan.iter.Seek(EncodeKey(scan.nextKey, scan.nextTs))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	encKey := scan.iter.Item().Key()
	encWrite, err := scan.iter.Item().Value()
	if err != nil {
		return nil, nil, err
	}

	key := DecodeUserKey(encKey)
	write, err := ParseWrite(encWrite)
	if err != nil {
		return nil, nil, err
	}

	// if key change, maybe find a write record with timestamp later than curr txn
	// find again use txn start timestamp
	if !bytes.Equal(key, scan.nextKey) {
		scan.nextTs = scan.mvccTxn.StartTS
		scan.nextKey = key
		return scan.Next()
	}

	scan.nextTs = write.StartTS
	scan.nextKey = key

	// if write is delete record, skip and find next one
	if write.Kind == WriteKindDelete {
		return scan.Next()
	}

	val, err := scan.mvccTxn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return nil, nil, err
	}

	return key, val, nil
}
