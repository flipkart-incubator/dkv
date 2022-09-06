package rocksdb

import (
	"errors"
	"github.com/linxGnu/grocksdb"
	"io"
)

// NewWriteBatchIterator creates a new write batch iterator initialised with the given
// write batch data byte array.
func NewWriteBatchIterator(wbData []byte) *WriteBatchIterator {
	if len(wbData) < 8+4 {
		return &WriteBatchIterator{}
	}
	return &WriteBatchIterator{data: wbData[12:]}
}

// WriteBatchIterator represents a iterator to iterator over records.
type WriteBatchIterator struct {
	data   []byte
	record grocksdb.WriteBatchRecord
	err    error
}

// Next returns the next record.
// Returns false if no further record exists.
func (iter *WriteBatchIterator) Next() bool {
	if iter.err != nil || len(iter.data) == 0 {
		return false
	}
	// reset the current record
	iter.record.CF = 0
	iter.record.Key = nil
	iter.record.Value = nil

	// parse the record type
	iter.record.Type = iter.decodeRecType()

	switch iter.record.Type {
	case
		grocksdb.WriteBatchDeletionRecord,
		grocksdb.WriteBatchSingleDeletionRecord:
		iter.record.Key = iter.decodeSlice()
	case
		grocksdb.WriteBatchCFDeletionRecord,
		grocksdb.WriteBatchCFSingleDeletionRecord:
		iter.record.CF = int(iter.decodeVarint())
		if iter.err == nil {
			iter.record.Key = iter.decodeSlice()
		}
	case
		grocksdb.WriteBatchValueRecord,
		grocksdb.WriteBatchMergeRecord,
		grocksdb.WriteBatchRangeDeletion,
		grocksdb.WriteBatchBlobIndex:
		iter.record.Key = iter.decodeSlice()
		if iter.err == nil {
			iter.record.Value = iter.decodeSlice()
		}
	case
		grocksdb.WriteBatchCFValueRecord,
		grocksdb.WriteBatchCFRangeDeletion,
		grocksdb.WriteBatchCFMergeRecord,
		grocksdb.WriteBatchCFBlobIndex:
		iter.record.CF = int(iter.decodeVarint())
		if iter.err == nil {
			iter.record.Key = iter.decodeSlice()
		}
		if iter.err == nil {
			iter.record.Value = iter.decodeSlice()
		}
	case grocksdb.WriteBatchLogDataRecord:
		iter.record.Value = iter.decodeSlice()
	case
		grocksdb.WriteBatchNoopRecord,
		grocksdb.WriteBatchBeginPrepareXIDRecord,
		grocksdb.WriteBatchBeginPersistedPrepareXIDRecord:
	case
		grocksdb.WriteBatchEndPrepareXIDRecord,
		grocksdb.WriteBatchCommitXIDRecord,
		grocksdb.WriteBatchRollbackXIDRecord:
		iter.record.Value = iter.decodeSlice()
	default:
		iter.err = errors.New("unsupported wal record type")
	}

	return iter.err == nil

}

// Record returns the current record.
func (iter *WriteBatchIterator) Record() *grocksdb.WriteBatchRecord {
	return &iter.record
}

// Error returns the error if the iteration is failed.
func (iter *WriteBatchIterator) Error() error {
	return iter.err
}

func (iter *WriteBatchIterator) decodeSlice() []byte {
	l := int(iter.decodeVarint())
	if l > len(iter.data) {
		iter.err = io.ErrShortBuffer
	}
	if iter.err != nil {
		return []byte{}
	}
	ret := iter.data[:l]
	iter.data = iter.data[l:]
	return ret
}

func (iter *WriteBatchIterator) decodeRecType() grocksdb.WriteBatchRecordType {
	if len(iter.data) == 0 {
		iter.err = io.ErrShortBuffer
		return grocksdb.WriteBatchNotUsedRecord
	}
	t := iter.data[0]
	iter.data = iter.data[1:]
	return grocksdb.WriteBatchRecordType(t)
}

func (iter *WriteBatchIterator) decodeVarint() uint64 {
	var n int
	var x uint64
	for shift := uint(0); shift < 64 && n < len(iter.data); shift += 7 {
		b := uint64(iter.data[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			iter.data = iter.data[n:]
			return x
		}
	}
	if n == len(iter.data) {
		iter.err = io.ErrShortBuffer
	} else {
		iter.err = errors.New("malformed varint")
	}
	return 0
}
