package storage

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"reflect"
	"unsafe"
)

type SoftLockCache struct {
	ReadMu struct {
		syncutil.Mutex
		readSoftLockCache map[Key]*ReadSoftLockQueue // txn key to metadata
	}
	WriteMu struct {
		syncutil.Mutex
		writeSoftLockCache map[Key]*WriteSoftLockQueue // txn key to metadata
	}
}
type Key string

type ReadSoftLockQueue struct {
	Queue []ReadSoftLock
}

type WriteSoftLockQueue struct {
	Queue []WriteSoftLock
}

type ReadSoftLock struct {
	TransactionMeta enginepb.TxnMeta
	key             roachpb.Key
}

type WriteSoftLock struct {
	TransactionMeta enginepb.TxnMeta
	key             roachpb.Key
	value           roachpb.Value
}

func NewReadSoftLockQueue() *ReadSoftLockQueue {
	r := &ReadSoftLockQueue{
		Queue: make([]ReadSoftLock, 0),
	}
	return r
}

func NewWriteSoftLockQueue() *WriteSoftLockQueue {
	w := &WriteSoftLockQueue{
		Queue: make([]WriteSoftLock, 0),
	}
	return w
}

func (s *SoftLockCache) addToSoftReadLockCache(readlk ReadSoftLock) {
	internalkey := ToInternalKey(readlk.key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	_, ok := s.ReadMu.readSoftLockCache[internalkey]
	if !ok {
		s.ReadMu.readSoftLockCache[internalkey] = NewReadSoftLockQueue()
	}
	s.ReadMu.readSoftLockCache[internalkey].append(readlk)

}

func (s *SoftLockCache) removeFromReadLockCache(readlk ReadSoftLock) {
	internalkey := ToInternalKey(readlk.key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	Q, ok := s.ReadMu.readSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == *readlk.TransactionMeta.ID {
				position = index
				fmt.Println("read lock found", lock)
			}
		}
		if position != -1 {
			Q.Queue = append(Q.Queue[:position], Q.Queue[position+1:]...)
			fmt.Println("removing read lock")
		} else {
			// no such lock
		}
	} else {
		// Nothing to remove
	}

}

func (s *SoftLockCache) addToSoftWriteLockCache(writelk WriteSoftLock) {
	internalkey := ToInternalKey(writelk.key)

	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	_, ok := s.WriteMu.writeSoftLockCache[internalkey]
	if !ok {
		s.WriteMu.writeSoftLockCache[internalkey] = NewWriteSoftLockQueue()
	}
	s.WriteMu.writeSoftLockCache[internalkey].append(writelk)

}

func (s *SoftLockCache) removeFromWriteLockCache(writelk WriteSoftLock) {
	internalkey := ToInternalKey(writelk.key)

	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	Q, ok := s.WriteMu.writeSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == *writelk.TransactionMeta.ID {
				position = index
			}
		}
		if position != -1 {
			Q.Queue = append(Q.Queue[:position], Q.Queue[position+1:]...)
		} else {
			// no such lock
		}
	} else {
		// Nothing to remove
	}

}

func (s *SoftLockCache) getWriteSoftLock(key roachpb.Key, tmeta enginepb.TxnMeta) (writelk WriteSoftLock) {
	internalkey := ToInternalKey(key)

	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	Q, ok := s.WriteMu.writeSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == *tmeta.ID {
				position = index
			}
		}
		if position != -1 {
			writelk = Q.Queue[position]
		}
	}
	return writelk
}

func (s *SoftLockCache) getAllReadSoftLocksOnKey(key roachpb.Key) []ReadSoftLock {
	internalkey := ToInternalKey(key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	var q []ReadSoftLock
	Q, ok := s.ReadMu.readSoftLockCache[internalkey]

	if ok {
		q = make([]ReadSoftLock, len(Q.Queue))
		copy(q, Q.Queue)
	} else {
		q = make([]ReadSoftLock, 0)
	}

	return q
}

func (s *SoftLockCache) getAllWriteSoftLocksOnKey(key roachpb.Key) []WriteSoftLock {
	internalkey := ToInternalKey(key)

	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	var q []WriteSoftLock
	Q, ok := s.WriteMu.writeSoftLockCache[internalkey]

	if ok {
		q = make([]WriteSoftLock, len(Q.Queue))
		copy(q, Q.Queue)
	} else {
		q = make([]WriteSoftLock, 0)
	}

	return q
}

func (r *ReadSoftLockQueue) append(readlk ReadSoftLock) {
	r.Queue = append(r.Queue, readlk)
}

func (w *WriteSoftLockQueue) append(writelk WriteSoftLock) {
	w.Queue = append(w.Queue, writelk)
}

//converting byte[] to string
func ToInternalKey(b []byte) Key {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	s := *(*string)(unsafe.Pointer(&sh))
	return Key(s)
}

/*
func (k *Key) StringToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{sh.Data, sh.Len, 0}
	return *(*[]byte)(unsafe.Pointer(&bh))
}*/

func NewSoftLockCache() *SoftLockCache {
	slc := &SoftLockCache{}

	slc.ReadMu.readSoftLockCache = map[Key]*ReadSoftLockQueue{}
	slc.WriteMu.writeSoftLockCache = map[Key]*WriteSoftLockQueue{}

	return slc
}
