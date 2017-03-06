package engine

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"golang.org/x/net/context"
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
	Queue []roachpb.ReadSoftLock
}

type WriteSoftLockQueue struct {
	Queue []roachpb.WriteSoftLock
}

func (s *SoftLockCache) processPlaceReadLockRequest(
	ctx context.Context,
	tmeta enginepb.TxnMeta,
	key roachpb.Key,
	reverse bool) []roachpb.WriteSoftLock {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceReadLockRequest , key %v , inetrnalkey %v", key, ToInternalKey(key))
	}
	readlk := roachpb.ReadSoftLock{TransactionMeta: tmeta, Reverse: reverse}
	s.addToSoftReadLockCache(readlk, key)
	wlks := s.getAllWriteSoftLocksOnKey(key)
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceReadLockRequest, wlks %v", wlks)
	}
	wlks = removeMyEntriesFromWriteLocks(ctx, wlks, *tmeta.ID)
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceReadLockRequest, wlks after %v", wlks)
	}
	return wlks
}

func (s *SoftLockCache) processPlaceWriteLockRequest(
	ctx context.Context,
	tmeta enginepb.TxnMeta,
	key roachpb.Key,
	req roachpb.RequestUnion) ([]roachpb.ReadSoftLock, []roachpb.WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceWriteLockRequest , key %v , inetrnalkey %v", key, ToInternalKey(key))
	}
	writelk := roachpb.WriteSoftLock{TransactionMeta: tmeta, Request: req}
	s.addToSoftWriteLockCache(writelk, key)
	rlks := s.getAllReadSoftLocksOnKey(key)
	rlks = removeMyEntriesFromReadLocks(ctx, rlks, *tmeta.ID)
	wlks := s.getAllWriteSoftLocksOnKey(key)
	wlks = removeMyEntriesFromWriteLocks(ctx, wlks, *tmeta.ID)

	return rlks, wlks
}

func removeMyEntriesFromReadLocks(ctx context.Context, rlks []roachpb.ReadSoftLock, txnID uuid.UUID) []roachpb.ReadSoftLock {
	newrlks := make([]roachpb.ReadSoftLock, 0)
	for _, lock := range rlks {
		if *lock.TransactionMeta.ID != txnID {
			newrlks = append(newrlks, lock)
		} else {
			if log.V(2) {
				log.Infof(ctx, "Removed my own read lock from retrived list", lock)
			}
		}

	}
	return newrlks
}

func removeMyEntriesFromWriteLocks(ctx context.Context, wlks []roachpb.WriteSoftLock, txnID uuid.UUID) []roachpb.WriteSoftLock {
	newwlks := make([]roachpb.WriteSoftLock, 0)
	for _, lock := range wlks {

		if *lock.TransactionMeta.ID != txnID {
			if log.V(2) {
				log.Infof(ctx, "removeMyEntriesFromWriteLocks adding %v ", lock)
			}
			newwlks = append(newwlks, lock)
		} else {
			if log.V(2) {
				log.Infof(ctx, "Removed my own write lock from retrived list", lock)
			}
		}

	}
	return newwlks
}

func NewReadSoftLockQueue() *ReadSoftLockQueue {
	r := &ReadSoftLockQueue{
		Queue: make([]roachpb.ReadSoftLock, 0),
	}
	return r
}

func NewWriteSoftLockQueue() *WriteSoftLockQueue {
	w := &WriteSoftLockQueue{
		Queue: make([]roachpb.WriteSoftLock, 0),
	}
	return w
}

func (s *SoftLockCache) addToSoftReadLockCache(readlk roachpb.ReadSoftLock, key roachpb.Key) {
	internalkey := ToInternalKey(key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	_, ok := s.ReadMu.readSoftLockCache[internalkey]
	if !ok {
		s.ReadMu.readSoftLockCache[internalkey] = NewReadSoftLockQueue()
	}
	s.ReadMu.readSoftLockCache[internalkey].append(readlk)

}

func (s *SoftLockCache) removeFromReadLockCache(readlk roachpb.ReadSoftLock, key roachpb.Key) {
	internalkey := ToInternalKey(key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	Q, ok := s.ReadMu.readSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == *readlk.TransactionMeta.ID {
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

func (s *SoftLockCache) addToSoftWriteLockCache(writelk roachpb.WriteSoftLock, key roachpb.Key) {
	internalkey := ToInternalKey(key)

	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	_, ok := s.WriteMu.writeSoftLockCache[internalkey]
	if !ok {
		s.WriteMu.writeSoftLockCache[internalkey] = NewWriteSoftLockQueue()
	}
	s.WriteMu.writeSoftLockCache[internalkey].append(writelk)

}

func (s *SoftLockCache) removeFromWriteLockCache(writelk roachpb.WriteSoftLock, key roachpb.Key) {
	internalkey := ToInternalKey(key)

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

func (s *SoftLockCache) getWriteSoftLock(
	key roachpb.Key,
	txnID uuid.UUID) (roachpb.WriteSoftLock, bool) {
	internalkey := ToInternalKey(key)
	var writelk roachpb.WriteSoftLock
	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	Q, ok := s.WriteMu.writeSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == txnID {
				position = index
			}
		}
		if position != -1 {
			writelk = Q.Queue[position]
		}
	}
	if !ok || position == -1 {
		return writelk, false
	}
	return writelk, true
}

func (s *SoftLockCache) getReadSoftLock(
	key roachpb.Key,
	txnID uuid.UUID) (roachpb.ReadSoftLock, bool) {
	internalkey := ToInternalKey(key)
	var readlk roachpb.ReadSoftLock
	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	Q, ok := s.ReadMu.readSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == txnID {
				position = index
			}
		}
		if position != -1 {
			readlk = Q.Queue[position]
		}
	}
	if !ok || position == -1 {
		return readlk, false
	}
	return readlk, true

}

func (s *SoftLockCache) getAllReadSoftLocksOnKey(
	key roachpb.Key) []roachpb.ReadSoftLock {
	internalkey := ToInternalKey(key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	var q []roachpb.ReadSoftLock
	Q, ok := s.ReadMu.readSoftLockCache[internalkey]

	if ok {
		q = make([]roachpb.ReadSoftLock, len(Q.Queue))
		copy(q, Q.Queue)
	} else {
		q = make([]roachpb.ReadSoftLock, 0)
	}

	return q
}

func (s *SoftLockCache) getAllWriteSoftLocksOnKey(
	key roachpb.Key) []roachpb.WriteSoftLock {
	internalkey := ToInternalKey(key)

	s.WriteMu.Lock()
	defer s.WriteMu.Unlock()

	var q []roachpb.WriteSoftLock
	Q, ok := s.WriteMu.writeSoftLockCache[internalkey]

	if ok {
		q = make([]roachpb.WriteSoftLock, len(Q.Queue))
		copy(q, Q.Queue)
	} else {
		q = make([]roachpb.WriteSoftLock, 0)
	}

	return q
}

func (r *ReadSoftLockQueue) append(readlk roachpb.ReadSoftLock) {
	r.Queue = append(r.Queue, readlk)
}

func (w *WriteSoftLockQueue) append(writelk roachpb.WriteSoftLock) {
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
