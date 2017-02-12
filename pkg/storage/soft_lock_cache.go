package storage

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

func (s *SoftLockCache) processPlaceReadLockRequest(ctx context.Context, h roachpb.Header, key roachpb.Key) []WriteSoftLock {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceReadLockRequest")
	}
	readlk := ReadSoftLock{TransactionMeta: h.Txn.TxnMeta, key: key}
	s.addToSoftReadLockCache(readlk)
	return s.getAllWriteSoftLocksOnKey(key)
}

func (s *SoftLockCache) processPlaceWriteLockRequest(ctx context.Context, h roachpb.Header, key roachpb.Key, value roachpb.Value) ([]ReadSoftLock, []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceWriteLockRequest")
	}
	writelk := WriteSoftLock{TransactionMeta: h.Txn.TxnMeta, key: key, value: value}
	s.addToSoftWriteLockCache(writelk)
	rlks := s.getAllReadSoftLocksOnKey(key)
	wlks := s.getAllWriteSoftLocksOnKey(key)
	position := -1
	for index, lock := range wlks {
		if *lock.TransactionMeta.ID == *h.Txn.TxnMeta.ID {
			position = index
			if log.V(2) {
				fmt.Println("Found my write lock", lock)
			}
		}
	}
	if position != -1 {
		wlks = append(wlks[:position], wlks[position+1:]...)
		if log.V(2) {
			fmt.Println("removing my write lock from retrived list lock")
		}
	}
	return rlks, wlks
}

func (s *SoftLockCache) serveGet(ctx context.Context, h roachpb.Header, req roachpb.Request) []WriteSoftLock {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveGet")
	}
	arg := req.(*roachpb.GetRequest)
	return s.processPlaceReadLockRequest(ctx, h, arg.Key)

}

func (s *SoftLockCache) servePut(ctx context.Context, h roachpb.Header, req roachpb.Request) ([]ReadSoftLock, []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In servePut")
	}
	arg := req.(*roachpb.PutRequest)

	rlks, wlks := s.processPlaceWriteLockRequest(ctx, h, arg.Key, arg.Value)

}

func (s *SoftLockCache) serveConditionalPut(ctx context.Context, h roachpb.Header, req roachpb.Request) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveConditionalPut")
	}
}

func (s *SoftLockCache) serveInitPut(ctx context.Context, h roachpb.Header, req roachpb.Request) {

}

func (s *SoftLockCache) serveIncrement(ctx context.Context, h roachpb.Header, req roachpb.Request) {

}

func (s *SoftLockCache) serveDelete(ctx context.Context, h roachpb.Header, req roachpb.Request) {

}

func (s *SoftLockCache) serveDeleteRange(ctx context.Context, h roachpb.Header, req roachpb.Request) {

}

func (s *SoftLockCache) serveScan(ctx context.Context, h roachpb.Header, req roachpb.Request) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveScan")
	}

}

func (s *SoftLockCache) serveReverseScan(ctx context.Context, h roachpb.Header, req roachpb.Request) {

}

func (s *SoftLockCache) serveEndTransaction(ctx context.Context, h roachpb.Header, req roachpb.Request) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveEndTransaction")
	}
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
