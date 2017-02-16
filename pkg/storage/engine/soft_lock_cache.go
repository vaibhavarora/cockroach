package engine

import (
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
		log.Infof(ctx, "Ravi : In processPlaceReadLockRequest")
	}
	readlk := roachpb.ReadSoftLock{TransactionMeta: tmeta, Reverse: reverse}
	s.addToSoftReadLockCache(readlk, key)
	return s.getAllWriteSoftLocksOnKey(key)
}

func (s *SoftLockCache) processPlaceWriteLockRequest(
	ctx context.Context,
	tmeta enginepb.TxnMeta,
	key roachpb.Key,
	req roachpb.RequestUnion) ([]roachpb.ReadSoftLock, []roachpb.WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In processPlaceWriteLockRequest")
	}
	writelk := roachpb.WriteSoftLock{TransactionMeta: tmeta, Request: req}
	s.addToSoftWriteLockCache(writelk, key)
	rlks := s.getAllReadSoftLocksOnKey(key)
	wlks := s.getAllWriteSoftLocksOnKey(key)
	position := -1
	for index, lock := range wlks {
		if *lock.TransactionMeta.ID == *tmeta.ID {
			position = index
			if log.V(2) {
				log.Infof(ctx, "Found my write lock", lock)
			}
		}
	}
	if position != -1 {
		wlks = append(wlks[:position], wlks[position+1:]...)
		if log.V(2) {
			log.Infof(ctx, "removing my write lock from retrived list lock")
		}
	}
	return rlks, wlks
}

/*

func (s *SoftLockCache) servePut(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request) ([]ReadSoftLock, []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In servePut")
	}
	arg := req.(*roachpb.PutRequest)



	return rlks, wlks
}

func (s *SoftLockCache) serveConditionalPut(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request) ([]ReadSoftLock, []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveConditionalPut")
	}
	arg := req.(*roachpb.ConditionalPutRequest)

	rlks, wlks := s.processPlaceWriteLockRequest(ctx, h, arg.Key, arg.Value, req)

	return rlks, wlks
}

func (s *SoftLockCache) serveInitPut(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request) ([]ReadSoftLock, []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveInitPut")
	}
	arg := req.(*roachpb.InitPutRequest)

	rlks, wlks := s.processPlaceWriteLockRequest(ctx, h, arg.Key, arg.Value, req)

	return rlks, wlks
}

func (s *SoftLockCache) serveIncrement(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveIncrement")
	}
	// Convert this to r(x),w(x)
	// that is.
	// Place read lock
	// get the value from mvcc
	// Increment it locally
	// Place write lock with the incremented value
	// send the incremented value in response
}

func (s *SoftLockCache) serveDelete(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request) (rlks []ReadSoftLock, wlks []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveDelete")
	}
	arg := req.(*roachpb.DeleteRequest)
	var dummy roachpb.Value
	rlks, wlks = s.processPlaceWriteLockRequest(ctx, h, arg.Key, dummy, req)

	return rlks, wlks
}

func (s *SoftLockCache) serveDeleteRange(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request,
	batch ReadWriter) (rlks []ReadSoftLock, wlks []WriteSoftLock) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In  serveDeleteRange")
	}
	arg := req.(*roachpb.DeleteRangeRequest)
	keys := s.getkeysusingIter(ctx, arg.Span.Key, arg.Span.EndKey, batch, h, false)
	var dummy roachpb.Value
	for _, key := range keys {
		rlk, wlk := s.processPlaceWriteLockRequest(ctx, h, key, dummy, req)
		rlks = append(rlks, rlk...)
		wlks = append(wlks, wlk...)
	}
	return rlks, wlks
}



func (s *SoftLockCache) serveScan(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request,
	batch ReadWriter) []WriteSoftLock {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveScan")
	}
	var wlks []WriteSoftLock

	arg := req.(*roachpb.ScanRequest)

	keys := s.getkeysusingIter(ctx, arg.Span.Key, arg.Span.EndKey, batch, h, false)

	if log.V(2) {
		log.Infof(ctx, "Ravi : Scan key :", keys)
	}
	for _, key := range keys {
		wlks = append(wlks, s.processPlaceReadLockRequest(ctx, h, key, req)...)
	}
	return wlks
}

func (s *SoftLockCache) serveReverseScan(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request,
	batch ReadWriter) []WriteSoftLock {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveReverseScan")
	}
	var wlks []WriteSoftLock
	arg := req.(*roachpb.ReverseScanRequest)

	keys := s.getkeysusingIter(ctx, arg.Span.Key, arg.Span.EndKey, batch, h, true)

	if log.V(2) {
		log.Infof(ctx, "Ravi : Reverse Scan key :", keys)
	}
	for _, key := range keys {
		wlks = append(wlks, s.processPlaceReadLockRequest(ctx, h, key, req)...)
	}
	return wlks
}

func (s *SoftLockCache) serveEndTransaction(
	ctx context.Context,
	h roachpb.Header,
	req roachpb.Request,
	batch ReadWriter) (wlks []WriteSoftLock, extrenal_write_spans []roachpb.Span, external_read_span []roachpb.Span) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In serveEndTransaction")
	}

	arg := req.(*roachpb.EndTransactionRequest)
	// remove all write locks
	for _, span := range arg.IntentSpans {
		if len(span.EndKey) == 0 { // single keys
			wlk := s.getWriteSoftLock(span.Key, h.Txn.TxnMeta)
			if wlk.request == nil {
				// intent belons to another range
				extrenal_write_spans = append(extrenal_write_spans, span)
				continue
			}
			wlks = append(wlks, wlk)
			s.removeFromWriteLockCache(wlk)
			if log.V(2) {
				log.Infof(ctx, "Ravi : removed write lock %v", wlk)
			}
		} else { // range of keys

		}
	}
	// remove all read locks
	for _, span := range arg.ReadSpans {
		if log.V(2) {
			log.Infof(ctx, "Ravi : removed read lock %v", span)
		}

		// get one read lock to check if the range is straight or reverse
		readlk := s.getReadSoftLock(span.Key, h.Txn.TxnMeta)
		req := readlk.request
		var keys []roachpb.Key

		if req == nil {
			if log.V(2) {
				log.Infof(ctx, "Ravi : no req in readlk %v, readlk %v", req, readlk)
			}
			// span belongs to different range
			external_read_span = append(external_read_span, span)
			continue
		}

		// forward range
		if req.Method() == roachpb.Scan {
			keys = s.getkeysusingIter(ctx, span.Key, span.EndKey, batch, h, false)
			// reverse range
		} else if req.Method() == roachpb.ReverseScan {
			keys = s.getkeysusingIter(ctx, span.Key, span.EndKey, batch, h, true)
		}
		// remove read lock on each key
		for _, k := range keys {
			rl := s.getReadSoftLock(k, h.Txn.TxnMeta)
			s.removeFromReadLockCache(rl)
		}

	}
	return wlks, external_read_span, extrenal_write_spans
}
*/
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
	tmeta enginepb.TxnMeta) (writelk roachpb.WriteSoftLock) {
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

func (s *SoftLockCache) getReadSoftLock(
	key roachpb.Key,
	tmeta enginepb.TxnMeta) (readlk roachpb.ReadSoftLock) {
	internalkey := ToInternalKey(key)

	s.ReadMu.Lock()
	defer s.ReadMu.Unlock()

	Q, ok := s.ReadMu.readSoftLockCache[internalkey]
	position := -1
	if ok {
		for index, lock := range Q.Queue {
			if *lock.TransactionMeta.ID == *tmeta.ID {
				position = index
			}
		}
		if position != -1 {
			readlk = Q.Queue[position]
		}
	}
	return readlk
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
