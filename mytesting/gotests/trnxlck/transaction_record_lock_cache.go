package main

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"reflect"
	"unsafe"
)

type Key string

type TransactionRecordLockCache struct {
	CacheMu struct {
		syncutil.Mutex
		Cache map[Key]*TransactionRecordMutex
	}
}

type TransactionRecordMutex struct {
	syncutil.Mutex
}

func (t *TransactionRecordLockCache) getMutex(k roachpb.Key) *TransactionRecordMutex {
	ikey := ToInternalKey(k)
	t.CacheMu.Lock()
	defer t.CacheMu.Unlock()
	_, ok := t.CacheMu.Cache[ikey]
	if !ok {
		t.CacheMu.Cache[ikey] = NewTransactionRecordMutex()
	}
	return t.CacheMu.Cache[ikey]
}

func (t *TransactionRecordLockCache) getAccess(k roachpb.Key) {

	mutex := t.getMutex(k)
	mutex.Lock()
	fmt.Println("Granted access to ", k)
}

func (t *TransactionRecordLockCache) releaseAccess(k roachpb.Key) {
	mutex := t.getMutex(k)
	fmt.Println("Access released by ", k)
	mutex.Unlock()
}

func ToInternalKey(b []byte) Key {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	s := *(*string)(unsafe.Pointer(&sh))
	return Key(s)
}

func NewTransactionRecordMutex() *TransactionRecordMutex {
	return &TransactionRecordMutex{}
}

func NewTransactionRecordLockCache() *TransactionRecordLockCache {
	tlc := &TransactionRecordLockCache{}
	tlc.CacheMu.Cache = map[Key]*TransactionRecordMutex{}

	return tlc
}
