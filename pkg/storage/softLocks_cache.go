package storage

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"time"
)

type SoftLockCache struct {
	ReadMu struct {
		syncutil.Mutex
		readLockcache map[roachpb.Key]*ReadSoftLock // txn key to metadata
	}
	WriteMu struct {
		syncutil.Mutex
		writeLockcache map[roachpb.Key]*WriteSoftLock // txn key to metadata
	}
}

type ReadSoftLock struct {
	TransactionMeta enginepb.TxnMeta
}

type WriteSoftLock struct {
	TransactionMeta enginepb.TxnMeta
}
