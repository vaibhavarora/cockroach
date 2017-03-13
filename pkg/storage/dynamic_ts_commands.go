package storage

import (
	//"fmt"
	"bytes"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type RpcArgs struct {
	lowerbound hlc.Timestamp
	upperbound hlc.Timestamp
	commitAQ   []enginepb.TxnMeta
	commitBQ   []enginepb.TxnMeta
}

type DyTSCommand struct {
	EvalDyTSCommand func(context.Context, engine.ReadWriter, CommandArgs, roachpb.Response) (EvalResult, error)
}

var DyTSCommands = map[roachpb.Method]DyTSCommand{
	roachpb.Get:            {EvalDyTSCommand: EvalDyTSGet},
	roachpb.Put:            {EvalDyTSCommand: EvalDyTSPut},
	roachpb.ConditionalPut: {EvalDyTSCommand: EvalDyTSConditionalPut},
	roachpb.InitPut:        {EvalDyTSCommand: EvalDyTSInitPut},
	roachpb.Increment:      {EvalDyTSCommand: EvalDyTSIncrement},
	roachpb.Delete:         {EvalDyTSCommand: EvalDyTSDelete},
	roachpb.DeleteRange:    {EvalDyTSCommand: EvalDyTSDeleteRange},
	roachpb.Scan:           {EvalDyTSCommand: EvalDyTSScan},
	roachpb.ReverseScan:    {EvalDyTSCommand: EvalDyTSReverseScan},
	roachpb.EndTransaction: {EvalDyTSCommand: EvalDyTSEndTransaction},
}

func (r *Replica) executeDyTSCmd(
	ctx context.Context,
	raftCmdID storagebase.CmdIDKey,
	index int,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	maxKeys int64,
	args roachpb.Request,
	reply roachpb.Response) (EvalResult, *roachpb.Error) {

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return EvalResult{}, nil
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi : In executeDyTSCmd")
	}
	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if filter := r.store.cfg.TestingKnobs.TestingCommandFilter; filter != nil {
		filterArgs := storagebase.FilterArgs{Ctx: ctx, CmdID: raftCmdID, Index: index,
			Sid: r.store.StoreID(), Req: args, Hdr: h}
		if pErr := filter(filterArgs); pErr != nil {
			log.Infof(ctx, "test injecting error: %s", pErr)
			return EvalResult{}, pErr
		}
	}

	var err error
	var pd EvalResult

	if cmd, ok := DyTSCommands[args.Method()]; ok {
		cArgs := CommandArgs{
			Repl:   r,
			Header: h,
			// Some commands mutate their arguments, so give each invocation
			// its own copy (shallow to mimic earlier versions of this code
			// in which args were passed by value instead of pointer).
			Args:    args.ShallowCopy(),
			MaxKeys: maxKeys,
			Stats:   ms,
		}
		if log.V(2) {
			log.Infof(ctx, "Ravi : executing cmd %v with cArgs %v ", args.Method(), cArgs)
		}
		pd, err = cmd.EvalDyTSCommand(ctx, batch, cArgs, reply)
	} else {
		err = errors.Errorf("unrecognized command %s", args.Method())
	}

	if log.V(2) {
		log.Infof(ctx, "executed %s command %+v: %+v, err=%v", args.Method(), args, reply, err)
	}

	// Create a roachpb.Error by initializing txn from the request/response header.
	var pErr *roachpb.Error
	if err != nil {
		txn := reply.Header().Txn
		if txn == nil {
			txn = h.Txn
		}
		pErr = roachpb.NewErrorWithTxn(err, txn)
	}

	return pd, pErr

}

func EvalDyTSGet(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSGet")
	}
	args := cArgs.Args.(*roachpb.GetRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.GetResponse)

	// Places read locks collects already placed write locks and reads
	val, _, wslocks, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)
	if err != nil {
		if log.V(2) {
			log.Infof(ctx, "failed to get")
		}
		return EvalResult{}, err
	}
	reply.Value = val

	// check if the read is snapshot read

	// Update Transaction record with write locks
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSGet %v", each)
		}
	}
	if len(wslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
	}
	if err := pushUpdatesOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
		panic("failed to place soft  locks in txn Record")
	}

	return EvalResult{}, nil
}

func EvalDyTSPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	// places write lock and returns already placed read and write locks
	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSPut: begin")
	}
	args := cArgs.Args.(*roachpb.PutRequest)
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	rslocks, wslocks := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSPut %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSPut %v", each)
		}
	}

	if len(wslocks) != 0 || len(rslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	if err := pushUpdatesOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
		panic("failed to place locks in transaction record")
	}
	return EvalResult{}, nil
}

func EvalDyTSConditionalPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSConditionalPut: begin")
	}
	args := cArgs.Args.(*roachpb.ConditionalPutRequest)
	h := cArgs.Header

	expVal := args.ExpValue
	check := func(existVal *roachpb.Value) error {
		if log.V(2) {
			log.Infof(ctx, " EvalDyTSConditionalPut 3 : existVal %v", existVal)
		}
		if expValPresent, existValPresent := expVal != nil, existVal != nil; expValPresent && existValPresent {
			// Every type flows through here, so we can't use the typed getters.
			if !bytes.Equal(expVal.RawBytes, existVal.RawBytes) {
				return &roachpb.ConditionFailedError{
					ActualValue: existVal.ShallowClone(),
				}
			}
		} else if expValPresent != existValPresent {
			return &roachpb.ConditionFailedError{
				ActualValue: existVal.ShallowClone(),
			}
		}
		return nil
	}
	if log.V(2) {
		log.Infof(ctx, " EvalDyTSConditionalPut 1")
	}
	existVal, _, wslocks, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)
	if err != nil {
		if log.V(2) {
			log.Infof(ctx, " error in MVCCGet")
		}
		return EvalResult{}, err
	}
	if log.V(2) {
		log.Infof(ctx, " EvalDyTSConditionalPut 2")
	}
	if err = check(existVal); err != nil {
		engine.MVCCRemoveReadSoftLock(ctx, batch, *h.Txn, args.Span, cArgs.Repl.slockcache, false)
		return EvalResult{}, err
	}
	if log.V(2) {
		log.Infof(ctx, " EvalDyTSConditionalPut 4")
	}
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	// places write lock and returns already placed read and write locks
	rslocks, wslocks1 := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)

	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSConditionalPut %v", each)
		}
	}
	// push locks collected on read

	if len(wslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on EvalDyTSConditionalPut ")
		}
	}
	if err := pushUpdatesOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
		panic("failed to place soft  locks in txn Record")
	}

	for _, each := range wslocks1 {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSConditionalPut %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSConditionalPut %v", each)
		}
	}
	// push locks collected on write

	if len(wslocks1) != 0 || len(rslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on EvalDyTSConditionalPut ")
		}
	}
	if err := pushUpdatesOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks1); err != nil {
		panic("failed to place locks in transaction record")
	}
	return EvalResult{}, nil

}

func EvalDyTSInitPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSInitPut: begin")
	}
	args := cArgs.Args.(*roachpb.InitPutRequest)
	h := cArgs.Header
	var errInitPutValueMatchesExisting = errors.New("the value matched the existing value")
	value := args.Value
	check := func(existVal *roachpb.Value) error {
		if existVal != nil {
			if !bytes.Equal(value.RawBytes, existVal.RawBytes) {
				return &roachpb.ConditionFailedError{
					ActualValue: existVal.ShallowClone(),
				}
			}
			// The existing value matches the supplied value; return an error
			// to prevent rewriting the value.
			return errInitPutValueMatchesExisting
		}
		return nil
	}

	existVal, _, _, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, nil, false)
	if err != nil {
		if log.V(2) {
			log.Infof(ctx, " error in MVCCGet")
		}
		return EvalResult{}, err
	}
	if err = check(existVal); err != nil {
		return EvalResult{}, err
	}
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	// places write lock and returns already placed read and write locks
	rslocks, wslocks := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSInitPut %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSInitPut %v", each)
		}
	}

	if len(wslocks) != 0 || len(rslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	if err := pushUpdatesOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
		panic("failed to place locks in transaction record")
	}
	return EvalResult{}, nil

}

// Since this method reads the value an then writes it we place both read and write locks
func EvalDyTSIncrement(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSIncrement: begin")
	}
	args := cArgs.Args.(*roachpb.IncrementRequest)
	inc := args.Increment
	h := cArgs.Header
	reply := resp.(*roachpb.IncrementResponse)
	var int64Val int64
	check := func(value *roachpb.Value) error {
		if value != nil {
			var err error
			if int64Val, err = value.GetInt(); err != nil {
				return errors.Errorf("key %q does not contain an integer value", args.Key)
			}
		}

		// Check for overflow and underflow.
		if engine.WillOverflow(int64Val, inc) {
			return errors.Errorf("key %s with value %d incremented by %d results in overflow", args.Key, int64Val, inc)
		}

		int64Val = int64Val + inc

		return nil
	}
	extvalue, _, wslocks, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)
	if err != nil {
		if log.V(2) {
			log.Infof(ctx, " error in MVCCGet")
		}
		return EvalResult{}, err
	}
	if err = check(extvalue); err != nil {
		engine.MVCCRemoveReadSoftLock(ctx, batch, *h.Txn, args.Span, cArgs.Repl.slockcache, false)
		return EvalResult{}, err
	}
	reply.NewValue = int64Val

	var req roachpb.RequestUnion
	req.MustSetInner(args)
	// places write lock and returns already placed read and write locks
	rslocks, wslocks1 := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)

	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSConditionalPut %v", each)
		}
	}
	// push locks collected on read

	if len(wslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Cput ")
		}
	}
	if err := pushUpdatesOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
		panic("failed to place soft  locks in txn Record")
	}

	for _, each := range wslocks1 {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSIncrement %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSIncrement %v", each)
		}
	}
	// push locks collected on write

	if len(wslocks1) != 0 || len(rslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	if err := pushUpdatesOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks1); err != nil {
		panic("failed to place locks in transaction record")
	}
	return EvalResult{}, nil

}

func EvalDyTSDelete(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSDelete: begin")
	}
	args := cArgs.Args.(*roachpb.DeleteRequest)
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	// places write lock and returns already placed read and write locks
	rslocks, wslocks := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSDelete %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSDelete %v", each)
		}
	}

	if len(wslocks) != 0 || len(rslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	if err := pushUpdatesOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
		panic("failed to place locks in transaction record")
	}
	return EvalResult{}, nil

}

func EvalDyTSDeleteRange(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSDelete: begin")
	}
	args := cArgs.Args.(*roachpb.DeleteRangeRequest)
	var req roachpb.RequestUnion
	req.MustSetInner(args)

	keys := engine.MVCCgetKeysUsingIter(ctx, args.Key, args.EndKey, batch, *cArgs.Header.Txn, false)

	var wslocks []roachpb.WriteSoftLock
	var rslocks []roachpb.ReadSoftLock

	for _, eachkey := range keys {
		rslockstmp, wslockstmp := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, eachkey, req, cArgs.Repl.slockcache)
		wslocks = append(wslocks, wslockstmp...)
		rslocks = append(rslocks, rslockstmp...)
	}

	if len(wslocks) != 0 || len(rslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	if err := pushUpdatesOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
		panic("failed to place locks in transaction record")
	}
	return EvalResult{}, nil

}

func EvalDyTSScan(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSScan")
	}
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	rows, resumeSpan, _, wslocks, _ := engine.MVCCScan(ctx, batch, args.Key, args.EndKey,
		cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)

	reply.NumKeys = int64(len(rows))
	reply.ResumeSpan = resumeSpan
	reply.Rows = rows

	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on Scan %v", each)
		}
	}

	if len(wslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Scan ")
		}
	}
	if err := pushUpdatesOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
		panic("failed to place soft  locks in txn Record")
	}
	return EvalResult{}, nil
}

func EvalDyTSReverseScan(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSReverseScan")
	}
	args := cArgs.Args.(*roachpb.ReverseScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ReverseScanResponse)

	rows, resumeSpan, _, wslocks, _ := engine.MVCCReverseScan(ctx, batch, args.Key, args.EndKey,
		cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)

	reply.NumKeys = int64(len(rows))
	reply.ResumeSpan = resumeSpan
	reply.Rows = rows

	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on ReverseScan %v", each)
		}
	}

	if len(wslocks) != 0 {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
	}
	if err := pushUpdatesOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
		panic("failed to place soft  locks in txn Record")
	}
	return EvalResult{}, nil
}

func EvalDyTSEndTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSEndTransaction")
	}
	var txnrcd roachpb.Transaction
	var err error
	args := cArgs.Args.(*roachpb.EndTransactionRequest)
	reply := resp.(*roachpb.EndTransactionResponse)

	if args.Commit {
		if txnrcd, err = executeValidator(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header); err != nil {
			return EvalResult{}, err
		}
	} else {
		if txnrcd, err = handleAbort(ctx, batch, cArgs.Header); err != nil {
			return EvalResult{}, err
		}
	}

	reply.Txn = &txnrcd
	return EvalResult{}, nil
}

func handleAbort(
	ctx context.Context,
	batch engine.ReadWriter,
	h roachpb.Header,
) (roachpb.Transaction, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In handleAbort")
	}
	var txnRecord roachpb.Transaction
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)
	ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord)
	if err != nil || !ok {
		if log.V(2) {
			log.Infof(ctx, "Ravi : Coundnt find the transaction record in this range")
		}

		return txnRecord, err
	}
	txnRecord.Status = roachpb.ABORTED

	return txnRecord, nil
}

func transactionRecordExists(
	ctx context.Context,
	batch engine.ReadWriter,
	tkey roachpb.Key,
	txnid uuid.UUID,
) bool {
	key := keys.TransactionKey(tkey, txnid)

	var txnRecord roachpb.Transaction

	ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord)
	if err != nil || !ok {
		if log.V(2) {
			log.Infof(ctx, "Ravi : Coundnt find the transaction record in this range")
		}

		return false
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi : found the transaction record in the same range")
	}
	return true
}

func updateTransactionRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	rArgs RpcArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In updateTransactionrecord")
	}

	if !transactionRecordExists(ctx, batch, h.Txn.Key, *h.Txn.ID) {
		// Transaction is in different range so making a RPC call
		return sendUpdateTransactionRecordRPC(ctx, s, h, rArgs)
	}

	return updateLocalTransactionRecord(ctx, batch, txncache, h, rArgs)

}

func updateLocalTransactionRecord(
	ctx context.Context,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	rArgs RpcArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In updateLocalTransactionRecord")
	}
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)
	if log.V(2) {
		log.Infof(ctx, "Ravi : In getting access to tnx Id %v", *h.Txn.ID)
	}
	if !txncache.getAccess(key, false /*wait until you get lock*/) {

		// should never be this case
	}
	defer txncache.releaseAccess(key)
	if log.V(2) {
		log.Infof(ctx, "Ravi : got access to tnx Id %v", *h.Txn.ID)
	}
	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return err
	} else if ok {
		// Update the Transaction record
		if log.V(2) {
			log.Infof(ctx, "Transaction record before updating %v with lb %v, ub %v", *txnRecord.ID, txnRecord.DynamicTimestampLowerBound, txnRecord.DynamicTimestampUpperBound)
		}
		txnRecord.DynamicTimestampLowerBound.Forward(rArgs.lowerbound)
		txnRecord.DynamicTimestampUpperBound.Backward(rArgs.upperbound)

		if log.V(2) {
			log.Infof(ctx, "Updating Transaction %v with lb %v, ub %v", *txnRecord.ID, txnRecord.DynamicTimestampLowerBound, txnRecord.DynamicTimestampUpperBound)
		}

		for _, txn := range rArgs.commitAQ {
			if log.V(2) {
				log.Infof(ctx, "Ravi : updating CommitAfterThem with %v", txn)
			}
			txnRecord.CommitAfterThem = append(txnRecord.CommitAfterThem, txn)
		}
		for _, txn := range rArgs.commitBQ {
			if log.V(2) {
				log.Infof(ctx, "Ravi : updating CommitBeforeThem with %v", txn)
			}
			txnRecord.CommitBeforeThem = append(txnRecord.CommitBeforeThem, txn)
		}
		// Save the updated Transaction record
		return engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)
	}

	return nil
}

func sendUpdateTransactionRecordRPC(
	ctx context.Context,
	s *Store,
	h roachpb.Header,
	rArgs RpcArgs,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : sendUpdateTransactionRecordRPC")
	}

	updatetxnReq := &roachpb.UpdateTransactionRecordRequest{
		Span: roachpb.Span{
			Key: h.Txn.Key,
		},
		Tmeta:            h.Txn.TxnMeta,
		LowerBound:       rArgs.lowerbound,
		UpperBound:       rArgs.upperbound,
		CommitAfterThem:  rArgs.commitAQ,
		CommitBeforeThem: rArgs.commitBQ,
	}

	b := &client.Batch{}
	//b.Header = cArgs.Header
	//b.Header.Timestamp = hlc.ZeroTimestamp
	if log.V(2) {
		log.Infof(ctx, "Ravi : updatetxnReq %v", updatetxnReq)
	}
	b.AddRawRequest(updatetxnReq)

	if err := s.db.Run(ctx, b); err != nil {
		_ = b.MustPErr()
		return err
	}

	br := b.RawResponse()
	for _, res := range br.Responses {
		r := res.GetInner().(*roachpb.UpdateTransactionRecordResponse)
		if log.V(2) {
			log.Infof(ctx, "updateTransactionrecord recieved response : %v", r)
		}
	}

	return nil
}

func pushUpdatesOnReadToTxnRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	r *Replica,
	span roachpb.Span,
	wslocks []roachpb.WriteSoftLock,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi :pushUpdatesOnReadToTxnRecord ")
	}

	var rARgs RpcArgs
	// Modify its Lower bound based on last committed write time stamp
	lowerbound := r.applyDyTSCache(span, false /*write*/)
	if log.V(2) {
		log.Infof(ctx, "In Timstampcache : retrived last commited write %v on span %v ", lowerbound, span)
	}
	// Forwards are used just to assign the values
	if h.Timestamp.Less(lowerbound) {
		rARgs.lowerbound.Forward(h.Timestamp)
		rARgs.upperbound.Forward(h.Timestamp)
	} else {
		rARgs.lowerbound.Forward(lowerbound)
		rARgs.upperbound.Forward(hlc.MaxTimestamp)

	}

	// Place txns of all the write locks
	for _, lock := range wslocks {
		rARgs.commitBQ = append(rARgs.commitBQ, lock.TransactionMeta)
	}

	// Update transaction
	if err := updateTransactionRecord(ctx, s, batch, txncache, h, rARgs); err != nil {
		panic("failed to update transaction")
	}

	return nil
}

func pushUpdatesOnWriteToTxnRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	r *Replica,
	span roachpb.Span,
	rslocks []roachpb.ReadSoftLock,
	wslocks []roachpb.WriteSoftLock,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :pushUpdatesOnWriteToTxnRecord ")
	}
	var rARgs RpcArgs
	// Modify its Lower bound based on last committed write time stamp
	lowerbound := r.applyDyTSCache(span, true /*read*/)
	if log.V(2) {
		log.Infof(ctx, "In Timstampcache : retrived last commited read %von span %v ", lowerbound, span)
	}
	// Forwards are used just to assign the values
	rARgs.lowerbound.Forward(lowerbound)
	rARgs.upperbound.Forward(hlc.MaxTimestamp)

	// Place txns of all the write locks
	for _, lock := range wslocks {
		rARgs.commitAQ = append(rARgs.commitAQ, lock.TransactionMeta)
	}

	// Place txns of all the read locks
	for _, lock := range rslocks {
		rARgs.commitAQ = append(rARgs.commitAQ, lock.TransactionMeta)
	}
	// Update transaction
	if err := updateTransactionRecord(ctx, s, batch, txncache, h, rARgs); err != nil {
		panic("failed to update transaction")
	}
	return nil
}

func manageCommitBeforeQueue(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :manageCommitBeforeQueue ")
	}

	for _, txn := range mytxnRecord.CommitBeforeThem {
		if err := validateCommitBefore(ctx, s, batch, txncache, mytxnRecord, txn); err != nil {
			return err
		}
	}
	return nil
}

// validates the commit before case
func validateCommitBefore(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
	othertxn enginepb.TxnMeta,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In validateCommitBefore")
	}
	upperBound := hlc.MaxTimestamp
	var err error
	if !transactionRecordExists(ctx, batch, othertxn.Key, *othertxn.ID) {
		// Transaction is in different range so making a RPC call
		if upperBound, err = sendValidateCommitBeforeRPC(ctx, s, mytxnRecord.DynamicTimestampLowerBound, othertxn); err != nil {
			return err
		}
	}

	if upperBound, err = executelocalValidateCommitBefore(ctx, s, batch, txncache, mytxnRecord.DynamicTimestampUpperBound, othertxn); err != nil {
		return err
	}

	mytxnRecord.DynamicTimestampUpperBound.Backward(upperBound)

	return nil
}

func sendValidateCommitBeforeRPC(
	ctx context.Context,
	s *Store,
	upperBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendValidateCommitBeforeRPC")
	}

	commitbeforereq := &roachpb.ValidateCommitBeforeRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Tmeta:      txn,
		UpperBound: &upperBound,
	}

	b := &client.Batch{}
	//b.Header = cArgs.Header
	//b.Header.Timestamp = hlc.ZeroTimestamp
	if log.V(2) {
		log.Infof(ctx, "Ravi : commitbeforereq %v", commitbeforereq)
	}
	b.AddRawRequest(commitbeforereq)

	if err := s.db.Run(ctx, b); err != nil {
		_ = b.MustPErr()
		return upperBound, err
	}

	br := b.RawResponse()
	for _, res := range br.Responses {
		ub := res.GetInner().(*roachpb.ValidateCommitBeforeResponse).UpperBound
		if log.V(2) {
			log.Infof(ctx, "updateTransactionrecord recieved response : %v", *ub)
		}
		upperBound = *ub
	}

	return upperBound, nil
}

func executelocalValidateCommitBefore(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	upperBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi :VCB In executelocalValidateCommitBefore")
	}
	key := keys.TransactionKey(txn.Key, *txn.ID)

	if log.V(2) {
		log.Infof(ctx, "Ravi :VCB In getting access to tnx Id %v", *txn.ID)
	}
	if !txncache.getAccess(key, true /*timed wait*/) {
		return upperBound, roachpb.NewTransactionAbortedError()
	}
	defer txncache.releaseAccess(key)
	if log.V(2) {
		log.Infof(ctx, "Ravi : got access to tnx Id %v", *txn.ID)
	}
	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return hlc.MaxTimestamp, err
	} else if ok {
		// Update the Transaction record
		switch txnRecord.Status {
		case roachpb.ABORTED:
		case roachpb.PENDING:
			if upperBound.Equal(hlc.MaxTimestamp) {
				upperBound.Backward(txnRecord.DynamicTimestampLowerBound)
			} else {
				txnRecord.DynamicTimestampLowerBound.Forward(upperBound)
			}
		case roachpb.COMMITTED:
			upperBound.Backward(txnRecord.DynamicTimestampLowerBound)
		}

		// Save the updated Transaction record
		if err := engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord); err != nil {
			return upperBound, err
		}
	}
	return upperBound, nil
}

func validateCommitAfter(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
	othertxn enginepb.TxnMeta,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In validateCommitAfter")
	}
	lowerBound := hlc.MinTimestamp
	var err error
	if !transactionRecordExists(ctx, batch, othertxn.Key, *othertxn.ID) {
		// Transaction is in different range so making a RPC call
		lowerBound, err = sendValidateCommitAfterRPC(ctx, s, mytxnRecord.DynamicTimestampLowerBound, othertxn)
	}

	lowerBound, err = executelocalValidateCommitAfter(ctx, s, batch, txncache, mytxnRecord.DynamicTimestampLowerBound, othertxn)

	mytxnRecord.DynamicTimestampLowerBound.Forward(lowerBound)

	return err
}

func sendValidateCommitAfterRPC(
	ctx context.Context,
	s *Store,
	lowerBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendValidateCommitAfterRPC")
	}
	commitafterreq := &roachpb.ValidateCommitAfterRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Tmeta:      txn,
		LowerBound: &lowerBound,
	}

	b := &client.Batch{}
	//b.Header = cArgs.Header
	//b.Header.Timestamp = hlc.ZeroTimestamp
	if log.V(2) {
		log.Infof(ctx, "Ravi : commitafterreq %v", commitafterreq)
	}
	b.AddRawRequest(commitafterreq)

	if err := s.db.Run(ctx, b); err != nil {
		_ = b.MustPErr()
	}

	br := b.RawResponse()
	for _, res := range br.Responses {
		lb := res.GetInner().(*roachpb.ValidateCommitAfterResponse).LowerBound
		if log.V(2) {
			log.Infof(ctx, "updateTransactionrecord recieved response : %v", *lb)
		}
		lowerBound = *lb
	}

	return lowerBound, nil
}

func executelocalValidateCommitAfter(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	lowerBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In executelocalValidateCommitAfter")
	}
	key := keys.TransactionKey(txn.Key, *txn.ID)
	if log.V(2) {
		log.Infof(ctx, "Ravi : VCA In getting access to tnx Id %v", *txn.ID)
	}
	if !txncache.getAccess(key, true /*timed wait*/) {
		return lowerBound, roachpb.NewTransactionAbortedError()
	}
	defer txncache.releaseAccess(key)

	if log.V(2) {
		log.Infof(ctx, "Ravi :VCA  got access to tnx Id %v", *txn.ID)
	}
	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return hlc.ZeroTimestamp, err
	} else if !ok {
		// should never be this case
		return hlc.ZeroTimestamp, roachpb.NewTransactionStatusError("does not exist")
	}

	switch txnRecord.Status {
	case roachpb.ABORTED:
	case roachpb.PENDING:
		if txnRecord.DynamicTimestampUpperBound.Equal(hlc.MaxTimestamp) {
			txnRecord.DynamicTimestampUpperBound.Backward(lowerBound)
		} else {
			lowerBound.Forward(txnRecord.DynamicTimestampUpperBound)
		}
	case roachpb.COMMITTED:
		lowerBound.Forward(txnRecord.DynamicTimestampUpperBound)
	}

	// Save the updated Transaction record
	err := engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)

	return lowerBound, err
}

func manageCommitAfterQueue(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :manageCommitAfterQueue ")
	}

	for _, txn := range mytxnRecord.CommitAfterThem {
		if err := validateCommitAfter(ctx, s, batch, txncache, mytxnRecord, txn); err != nil {
			return err
		}

	}
	return nil
}

func isTSIntact(
	ctx context.Context,
	lowerBound hlc.Timestamp,
	upperBound hlc.Timestamp,
) bool {
	if log.V(2) {
		log.Infof(ctx, "Ravi :isTSIntact lower bound %v , upper bound %v ", lowerBound, upperBound)
	}
	//if lowerBound.Less(upperBound) || lowerBound.Equal(upperBound) {
	if lowerBound.Less(upperBound) {
		return true
	}
	return false
}

func pickCommitTimeStamp(
	lowerBound hlc.Timestamp,
	upperBound hlc.Timestamp,
) hlc.Timestamp {

	/*if upperBound == lowerBound {
		return lowerBound
	}*/

	return lowerBound.Next()

}

func makeDecision(
	ctx context.Context,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In makeDecision")
	}
	if !isTSIntact(ctx, mytxnRecord.DynamicTimestampLowerBound, mytxnRecord.DynamicTimestampUpperBound) {
		mytxnRecord.Status = roachpb.ABORTED

	} else {
		commitTimestamp := pickCommitTimeStamp(mytxnRecord.DynamicTimestampLowerBound, mytxnRecord.DynamicTimestampUpperBound)
		mytxnRecord.OrigTimestamp = commitTimestamp
		mytxnRecord.DynamicTimestampLowerBound = commitTimestamp
		mytxnRecord.DynamicTimestampUpperBound = commitTimestamp
		mytxnRecord.Status = roachpb.COMMITTED
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi : In decision id %v, decision %v, timestamp %v", *mytxnRecord.ID, mytxnRecord.Status, mytxnRecord.OrigTimestamp)
	}
	return nil
}

func markAsAbort(
	ctx context.Context,
	batch engine.ReadWriter,
	key roachpb.Key,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In markAsAbort")
	}
	mytxnRecord.Status = roachpb.ABORTED

	if err := engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, mytxnRecord); err != nil {
		return err
	}
	return nil
}
func executeLocalValidator(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
) (roachpb.Transaction, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In executeLocalValidator")
	}
	var txnRecord roachpb.Transaction
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)
	if log.V(2) {
		log.Infof(ctx, "Ravi : In getting access to tnx Id %v", *h.Txn.ID)
	}

	if !txncache.getAccess(key, false /*wait until you get*/) {
		if err := markAsAbort(ctx, batch, key, &txnRecord); err != nil {
			return txnRecord, err
		}
		return txnRecord, nil
	}
	defer txncache.releaseAccess(key)

	if log.V(2) {
		log.Infof(ctx, "Ravi : Got access to tnx Id %v", *h.Txn.ID)
	}

	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return txnRecord, err
	} else if ok {
		if err := manageCommitBeforeQueue(ctx, s, batch, txncache, &txnRecord); err != nil {
			if err = markAsAbort(ctx, batch, key, &txnRecord); err != nil {
				return txnRecord, err
			}
			return txnRecord, nil
		}
		if err := manageCommitAfterQueue(ctx, s, batch, txncache, &txnRecord); err != nil {
			if err = markAsAbort(ctx, batch, key, &txnRecord); err != nil {
				return txnRecord, err
			}
			return txnRecord, nil
		}
		if err := makeDecision(ctx, &txnRecord); err != nil {
			return txnRecord, err
		}

	}
	if err := engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord); err != nil {
		return txnRecord, err
	}

	return txnRecord, nil
}

func sendexecuteValidatorRPC(ctx context.Context,
	s *Store,
	h roachpb.Header,
) (roachpb.Transaction, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendexecuteValidatorRPC")
	}
	var txnRecord roachpb.Transaction
	return txnRecord, nil
}

func executeValidator(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
) (roachpb.Transaction, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In executeValidator")
	}

	if !transactionRecordExists(ctx, batch, h.Txn.Key, *h.Txn.ID) {
		// Transaction is in different range so making a RPC call
		return sendexecuteValidatorRPC(ctx, s, h)
	}

	return executeLocalValidator(ctx, s, batch, txncache, h)
}
