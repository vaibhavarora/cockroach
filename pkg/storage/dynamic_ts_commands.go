package storage

import (
	//"fmt"
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
	val, _, wslocks, _ := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)
	reply.Value = val

	// check if the read is snapshot read

	// Update Transaction record with write locks
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSGet %v", each)
		}
	}
	if len(wslocks) != 0 {
		if err := pushSoftLocksOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
			panic("failed to place soft  locks in txn Record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
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
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
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
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	// places write lock and returns already placed read and write locks
	rslocks, wslocks := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSConditionalPut %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSConditionalPut %v", each)
		}
	}

	if len(wslocks) != 0 || len(rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
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
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
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
	// Place read lock
	wslocks := engine.MVCCPlaceReadSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, false, cArgs.Repl.slockcache)
	//Place write lock
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	// places write lock and returns already placed read and write locks
	rslocks, wslockstmp := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)
	wslocks = append(wslocks, wslockstmp...)
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSIncrement %v", each)
		}
	}
	for _, each := range rslocks {
		if log.V(2) {
			log.Infof(ctx, "Read locks acqurired on EvalDyTSIncrement %v", each)
		}
	}

	if len(wslocks) != 0 || len(rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
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
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
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
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, rslocks, wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
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
		if err := pushSoftLocksOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
			panic("failed to place soft  locks in txn Record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
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
		if err := pushSoftLocksOnReadToTxnRecord(ctx, cArgs.Repl.store, batch, cArgs.Repl.txnlockcache, cArgs.Header, cArgs.Repl, args.Span, wslocks); err != nil {
			panic("failed to place soft  locks in txn Record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
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
		txnrcd.Status = roachpb.ABORTED
	}

	reply.Txn = &txnrcd
	return EvalResult{}, nil
}

func transactionRecordExists(
	ctx context.Context,
	s *Store,
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

	if !transactionRecordExists(ctx, s, batch, h.Txn.Key, *h.Txn.ID) {
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

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return err
	} else if ok {
		// Update the Transaction record
		txnRecord.DynamicTimestampLowerBound.Forward(rArgs.lowerbound)
		txnRecord.DynamicTimestampUpperBound.Backward(rArgs.upperbound)
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
	} else {

		br := b.RawResponse()
		for _, res := range br.Responses {
			r := res.GetInner().(*roachpb.UpdateTransactionRecordResponse)
			if log.V(2) {
				log.Infof(ctx, "updateTransactionrecord recieved response : %v", r)
			}
		}
	}
	return nil
}

func pushSoftLocksOnReadToTxnRecord(
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
		log.Infof(ctx, "Ravi :pushSoftLocksOnReadToTxnRecord ")
	}

	var rARgs RpcArgs
	// Modify its Lower bound based on last committed write time stamp
	lowerbound := r.applyDyTSCache(span, true /*read*/)
	if h.Timestamp.Less(lowerbound) {
		rARgs.lowerbound = h.Timestamp
		rARgs.upperbound = h.Timestamp
	} else {
		rARgs.lowerbound = lowerbound
		rARgs.upperbound = hlc.MaxTimestamp
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

func pushSoftLocksOnWriteToTxnRecord(
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
		log.Infof(ctx, "Ravi :pushSoftLocksOnWriteToTxnRecord ")
	}
	var rARgs RpcArgs
	// Modify its Lower bound based on last committed write time stamp
	// Temporarily
	rARgs.lowerbound = r.applyDyTSCache(span, false /* write*/)
	rARgs.upperbound = hlc.MaxTimestamp

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
	var upperBound hlc.Timestamp
	var err error
	if !transactionRecordExists(ctx, s, batch, othertxn.Key, *othertxn.ID) {
		// Transaction is in different range so making a RPC call
		upperBound, err = sendValidateCommitBeforeRPC(ctx, s, mytxnRecord.DynamicTimestampLowerBound, othertxn)
	}

	upperBound, err = executelocalValidateCommitBefore(ctx, s, batch, txncache, mytxnRecord.DynamicTimestampLowerBound, othertxn)

	mytxnRecord.DynamicTimestampUpperBound.Backward(upperBound)

	return err
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
	} else {

		br := b.RawResponse()
		for _, res := range br.Responses {
			ub := res.GetInner().(*roachpb.ValidateCommitBeforeResponse).UpperBound
			if log.V(2) {
				log.Infof(ctx, "updateTransactionrecord recieved response : %v", *ub)
			}
			upperBound = *ub
		}
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
		log.Infof(ctx, "Ravi : In executelocalValidateCommitBefore")
	}
	key := keys.TransactionKey(txn.Key, *txn.ID)

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return hlc.MaxTimestamp, err
	} else if ok {
		// Update the Transaction record
		if txnRecord.Status == roachpb.PENDING && !upperBound.Equal(hlc.MaxTimestamp) {
			txnRecord.DynamicTimestampLowerBound.Forward(upperBound)
		} else {
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
	var lowerBound hlc.Timestamp
	var err error
	if !transactionRecordExists(ctx, s, batch, othertxn.Key, *othertxn.ID) {
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
	} else {

		br := b.RawResponse()
		for _, res := range br.Responses {
			lb := res.GetInner().(*roachpb.ValidateCommitAfterResponse).LowerBound
			if log.V(2) {
				log.Infof(ctx, "updateTransactionrecord recieved response : %v", *lb)
			}
			lowerBound = *lb
		}
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

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return hlc.ZeroTimestamp, err
	} else if !ok {
		// should never be this case
		return hlc.ZeroTimestamp, roachpb.NewTransactionStatusError("does not exist")
	}
	// Update the Transaction record
	if txnRecord.Status == roachpb.PENDING && !txnRecord.DynamicTimestampLowerBound.Equal(hlc.MaxTimestamp) {
		txnRecord.DynamicTimestampUpperBound.Backward(lowerBound)
	} else {
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
	if lowerBound.Less(upperBound) || lowerBound.Equal(upperBound) {
		return true
	}
	return false
}

func pickCommitTimeStamp(
	lowerBound hlc.Timestamp,
	upperBound hlc.Timestamp,
) hlc.Timestamp {

	if upperBound == lowerBound {
		return lowerBound
	}

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
		log.Infof(ctx, "Ravi : In decision %v", mytxnRecord.Status)
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
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return txnRecord, err
	} else if ok {
		if err := manageCommitBeforeQueue(ctx, s, batch, txncache, &txnRecord); err != nil {
			return txnRecord, err
		}
		if err := manageCommitAfterQueue(ctx, s, batch, txncache, &txnRecord); err != nil {
			return txnRecord, err
		}
		if err := makeDecision(ctx, &txnRecord); err != nil {
			return txnRecord, err
		}

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

	if !transactionRecordExists(ctx, s, batch, h.Txn.Key, *h.Txn.ID) {
		// Transaction is in different range so making a RPC call
		return sendexecuteValidatorRPC(ctx, s, h)
	}

	return executeLocalValidator(ctx, s, batch, txncache, h)
}
