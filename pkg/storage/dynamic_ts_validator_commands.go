package storage

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type DyTSValidatorCommand struct {
	//EvalDyTSCommand func(context.Context, roachpb.Header, roachpb.Request)
	EvalDyTSValidatorCommand func(context.Context, engine.ReadWriter, CommandArgs, roachpb.Response) (EvalResult, error)
}

var DyTSValidatorCommands = map[roachpb.Method]DyTSValidatorCommand{
	roachpb.UpdateTxnRecord:      {EvalDyTSValidatorCommand: EvalDyTSUpdateTransactionRecord},
	roachpb.DyTsEndTransaction:   {EvalDyTSValidatorCommand: EvalDyTSValidatorEndTransaction},
	roachpb.ValidateCommitAfter:  {EvalDyTSValidatorCommand: EvalDyTSValidateCommitAfter},
	roachpb.ValidateCommitBefore: {EvalDyTSValidatorCommand: EvalDyTSValidateCommitBefore},
	roachpb.GcWriteSoftLock:      {EvalDyTSValidatorCommand: EvalDyTSGcWriteSoftLock},
	roachpb.GcReadSoftLock:       {EvalDyTSValidatorCommand: EvalDyTSGcReadSoftLock},
	roachpb.ResolveWriteSlock:    {EvalDyTSValidatorCommand: EvalDyTSResolveWriteSoftLock},
}

func (r *Replica) executeDyTSValidatorCmd(
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
		log.Infof(ctx, "Ravi : In executeDyTSValidatorCmd")
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

	if cmd, ok := DyTSValidatorCommands[args.Method()]; ok {
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
		pd, err = cmd.EvalDyTSValidatorCommand(ctx, batch, cArgs, reply)
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

func EvalDyTSUpdateTransactionRecord(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSUpdateTransactionRecord")
	}
	args := cArgs.Args.(*roachpb.UpdateTransactionRecordRequest)

	key := keys.TransactionKey(args.Tmeta.Key, *args.Tmeta.ID)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSUpdateTransactionRecord : coudnt find transaction record in this range")
		}
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	if log.V(2) {
		log.Infof(ctx, "EvalDyTSUpdateTransactionRecord : found transaction record in this range")
	}
	// Update the Transaction record
	txnRecord.DynamicTimestampLowerBound.Forward(args.LowerBound)
	txnRecord.DynamicTimestampUpperBound.Backward(args.UpperBound)
	for _, txn := range args.CommitAfterThem {
		txnRecord.CommitAfterThem = append(txnRecord.CommitAfterThem, txn)
	}
	for _, txn := range args.CommitBeforeThem {
		txnRecord.CommitBeforeThem = append(txnRecord.CommitBeforeThem, txn)
	}
	// Save the updated Transaction record
	return EvalResult{}, engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)

}

func EvalDyTSValidatorEndTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSValidatorEndTransaction ")
	}
	args := cArgs.Args.(*roachpb.DyTSEndTransactionRequest)
	reply := resp.(*roachpb.DyTSEndTransactionResponse)
	if log.V(2) {
		log.Infof(ctx, "args %v", args)
	}
	if log.V(2) {
		log.Infof(ctx, "key %v", args.Tmeta.Key)
	}
	if log.V(2) {
		log.Infof(ctx, "id %v", args.Tmeta.ID)
	}

	if err := verifyTransaction(cArgs.Header, args); err != nil {
		return EvalResult{}, err
	}

	key := keys.TransactionKey(args.Tmeta.Key, *args.Tmeta.ID)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSValidatorEndTransaction : coudnt find transaction record in this range")
		}
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	if log.V(2) {
		log.Infof(ctx, "EvalDyTSValidatorEndTransaction : found transaction record in this range")
	}

	if args.Commit {
		txnRecord.DynamicTimestampLowerBound = *args.Deadline
		txnRecord.DynamicTimestampUpperBound = *args.Deadline
		txnRecord.OrigTimestamp = *args.Deadline
		txnRecord.Status = roachpb.COMMITTED
	} else {
		txnRecord.Status = roachpb.ABORTED
	}
	// Save the updated Transaction record
	if err := engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord); err != nil {
		return EvalResult{}, err
	}
	reply.Txn = &txnRecord

	var pd EvalResult
	if args.Commit {
		// Resolve Local Soft locks
		externalReadSpans, externalReverseReadSpans, externalWriteSpans := cArgs.Repl.resolveLocalSoftLocks(ctx, batch, cArgs.Stats, *args, &txnRecord)
		// Add external write soft locks to resolve asyncronous processing
		pd.Local.resolvewslocks = &externalWriteSpans
		// add external read spans to garbage collect asncronously
		pd.Local.gcrslocks = &externalReadSpans
		// add external reverse read spans to garbage collect asncronously
		pd.Local.gcrevrslocks = &externalReverseReadSpans
	} else {
		//  garbage collect Local Soft locks
		externalReadSpans, externalReverseReadSpans, externalWriteSpans := cArgs.Repl.GCLocalSoftLocks(ctx, batch, *args, &txnRecord)
		//Add external write soft locks to GC asyncronous processing
		pd.Local.gcwslocks = &externalWriteSpans
		pd.Local.gcrslocks = &externalReadSpans
		pd.Local.gcrevrslocks = &externalReverseReadSpans
	}
	pd.Local.txnrecord = &txnRecord

	// Run triggers if successfully committed.
	var pdr EvalResult
	if txnRecord.Status == roachpb.COMMITTED {
		var err error
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSValidatorEndTransaction : calling commit trigger")
		}
		if pdr, err = cArgs.Repl.runCommitTrigger(ctx, batch.(engine.Batch), cArgs.Stats, args.InternalCommitTrigger, &txnRecord); err != nil {
			return EvalResult{}, NewReplicaCorruptionError(err)
		}
	}

	if err := pdr.MergeAndDestroy(pd); err != nil {
		return EvalResult{}, err
	}

	return pdr, nil
}

func EvalDyTSValidateCommitAfter(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSValidateCommitAfter")
	}
	args := cArgs.Args.(*roachpb.ValidateCommitAfterRequest)

	key := keys.TransactionKey(args.Tmeta.Key, *args.Tmeta.ID)

	if !cArgs.Repl.txnlockcache.getAccess(key, true /*timed wait*/) {
		return EvalResult{}, roachpb.NewTransactionAbortedError()
	}
	defer cArgs.Repl.txnlockcache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSValidateCommitAfter : couldnt find transaction record in this range")
		}
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	if log.V(2) {
		log.Infof(ctx, "EvalDyTSValidateCommitAfter : found transaction record in this range")
	}
	lowerBound := *args.LowerBound
	// Update the Transaction record
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
	if err := engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord); err != nil {
		return EvalResult{}, err
	}

	reply := resp.(*roachpb.ValidateCommitAfterResponse)
	reply.LowerBound = &lowerBound

	return EvalResult{}, nil

}

func EvalDyTSValidateCommitBefore(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSValidateCommitBefore")
	}
	args := cArgs.Args.(*roachpb.ValidateCommitBeforeRequest)

	key := keys.TransactionKey(args.Tmeta.Key, *args.Tmeta.ID)

	if !cArgs.Repl.txnlockcache.getAccess(key, true /*timed wait*/) {
		return EvalResult{}, roachpb.NewTransactionAbortedError()
	}
	defer cArgs.Repl.txnlockcache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSValidateCommitBefore : coudnt find transaction record in this range")
		}
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	if log.V(2) {
		log.Infof(ctx, "EvalDyTSValidateCommitBefore : found transaction record in this range")
	}
	// Update the Transaction record
	upperBound := *args.UpperBound

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

	reply := resp.(*roachpb.ValidateCommitBeforeResponse)
	reply.UpperBound = &upperBound

	// Save the updated Transaction record
	return EvalResult{}, engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)

}

func EvalDyTSGcWriteSoftLock(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSGcWriteSoftLock")
	}
	args := cArgs.Args.(*roachpb.GCWriteSoftockRequest)
	ok, err := engine.MVCCRemoveWriteSoftLock(ctx, batch, args.Txnrecord, args.Span, cArgs.Repl.slockcache)
	if err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "In EvalDyTSGcWriteSoftLock : Couldnt retrive the write lock %v", args.Txnrecord.ID)
		}
	}
	return EvalResult{}, nil
}
func EvalDyTSGcReadSoftLock(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSGcReadSoftLock")
	}
	args := cArgs.Args.(*roachpb.GCReadSoftockRequest)

	if args.Txnrecord.Status == roachpb.COMMITTED {
		if log.V(2) {
			log.Infof(ctx, "In Timstampcache updating read %v for span %v", args.Txnrecord.OrigTimestamp, args.Span)
		}
		cArgs.Repl.updateDyTSCache(args.Span, true /*read*/, args.Txnrecord.OrigTimestamp)
	}
	ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, args.Txnrecord, args.Span, cArgs.Repl.slockcache, args.Reverse)
	if err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "In EvalDyTSGcReadSoftLock : Couldnt retrive the read lock %v", args.Txnrecord.ID)
		}
	}
	return EvalResult{}, nil
}

func EvalDyTSResolveWriteSoftLock(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSResolveWriteSoftLock")
	}
	args := cArgs.Args.(*roachpb.ResolveWriteSoftLocksRequest)
	// Update the DyTs time stamp with latest commited timestamp
	cArgs.Repl.updateDyTSCache(args.Span, false /*write*/, args.Txnrecord.OrigTimestamp)
	if log.V(2) {
		log.Infof(ctx, "In Timstampcache updating write %v for span %v", args.Txnrecord.OrigTimestamp, args.Span)
	}
	ok, err := engine.MVCCresolveWriteSoftLock(ctx, batch, cArgs.Stats, args.Span, args.Txnrecord.OrigTimestamp, &args.Txnrecord, cArgs.Repl.slockcache)
	if err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "In EvalDyTSResolveWriteSoftLock : Couldnt retrive the write lock, txn id %v", args.Txnrecord.ID)
		}
	}
	return EvalResult{}, nil
}

// resolveLocalSoftLocks synchronously resolves any soft locks that are
// local to this range in the same batch. The remainder are collected
// and returned so that they can be handed off to asynchronous
// processing.
func (r *Replica) resolveLocalSoftLocks(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args roachpb.DyTSEndTransactionRequest,
	txn *roachpb.Transaction,
) ([]roachpb.Span, []roachpb.Span, []roachpb.Span) {
	if log.V(2) {
		log.Infof(ctx, "Ravi :resolveLocalSoftLocks : begin ")
	}
	desc := r.Desc()
	var preMergeDesc *roachpb.RangeDescriptor
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		preMergeDesc = desc
		desc = &mergeTrigger.LeftDesc
	}

	var externalSoftWriteSpan []roachpb.Span
	for _, span := range args.WriteSpans {
		if log.V(2) {
			log.Infof(ctx, "Ravi :resolving write span : %v ", span)
		}
		if err := func() error {
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(*desc, span.Key) {
					externalSoftWriteSpan = append(externalSoftWriteSpan, span)
					return nil
				}
				resolveMS := ms
				if preMergeDesc != nil && !containsKey(*preMergeDesc, span.Key) {
					// If this transaction included a merge and the intents
					// are from the subsumed range, ignore the intent resolution
					// stats, as they will already be accounted for during the
					// merge trigger.
					resolveMS = nil
				}
				// Update the DyTs time stamp with latest commited timestamp
				r.updateDyTSCache(span, false /*write*/, *args.Deadline)
				if log.V(2) {
					log.Infof(ctx, "In Timstampcache updating write %v for span %v", *args.Deadline, span)
				}
				ok, err := engine.MVCCresolveWriteSoftLock(ctx, batch, resolveMS, span, txn.OrigTimestamp, txn, r.slockcache)
				if !ok {
					externalSoftWriteSpan = append(externalSoftWriteSpan, span)
				}
				return err
				//return engine.MVCCResolveWriteSoftLockUsingIter(ctx, batch, iterAndBuf, resolveMS, span)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				externalSoftWriteSpan = append(externalSoftWriteSpan, span)
			}
			if inSpan != nil {
				// Update the DyTs time stamp with latest commited timestamp
				r.updateDyTSCache(span, false /*write*/, *args.Deadline)
				if log.V(2) {
					log.Infof(ctx, "In Timstampcache updating write %v for span %v", *args.Deadline, span)
				}
				ok, err := engine.MVCCresolveWriteSoftLock(ctx, batch, ms, span, txn.OrigTimestamp, txn, r.slockcache)
				if !ok {
					externalSoftWriteSpan = append(externalSoftWriteSpan, span)
				}
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving Local Write Soft locks at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	var externalSoftReadSpan []roachpb.Span
	for _, span := range args.ReadSpans {
		if log.V(2) {
			log.Infof(ctx, "Ravi :resolving read span : %v ", span)
		}
		if err := func() error {
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(*desc, span.Key) {
					externalSoftReadSpan = append(externalSoftReadSpan, span)
					return nil
				}
				// Update the DyTs time stamp with latest commited timestamp
				r.updateDyTSCache(span, true /*read*/, *args.Deadline)
				if log.V(2) {
					log.Infof(ctx, "In Timstampcache updating read %v for span %v", *args.Deadline, span)
				}
				ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, *txn, span, r.slockcache, false)
				if !ok {
					externalSoftReadSpan = append(externalSoftReadSpan, span)
				}
				return err
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				externalSoftReadSpan = append(externalSoftReadSpan, span)
			}
			if inSpan != nil {
				// Update the DyTs time stamp with latest commited timestamp
				r.updateDyTSCache(span, true /*read*/, *args.Deadline)
				if log.V(2) {
					log.Infof(ctx, "In Timstampcache updating read %v for span %v", *args.Deadline, span)
				}
				ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, *txn, span, r.slockcache, false)
				if !ok {
					externalSoftReadSpan = append(externalSoftReadSpan, span)
				}
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving Local Write Soft locks at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	var externalReverseSoftReadSpan []roachpb.Span
	for _, span := range args.ReverseReadSpans {
		if log.V(2) {
			log.Infof(ctx, "Ravi :resolving reverse read span : %v ", span)
		}
		if err := func() error {

			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				externalReverseSoftReadSpan = append(externalReverseSoftReadSpan, span)
			}
			if inSpan != nil {
				// Update the DyTs time stamp with latest commited timestamp
				r.updateDyTSCache(span, true /*read*/, *args.Deadline)
				ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, *txn, span, r.slockcache, true /*reverse*/)
				if !ok {
					externalReverseSoftReadSpan = append(externalReverseSoftReadSpan, span)
				}
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving Local Write Soft locks at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi :resolveLocalSoftLocks end with external spans r %v, w %w", externalSoftReadSpan, externalSoftWriteSpan)
	}
	return externalSoftReadSpan, externalReverseSoftReadSpan, externalSoftWriteSpan
}

// GCLocalSoftLocks synchronously GCs any soft locks that are
// local to this range in the same batch. The remainder are collected
// and returned so that they can be handed off to asynchronous
// processing.
func (r *Replica) GCLocalSoftLocks(
	ctx context.Context,
	batch engine.ReadWriter,
	args roachpb.DyTSEndTransactionRequest,
	txn *roachpb.Transaction,
) ([]roachpb.Span, []roachpb.Span, []roachpb.Span) {
	if log.V(2) {
		log.Infof(ctx, "Ravi :GCLocalSoftLocks : begin ")
	}
	desc := r.Desc()
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).

		desc = &mergeTrigger.LeftDesc
	}

	var externalSoftWriteSpan []roachpb.Span
	for _, span := range args.WriteSpans {
		if err := func() error {
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(*desc, span.Key) {
					externalSoftWriteSpan = append(externalSoftWriteSpan, span)
					return nil
				}

				ok, err := engine.MVCCRemoveWriteSoftLock(ctx, batch, *txn, span, r.slockcache)
				if !ok {
					externalSoftWriteSpan = append(externalSoftWriteSpan, span)
				}
				return err
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				externalSoftWriteSpan = append(externalSoftWriteSpan, span)
			}
			if inSpan != nil {
				ok, err := engine.MVCCRemoveWriteSoftLock(ctx, batch, *txn, span, r.slockcache)
				if !ok {
					externalSoftWriteSpan = append(externalSoftWriteSpan, span)
				}
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving Local Write Soft locks at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	var externalSoftReadSpan []roachpb.Span
	for _, span := range args.ReadSpans {
		if err := func() error {
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(*desc, span.Key) {
					externalSoftReadSpan = append(externalSoftReadSpan, span)
					return nil
				}

				ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, *txn, span, r.slockcache, false)
				if !ok {
					externalSoftReadSpan = append(externalSoftReadSpan, span)
				}
				return err
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				externalSoftReadSpan = append(externalSoftReadSpan, span)
			}
			if inSpan != nil {
				ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, *txn, span, r.slockcache, false)
				if !ok {
					externalSoftReadSpan = append(externalSoftReadSpan, span)
				}
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving Local Write Soft locks at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	var externalReverseSoftReadSpan []roachpb.Span
	for _, span := range args.ReverseReadSpans {
		if log.V(2) {
			log.Infof(ctx, "Ravi :resolving reverse read span : %v ", span)
		}
		if err := func() error {

			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				externalReverseSoftReadSpan = append(externalReverseSoftReadSpan, span)
			}
			if inSpan != nil {
				// Update the DyTs time stamp with latest commited timestamp
				r.updateDyTSCache(span, true /*read*/, *args.Deadline)
				ok, err := engine.MVCCRemoveReadSoftLock(ctx, batch, *txn, span, r.slockcache, true /*reverse*/)
				if !ok {
					externalReverseSoftReadSpan = append(externalReverseSoftReadSpan, span)
				}
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving Local Write Soft locks at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi :GCLocalSoftLocks : end")
	}
	return externalSoftReadSpan, externalReverseSoftReadSpan, externalSoftWriteSpan
}
