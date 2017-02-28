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
		log.Infof(ctx, "In EvalDyTSValidatorEndTransaction")
	}
	args := cArgs.Args.(*roachpb.DyTSEndTransactionRequest)

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

	if args.Commit {
		// Resolve Local Write Soft locks
		externalWriteSpans := cArgs.Repl.resolveLocalSoftWriteLocks(ctx, batch, cArgs.Stats, *args, &txnRecord)
		if log.V(2) {
			log.Infof(ctx, "external write spans : %v", externalWriteSpans)
		}
		// Add external write soft locks to asyncronous processing
	} else {
		// add write spans to garbage collect
	}
	// add read spans to garbage collect

	//Asyncronously garbage collect soft locks

	return EvalResult{}, nil
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

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSValidateCommitAfter : coudnt find transaction record in this range")
		}
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	if log.V(2) {
		log.Infof(ctx, "EvalDyTSValidateCommitAfter : found transaction record in this range")
	}
	lowerBound := *args.LowerBound
	// Update the Transaction record
	if txnRecord.Status == roachpb.PENDING && !txnRecord.DynamicTimestampLowerBound.Equal(hlc.MaxTimestamp) {
		txnRecord.DynamicTimestampUpperBound.Backward(lowerBound)
	} else {
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

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "EvalDyTSValidateCommitAfter : coudnt find transaction record in this range")
		}
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	if log.V(2) {
		log.Infof(ctx, "EvalDyTSValidateCommitAfter : found transaction record in this range")
	}
	// Update the Transaction record
	upperBound := *args.UpperBound

	// Update the Transaction record
	if txnRecord.Status == roachpb.PENDING && !upperBound.Equal(hlc.MaxTimestamp) {
		txnRecord.DynamicTimestampLowerBound.Forward(upperBound)
	} else {
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
	if err := engine.MVCCRemoveWriteSoftLock(ctx, batch, args.Txnrecord, args.Span, cArgs.Repl.slockcache); err != nil {
		return EvalResult{}, err
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
	if err := engine.MVCCRemoveReadSoftLock(ctx, batch, args.Txnrecord, args.Span, cArgs.Repl.slockcache); err != nil {
		return EvalResult{}, err
	}
	return EvalResult{}, nil
}

// resolveLocalSoftWriteLocks synchronously resolves any soft locks that are
// local to this range in the same batch. The remainder are collected
// and returned so that they can be handed off to asynchronous
// processing.
func (r *Replica) resolveLocalSoftWriteLocks(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args roachpb.DyTSEndTransactionRequest,
	txn *roachpb.Transaction,
) []roachpb.Span {
	if log.V(2) {
		log.Infof(ctx, "Ravi :resolveLocalIntents : begin ")
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
				return engine.MVCCresolveWriteSoftLock(ctx, batch, resolveMS, span, txn.OrigTimestamp, txn, r.slockcache)
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
				return engine.MVCCresolveWriteSoftLock(ctx, batch, ms, span, txn.OrigTimestamp, txn, r.slockcache)
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
		log.Infof(ctx, "Ravi :resolveLocalIntents : end")
	}
	return externalSoftWriteSpan
}
