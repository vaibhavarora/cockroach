package storage

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
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
	roachpb.UpdateTxnRecord: {EvalDyTSValidatorCommand: EvalDyTSUpdateTransactionRecord},
}

type RpcArgs struct {
	lowerbound hlc.Timestamp
	upperbound hlc.Timestamp
	commitAQ   []enginepb.TxnMeta
	commitBQ   []enginepb.TxnMeta
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
	} else if ok {
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

	return EvalResult{}, nil
}

func updateTransactionrecord(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	rArgs RpcArgs) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : updateTransactionrecord")
	}

	updateTnxReq := &roachpb.UpdateTransactionRecordRequest{
		Span: roachpb.Span{
			Key: cArgs.Header.Txn.Key,
		},
		Tmeta:            cArgs.Header.Txn.TxnMeta,
		LowerBound:       rArgs.lowerbound,
		UpperBound:       rArgs.upperbound,
		CommitAfterThem:  rArgs.commitAQ,
		CommitBeforeThem: rArgs.commitBQ,
	}

	b := &client.Batch{}
	//b.Header = cArgs.Header
	//b.Header.Timestamp = hlc.ZeroTimestamp
	if log.V(2) {
		log.Infof(ctx, "Ravi : updateTnxReq %v", updateTnxReq)
	}
	b.AddRawRequest(updateTnxReq)

	if err := cArgs.Repl.store.db.Run(ctx, b); err != nil {
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

func makeTnxIntact(oldTnx roachpb.Transaction, newTnx *roachpb.Transaction) {
	newTnx.DynamicTimestampLowerBound.Forward(oldTnx.DynamicTimestampLowerBound)
	newTnx.DynamicTimestampUpperBound.Backward(oldTnx.DynamicTimestampUpperBound)

}

func pushSoftLocksOnReadToTnxRecord(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	wslocks []roachpb.WriteSoftLock) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi :pushSoftLocksOnReadToTnxRecord ")
	}

	var rARgs RpcArgs
	// Modify its Lower bound based on last committed write time stamp
	// Temporarily
	rARgs.lowerbound = hlc.ZeroTimestamp
	rARgs.upperbound = hlc.MaxTimestamp

	// Place txns of all the write locks
	for _, lock := range wslocks {
		rARgs.commitBQ = append(rARgs.commitBQ, lock.TransactionMeta)
	}

	// Update transaction
	if err := updateTransactionrecord(ctx, batch, cArgs, rARgs); err != nil {
		panic("failed to update transaction")
	}

	return nil
}

func pushSoftLocksOnWriteToTnxRecord(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	rslocks []roachpb.ReadSoftLock,
	wslocks []roachpb.WriteSoftLock) error {

	var rARgs RpcArgs
	// Modify its Lower bound based on last committed write time stamp
	// Temporarily
	rARgs.lowerbound = hlc.ZeroTimestamp
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
	if err := updateTransactionrecord(ctx, batch, cArgs, rARgs); err != nil {
		panic("failed to update transaction")
	}
	return nil
}

func manageCommitBeforeQueue(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	mytnxRecord *roachpb.Transaction) {
	/*
		for _, tnx := range mytnxRecord.CommitBeforeThem {
			tnxrcd, _ := fetchTransactionrecordv2(ctx, batch, cArgs, tnx)
			switch tnxrcd.Status {
			case roachpb.COMMITTED:
				ts := tnxrcd.DynamicTimestampLowerBound.Prev()
				mytnxRecord.DynamicTimestampUpperBound.Backward(ts)
				// trigger resolveSoftLock
			case roachpb.PENDING:
				ts := tnxrcd.DynamicTimestampLowerBound.Prev()
				mytnxRecord.DynamicTimestampUpperBound.Backward(ts)
			}
			updateTransactionrecordv2(ctx, batch, cArgs, tnx, tnxrcd)

		}*/
}

func manageCommitAfterQueue(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	mytnxRecord *roachpb.Transaction) {
	/*
		for _, tnx := range mytnxRecord.CommitAfterThem {
			tnxrcd, _ := fetchTransactionrecordv2(ctx, batch, cArgs, tnx)
			switch tnxrcd.Status {
			case roachpb.COMMITTED:
				ts := tnxrcd.DynamicTimestampUpperBound.Next()
				mytnxRecord.DynamicTimestampLowerBound.Forward(ts)
				// trigger resolveSoftLock
			case roachpb.PENDING:
				ts := mytnxRecord.DynamicTimestampLowerBound.Prev()
				tnxrcd.DynamicTimestampUpperBound.Backward(ts)
			}
			updateTransactionrecordv2(ctx, batch, cArgs, tnx, tnxrcd)
		}*/

}

func isTSIntact(lowerBound hlc.Timestamp, upperBound hlc.Timestamp) bool {
	if lowerBound.Less(upperBound) || lowerBound.Equal(upperBound) {
		return true
	}
	return false
}

func pickCommitTimeStamp(lowerBound hlc.Timestamp, upperBound hlc.Timestamp) hlc.Timestamp {
	if upperBound == hlc.MaxTimestamp || upperBound == lowerBound {
		return lowerBound
	}
	return lowerBound
}

func validateTimeStamp(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	mytnxRecord *roachpb.Transaction) {

	manageCommitBeforeQueue(ctx, batch, cArgs, mytnxRecord)
	manageCommitAfterQueue(ctx, batch, cArgs, mytnxRecord)

	if !isTSIntact(mytnxRecord.DynamicTimestampLowerBound, mytnxRecord.DynamicTimestampUpperBound) {
		mytnxRecord.Status = roachpb.ABORTED
	}
	pickCommitTimeStamp(mytnxRecord.DynamicTimestampLowerBound, mytnxRecord.DynamicTimestampUpperBound)

}
