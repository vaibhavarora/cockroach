package storage

import (
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
	roachpb.UpdateTxnRecord:    {EvalDyTSValidatorCommand: EvalDyTSUpdateTransactionRecord},
	roachpb.DyTsEndTransaction: {EvalDyTSValidatorCommand: EvalDyTSValidatorEndTransaction},
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

func EvalDyTSValidatorEndTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	return EvalResult{}, nil

}
