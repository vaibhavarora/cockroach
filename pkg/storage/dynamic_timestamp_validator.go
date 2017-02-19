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
	roachpb.GetTxnRecord: {EvalDyTSValidatorCommand: EvalDyTSGetTransactionRecord},
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

func EvalDyTSGetTransactionRecord(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSGetTransactionRecord")
	}
	//args := cArgs.Args.(*roachpb.ReverseScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.GetTransactionRecordResponse)

	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)

	var existingTxn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &existingTxn,
	); err != nil {
		return EvalResult{}, err
	} else if !ok {
		return EvalResult{}, roachpb.NewTransactionStatusError("does not exist")
	}
	reply.Txn = &existingTxn

	return EvalResult{}, nil
}

func fetchTransactionrecord(
	ctx context.Context,
	batch engine.ReadWriter,
	h roachpb.Header,
	store *Store) (roachpb.Transaction, error) {

	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)

	var txnRecord roachpb.Transaction

	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return txnRecord, err
	} else if !ok {
		if log.V(2) {
			log.Infof(ctx, "fetchTransactionrecord : Couldnt find transaction record in same range, going for RPC")
		}
		getTnxReq := &roachpb.GetTransactionRecordRequest{
			Span: roachpb.Span{
				Key: h.Txn.Key,
			},
		}

		b := &client.Batch{}
		b.Header = h
		b.Header.Timestamp = hlc.ZeroTimestamp
		b.AddRawRequest(getTnxReq)

		if err := store.db.Run(ctx, b); err != nil {
			_ = b.MustPErr()
		}

		var r roachpb.GetTransactionRecordResponse
		br := b.RawResponse()
		for _, res := range br.Responses {
			r := res.GetInner().(*roachpb.GetTransactionRecordResponse)
			if log.V(2) {
				log.Infof(ctx, "recieved tnx record : %v", r.TxnRecord)
			}
		}
		txnRecord = r.TxnRecord
	} else if ok {
		if log.V(2) {
			log.Infof(ctx, "fetchTransactionrecord : found transaction record in same range")
		}
	}
	return txnRecord, nil
}
