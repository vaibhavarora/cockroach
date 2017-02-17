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
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type DyTSCommand struct {
	//EvalDyTSCommand func(context.Context, roachpb.Header, roachpb.Request)
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
	roachpb.GetTxnRecord:   {EvalDyTSCommand: EvalDyTSGetTransactionRecord},
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
			log.Infof(ctx, "Ravi : executing cmd %v with cArgs %v ", cmd, cArgs)
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
	reply.Value = val
	// check if the read is snapshot read

	// Update Transaction record with write locks
	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on EvalDyTSGet %v", each)
		}
	}
	return EvalResult{}, err
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
	args := cArgs.Args.(*roachpb.InitPutRequest)
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
	args := cArgs.Args.(*roachpb.InitPutRequest)
	var req roachpb.RequestUnion
	req.MustSetInner(args)

	keys := engine.MVCCgetKeysUsingIter(ctx, args.Key, args.EndKey, batch, cArgs.Header, false)

	var wslocks []roachpb.WriteSoftLock
	var rslocks []roachpb.ReadSoftLock

	for _, eachkey := range keys {
		rslockstmp, wslockstmp := engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, eachkey, req, cArgs.Repl.slockcache)
		wslocks = append(wslocks, wslockstmp...)
		rslocks = append(rslocks, rslockstmp...)
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

	rows, resumeSpan, _, wslocks, err := engine.MVCCScan(ctx, batch, args.Key, args.EndKey,
		cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)

	reply.NumKeys = int64(len(rows))
	reply.ResumeSpan = resumeSpan
	reply.Rows = rows

	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on Scan %v", each)
		}
	}
	return EvalResult{}, err
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

	rows, resumeSpan, _, wslocks, err := engine.MVCCReverseScan(ctx, batch, args.Key, args.EndKey,
		cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, cArgs.Repl.slockcache, true)

	reply.NumKeys = int64(len(rows))
	reply.ResumeSpan = resumeSpan
	reply.Rows = rows

	for _, each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on Scan %v", each)
		}
	}

	return EvalResult{}, err
}

func EvalDyTSEndTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	//removes all the read and write locks placed by the transaction

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSEndTransaction")
	}
	return EvalResult{}, nil
}

func DyTSgetTransactionrecord(ctx context.Context, getTnxReq roachpb.Request, store *Store) roachpb.Transaction {
	/*getTnxReq := &roachpb.GetTransactionRecordRequest{
		Span: roachpb.Span{
			Key: cArgs.Header.Txn.Key,
		},
	}*/

	b := &client.Batch{}
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

	return r.TxnRecord
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
