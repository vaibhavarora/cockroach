package storage

import (
	//"fmt"
	//"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	//"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type DyTSCommand struct {
	//EvalDyTSCommand func(context.Context, roachpb.Header, roachpb.Request)
	EvalDyTSCommand func(context.Context, engine.ReadWriter, CommandArgs, roachpb.Response) (EvalResult, SoftLocks, error)
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
	reply roachpb.Response) (EvalResult, SoftLocks, *roachpb.Error) {

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return EvalResult{}, SoftLocks{}, nil
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
			return EvalResult{}, SoftLocks{}, pErr
		}
	}

	var err error
	var pd EvalResult
	var slocks SoftLocks

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
		pd, slocks, err = cmd.EvalDyTSCommand(ctx, batch, cArgs, reply)
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

	return pd, slocks, pErr

}

func EvalDyTSGet(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {
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
	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {
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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	slocks.rslocks = append(slocks.rslocks, rslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSConditionalPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {

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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	slocks.rslocks = append(slocks.rslocks, rslocks...)
	return EvalResult{}, slocks, nil

}

func EvalDyTSInitPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {

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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	slocks.rslocks = append(slocks.rslocks, rslocks...)
	return EvalResult{}, slocks, nil
}

// Since this method reads the value an then writes it we place both read and write locks
func EvalDyTSIncrement(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {

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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	slocks.rslocks = append(slocks.rslocks, rslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSDelete(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {

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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	slocks.rslocks = append(slocks.rslocks, rslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSDeleteRange(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : EvalDyTSDelete: begin")
	}
	args := cArgs.Args.(*roachpb.DeleteRangeRequest)
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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	slocks.rslocks = append(slocks.rslocks, rslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSScan(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {
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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSReverseScan(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {
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

	var slocks SoftLocks
	slocks.wslocks = append(slocks.wslocks, wslocks...)
	return EvalResult{}, slocks, nil
}

func EvalDyTSEndTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, SoftLocks, error) {

	if log.V(2) {
		log.Infof(ctx, "In EvalDyTSEndTransaction")
	}

	//r := cArgs.Repl
	args := cArgs.Args.(*roachpb.EndTransactionRequest)
	//h := cArgs.Header
	//ms := cArgs.Stats
	//reply := resp.(*roachpb.EndTransactionResponse)

	// split the read lock spans into local and external
	rlocalspans, rexternalspans := cArgs.Repl.getSpans(ctx, batch, *args, args.ReadSpans)

	// split  the write lock spans into local and external
	wlocalspans, wexternalspans := cArgs.Repl.getSpans(ctx, batch, *args, args.IntentSpans)

	// get all local write soft locks placed by the transaction
	//wslocks := engine.MVCCGetWriteSoftLock(ctx, batch, cArgs.Header, wlocalspans, cArgs.Repl.slockcache)

	var slocks SoftLocks
	//slocks.rslocks = append(slocks.rslocks, rslocks...)
	slocks.wlocalspans = append(slocks.wlocalspans, wlocalspans.)
	slocks.rlocalspans = append(slocks.rlocalspans, rlocalspans...)
	slocks.rextspans = append(slocks.rextspans, rexternalspans...)
	slocks.wextspans = append(slocks.wextspans, wexternalspans...)

	return EvalResult{}, slocks, nil
}

func (r *Replica) getSpans(
	ctx context.Context,
	batch engine.ReadWriter,
	args roachpb.EndTransactionRequest,
	Spans []roachpb.Span,
) ([]roachpb.Span, []roachpb.Span) {
	if log.V(2) {
		log.Infof(ctx, "Ravi :getSpans : begin ")
	}
	desc := r.Desc()
	//var preMergeDesc *roachpb.RangeDescriptor
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		//preMergeDesc = desc
		desc = &mergeTrigger.LeftDesc
	}

	iterAndBuf := engine.GetIterAndBuf(batch)
	defer iterAndBuf.Cleanup()

	var externalSoftLocks []roachpb.Span
	var localSoftLocks []roachpb.Span
	for _, span := range Spans {

		if len(span.EndKey) == 0 {
			// For single-key intents, do a KeyAddress-aware check of
			// whether it's contained in our Range.
			if !containsKey(*desc, span.Key) {
				externalSoftLocks = append(externalSoftLocks, span)
			} else {
				localSoftLocks = append(localSoftLocks, span)
			}
			/*resolveMS := ms
			if preMergeDesc != nil && !containsKey(*preMergeDesc, span.Key) {
				// If this transaction included a merge and the intents
				// are from the subsumed range, ignore the intent resolution
				// stats, as they will already be accounted for during the
				// merge trigger.
				resolveMS = nil
			}*/

		}
		// For intent ranges, cut into parts inside and outside our key
		// range. Resolve locally inside, delegate the rest. In particular,
		// an intent range for range-local data is correctly considered local.
		inSpan, outSpans := intersectSpan(span, *desc)
		for _, span := range outSpans {
			externalSoftLocks = append(externalSoftLocks, span)
		}
		if inSpan != nil {
			localSoftLocks = append(localSoftLocks, *inSpan)
		}

	}

	return localSoftLocks, externalSoftLocks
}
