package storage

import (
	//"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	//"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"math"
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
	for each := range wslocks {
		if log.V(2) {
			log.Infof(ctx, "Write locks acqurired on Get %v", each)
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
		log.Infof(ctx, "Ravi : evalPut: begin")
	}
	args := cArgs.Args.(*roachpb.PutRequest)
	var req roachpb.RequestUnion
	req.MustSetInner(args)
	engine.MVCCPlaceWriteSoftLock(ctx, cArgs.Header.Txn.TxnMeta, args.Key, req, cArgs.Repl.slockcache)
	return EvalResult{}, nil
}

func EvalDyTSConditionalPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	// places write lock and returns already placed read and write locks
	//cArgs.Repl.slockcache.serveConditionalPut(ctx, cArgs.Header, cArgs.Args)
	return EvalResult{}, nil
}

func EvalDyTSInitPut(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	//cArgs.Repl.slockcache.serveInitPut(ctx, cArgs.Header, cArgs.Args)
	return EvalResult{}, nil
}

func EvalDyTSIncrement(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {
	return EvalResult{}, nil
}

func EvalDyTSDelete(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	//cArgs.Repl.slockcache.serveDelete(ctx, cArgs.Header, cArgs.Args)
	return EvalResult{}, nil
}

func EvalDyTSDeleteRange(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	resp roachpb.Response) (EvalResult, error) {

	//cArgs.Repl.slockcache.serveDeleteRange(ctx, cArgs.Header, cArgs.Args, batch)
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

	for each := range wslocks {
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

	for each := range wslocks {
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
	//cArgs.Repl.slockcache.serveEndTransaction(ctx, cArgs.Header, cArgs.Args, batch)
	return EvalResult{}, nil
}

func getkeysusingIter(
	ctx context.Context,
	start, end roachpb.Key,
	batch engine.ReadWriter,
	h roachpb.Header,
	reverse bool) (keys []roachpb.Key) {
	maxKeys := int64(math.MaxInt64)
	if h.MaxSpanRequestKeys != 0 {
		// We have a batch of requests with a limit. We keep track of how many
		// remaining keys we can touch.
		maxKeys = h.MaxSpanRequestKeys
	}
	var res []roachpb.Key
	_, _, _ = engine.MVCCIterate(ctx, batch, start, end, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn, reverse,
		func(kv roachpb.KeyValue) (bool, error) {
			if int64(len(res)) == maxKeys {
				// Another key was found beyond the max limit.
				return true, nil
			}
			res = append(res, kv.Key)
			return false, nil
		}, nil, false)

	return res
}
