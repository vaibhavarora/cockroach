package engine

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
	"math"
)

type DyTSMVCCCommand struct {
	EvalDyTSMVCCCommand func(context.Context, DyTSmArgs) error
}

var DyTSMVCCCommands = map[roachpb.Method]DyTSMVCCCommand{
	roachpb.Put:            {EvalDyTSMVCCCommand: EvalDyTSMVCCPut},
	roachpb.ConditionalPut: {EvalDyTSMVCCCommand: EvalDyTSMVCCConditionalPut},
	roachpb.InitPut:        {EvalDyTSMVCCCommand: EvalDyTSMVCCInitPut},
	roachpb.Increment:      {EvalDyTSMVCCCommand: EvalDyTSMVCCIncrement},
	roachpb.Delete:         {EvalDyTSMVCCCommand: EvalDyTSMVCCDelete},
	roachpb.DeleteRange:    {EvalDyTSMVCCCommand: EvalDyTSMVCCDeleteRange},
}

type DyTSmArgs struct {
	engine    ReadWriter
	ms        *enginepb.MVCCStats
	timestamp hlc.Timestamp
	txn       *roachpb.Transaction
	Args      roachpb.Request
	slcache   *SoftLockCache
}

func EvalDyTSMVCCPut(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCPut")
	}
	args := mARgs.Args.(*roachpb.PutRequest)

	ts := hlc.ZeroTimestamp
	if !args.Inline {
		ts = mARgs.timestamp
	}
	/*
		if h.DistinctSpans {
			if b, ok := batch.(engine.Batch); ok {
				// Use the distinct batch for both blind and normal ops so that we don't
				// accidentally flush mutations to make them visible to the distinct
				// batch.
				batch = b.Distinct()
				defer batch.Close()
			}
		}*/
	if args.Blind {
		return MVCCBlindPut(ctx, mARgs.engine, mARgs.ms, args.Key, ts, args.Value, mARgs.txn, mARgs.slcache, true)
	}
	return MVCCPut(ctx, mARgs.engine, mARgs.ms, args.Key, ts, args.Value, mARgs.txn, mARgs.slcache, true)
}

func EvalDyTSMVCCConditionalPut(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCConditionalPut")
	}
	args := mARgs.Args.(*roachpb.ConditionalPutRequest)

	/*if h.DistinctSpans {
		if b, ok := batch.(engine.Batch); ok {
			// Use the distinct batch for both blind and normal ops so that we don't
			// accidentally flush mutations to make them visible to the distinct
			// batch.
			batch = b.Distinct()
			defer batch.Close()
		}
	}*/
	if args.Blind {
		return MVCCBlindConditionalPut(ctx, mARgs.engine, mARgs.ms, args.Key, mARgs.timestamp, args.Value, args.ExpValue, mARgs.txn, mARgs.slcache, true)
	}
	return MVCCConditionalPut(ctx, mARgs.engine, mARgs.ms, args.Key, mARgs.timestamp, args.Value, args.ExpValue, mARgs.txn, mARgs.slcache, true)
}

func EvalDyTSMVCCInitPut(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCInitPut")
	}
	args := mARgs.Args.(*roachpb.InitPutRequest)

	return MVCCInitPut(ctx, mARgs.engine, mARgs.ms, args.Key, mARgs.timestamp, args.Value, mARgs.txn, mARgs.slcache, true)
}

func EvalDyTSMVCCIncrement(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCIncrement")
	}
	args := mARgs.Args.(*roachpb.IncrementRequest)

	//reply := resp.(*roachpb.IncrementResponse)

	_, err := MVCCIncrement(ctx, mARgs.engine, mARgs.ms, args.Key, mARgs.timestamp, mARgs.txn, args.Increment, mARgs.slcache, true)
	//reply.NewValue = newVal
	return err
}

func EvalDyTSMVCCDelete(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCDelete")
	}
	args := mARgs.Args.(*roachpb.DeleteRequest)

	return MVCCDelete(ctx, mARgs.engine, mARgs.ms, args.Key, mARgs.timestamp, mARgs.txn, mARgs.slcache, true)
}

func EvalDyTSMVCCDeleteRange(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCDeleteRange")
	}
	args := mARgs.Args.(*roachpb.DeleteRangeRequest)

	timestamp := hlc.ZeroTimestamp
	if !args.Inline {
		timestamp = mARgs.timestamp
	}
	maxKeys := int64(math.MaxInt64)
	_, _, _, err := MVCCDeleteRange(
		ctx, mARgs.engine, mARgs.ms, args.Key, args.EndKey, maxKeys, timestamp, mARgs.txn, args.ReturnKeys, mARgs.slcache, true,
	)

	return err
}
