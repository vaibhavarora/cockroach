package engine

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
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
}

func EvalDyTSMVCCPut(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCPut")
	}
	return nil
}

func EvalDyTSMVCCConditionalPut(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCConditionalPut")
	}
	return nil
}

func EvalDyTSMVCCInitPut(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCInitPut")
	}
	return nil
}

func EvalDyTSMVCCIncrement(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCIncrement")
	}
	return nil
}

func EvalDyTSMVCCDelete(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCDelete")
	}
	return nil
}

func EvalDyTSMVCCDeleteRange(
	ctx context.Context,
	mARgs DyTSmArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSMVCCDeleteRange")
	}
	return nil
}
