package storage

import (
	//"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	//"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	//"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type DynanicTimeStamper struct {
	slockcache *SoftLockCache
}

var consultsDyTSMethods = [...]bool{
	roachpb.Put:            true,
	roachpb.ConditionalPut: true,
	roachpb.Increment:      true,
	roachpb.Delete:         true,
	roachpb.DeleteRange:    true,
	roachpb.Get:            true,
	roachpb.Scan:           true,
	roachpb.ReverseScan:    true,
	roachpb.EndTransaction: true,
}

func consultsDyTSCommands(r roachpb.Request) bool {
	m := r.Method()
	if m < 0 || m >= roachpb.Method(len(consultsDyTSMethods)) {
		return false
	}
	return consultsDyTSMethods[m]
}

type DyTSCommand struct {
	EvalDyTSCommand func(context.Context, *DynanicTimeStamper, roachpb.Header, roachpb.Request)
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

func NewDynamicTImeStamper() *DynanicTimeStamper {
	d := &DynanicTimeStamper{
		slockcache: NewSoftLockCache(),
	}
	return d
}

//func (d *DynanicTimeStamper) applySoftLockCache(ctx context.Context, ba *roachpb.BatchRequest) {
func (d *DynanicTimeStamper) processDynamicTimestamping(ctx context.Context, ba *roachpb.BatchRequest) (err error) {
	if ba.Header.Txn == nil {
		if log.V(2) {
			log.Infof(ctx, "Non transactional request ")
		}
		return nil
	}

	for _, union := range ba.Requests {
		args := union.GetInner()
		if consultsDyTSCommands(args) {
			if cmd, ok := DyTSCommands[args.Method()]; ok {
				if log.V(2) {
					log.Infof(ctx, "Ravi : In applySoftLockCache executing cmd %v , args %v", cmd, args.Method())
				}
				cmd.EvalDyTSCommand(ctx, d, ba.Header, args)
			} else {
				err = errors.Errorf("unrecognized command %s", args.Method())
				return err
			}
		}
	}
	return nil
}

func EvalDyTSGet(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {
	// Places read lock and returns already placed write locks
	d.slockcache.serveGet(ctx, h, req)

}

func EvalDyTSPut(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {
	// places write lock and returns already placed read and write locks
	d.slockcache.servePut(ctx, h, req)

}

func EvalDyTSConditionalPut(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {
	// places write lock and returns already placed read and write locks
	d.slockcache.serveConditionalPut(ctx, h, req)
}

func EvalDyTSInitPut(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {

}

func EvalDyTSIncrement(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {

}

func EvalDyTSDelete(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {

}

func EvalDyTSDeleteRange(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {

}

func EvalDyTSScan(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {

}

func EvalDyTSReverseScan(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {

}

func EvalDyTSEndTransaction(ctx context.Context, d *DynanicTimeStamper, h roachpb.Header, req roachpb.Request) {
	//removes all the read and write locks placed by the transaction
	d.slockcache.serveEndTransaction(ctx, h, req)
}
