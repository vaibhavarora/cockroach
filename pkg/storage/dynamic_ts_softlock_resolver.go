package storage

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"golang.org/x/net/context"
)

const softLockResolverTaskLimit = 100

type softLockResolver struct {
	store *Store

	sem chan struct{} // Semaphore to limit async goroutines.
}

func newSoftLockResolver(store *Store) *softLockResolver {
	ir := &softLockResolver{
		store: store,
		sem:   make(chan struct{}, softLockResolverTaskLimit),
	}
	return ir
}

// processSoftLocksAsync asynchronously processes soft locks which were
// encountered during another command but did not interfere with the
// execution of that command. This occurs in two cases: inconsistent
// reads and EndTransaction (which queues its own external intents for
// processing via this method). The two cases are handled somewhat
// differently and would be better served by different entry points,
// but combining them simplifies the plumbing necessary in Replica.
func (sr *softLockResolver) processWriteSoftLocksAsync(
	r *Replica,
	wslockspans []roachpb.Span,
	rslockspans []roachpb.Span,
	txnrecord roachpb.Transaction) {

	now := r.store.Clock().Now()
	ctx := context.TODO()
	stopper := r.store.Stopper()

	if txnrecord.Status == roachpb.COMMITTED && len(wslockspans) != 0 {
		err := stopper.RunLimitedAsyncTask(
			ctx, sr.sem, false /* wait */, func(ctx context.Context) {
				sr.resolveWriteSoftLocks(ctx, r, wslockspans, txnrecord, now, true /* wait */)
			})
		if err != nil {
			if err == stop.ErrThrottled {
				// A limited task was not available. Rather than waiting for one, we
				// reuse the current goroutine.
				sr.resolveWriteSoftLocks(ctx, r, wslockspans, txnrecord, now, true /* wait */)
			} else {
				log.Warningf(ctx, "failed to resolve intents: %s", err)
				return
			}
		}
	} else {
		err := stopper.RunLimitedAsyncTask(
			ctx, sr.sem, false /* wait */, func(ctx context.Context) {
				sr.gcWriteSoftLocks(ctx, r, wslockspans, txnrecord, now, true /* wait */)
			})
		if err != nil {
			if err == stop.ErrThrottled {
				// A limited task was not available. Rather than waiting for one, we
				// reuse the current goroutine.
				sr.gcWriteSoftLocks(ctx, r, wslockspans, txnrecord, now, true /* wait */)
			} else {
				log.Warningf(ctx, "failed to resolve intents: %s", err)
				return
			}
		}
	}

	if len(rslockspans) != 0 {
		err := stopper.RunLimitedAsyncTask(
			ctx, sr.sem, false /* wait */, func(ctx context.Context) {
				sr.gcReadSoftLocks(ctx, r, rslockspans, txnrecord, now, true /* wait */)
			})
		if err != nil {
			if err == stop.ErrThrottled {
				// A limited task was not available. Rather than waiting for one, we
				// reuse the current goroutine.
				sr.gcReadSoftLocks(ctx, r, rslockspans, txnrecord, now, true /* wait */)
			} else {
				log.Warningf(ctx, "failed to resolve intents: %s", err)
				return
			}
		}
	}

}

func (sr *softLockResolver) resolveWriteSoftLocks(
	ctx context.Context,
	r *Replica,
	wslockspans []roachpb.Span,
	txnrecord roachpb.Transaction,
	now hlc.Timestamp,
	wait bool,
) error {
	// Everything here is best effort; give up rather than waiting
	// too long (helps avoid deadlocks during test shutdown,
	// although this is imperfect due to the use of an
	// uninterruptible WaitGroup.Wait in beginCmds).
	ctxWithTimeout, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()

	var reqs []roachpb.Request
	for i := range wslockspans {
		wslockspan := wslockspans[i] // avoids a race in `i, intent := range ...`
		var resolveArgs roachpb.Request
		{

			resolveArgs = &roachpb.ResolveWriteSoftLocksRequest{
				Span:      wslockspan,
				Txnrecord: txnrecord,
			}

		}

		reqs = append(reqs, resolveArgs)
	}
	// Resolve all of the write soft locks.
	if len(reqs) > 0 {
		b := &client.Batch{}
		b.AddRawRequest(reqs...)
		action := func() error {
			// TODO(tschottdorf): no tracing here yet.
			return sr.store.DB().Run(ctx, b)
		}
		if wait || sr.store.Stopper().RunLimitedAsyncTask(
			ctxWithTimeout, sr.sem, true /* wait */, func(ctx context.Context) {
				if err := action(); err != nil {
					log.Warningf(ctx, "unable to resolve external soft locks: %s", err)
				}
			}) != nil {
			// Try async to not keep the caller waiting, but when draining
			// just go ahead and do it synchronously. See #1684.
			// TODO(tschottdorf): This is ripe for removal.
			if err := action(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sr *softLockResolver) gcWriteSoftLocks(
	ctx context.Context,
	r *Replica,
	wslockspans []roachpb.Span,
	txnrecord roachpb.Transaction,
	now hlc.Timestamp,
	wait bool,
) error {
	return nil
}

func (sr *softLockResolver) gcReadSoftLocks(
	ctx context.Context,
	r *Replica,
	wslockspans []roachpb.Span,
	txnrecord roachpb.Transaction,
	now hlc.Timestamp,
	wait bool,
) error {
	return nil
}
