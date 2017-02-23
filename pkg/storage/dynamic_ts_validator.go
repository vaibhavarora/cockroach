package storage

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type DyTSValidationRequest struct {
	//EvalDyTSCommand func(context.Context, roachpb.Header, roachpb.Request)
	EvalDyTSValidationRequest func(context.Context, *Store, engine.ReadWriter, roachpb.Header, SoftLocks) error
}

var DyTSValidationRequests = map[roachpb.Method]DyTSValidationRequest{
	roachpb.Get:            {EvalDyTSValidationRequest: EvalDyTSValidationRequestGet},
	roachpb.Put:            {EvalDyTSValidationRequest: EvalDyTSValidationRequestPut},
	roachpb.ConditionalPut: {EvalDyTSValidationRequest: EvalDyTSValidationRequestConditionalPut},
	roachpb.InitPut:        {EvalDyTSValidationRequest: EvalDyTSValidationRequestInitPut},
	roachpb.Increment:      {EvalDyTSValidationRequest: EvalDyTSValidationRequestIncrement},
	roachpb.Delete:         {EvalDyTSValidationRequest: EvalDyTSValidationRequestDelete},
	roachpb.DeleteRange:    {EvalDyTSValidationRequest: EvalDyTSValidationRequestDeleteRange},
	roachpb.Scan:           {EvalDyTSValidationRequest: EvalDyTSValidationRequestScan},
	roachpb.ReverseScan:    {EvalDyTSValidationRequest: EvalDyTSValidationRequestReverseScan},
	roachpb.EndTransaction: {EvalDyTSValidationRequest: EvalDyTSValidationRequestEndTransaction},
}

type RpcArgs struct {
	lowerbound hlc.Timestamp
	upperbound hlc.Timestamp
	commitAQ   []enginepb.TxnMeta
	commitBQ   []enginepb.TxnMeta
}

func (r *Replica) ApplyDyTSValidation(
	ctx context.Context,
	args roachpb.Request,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) (err error) {

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return nil
	}

	if log.V(2) {
		log.Infof(ctx, "Ravi : In applyDyTSValidation")
	}

	if cmd, ok := DyTSValidationRequests[args.Method()]; ok {
		err = cmd.EvalDyTSValidationRequest(ctx, r.store, batch, h, slocks)
	} else {
		err = errors.Errorf("unrecognized command %s", args.Method())
		return err
	}

	if log.V(2) {
		log.Infof(ctx, "executed %s command %+v: %+v, err=%v", args.Method(), args, err)
	}
	if err != nil {
		return err
	}
	return nil
}

func EvalDyTSValidationRequestGet(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestGet")
	}
	if len(slocks.wslocks) != 0 {
		if err := pushSoftLocksOnReadToTnxRecord(ctx, s, batch, h, slocks.wslocks); err != nil {
			panic("failed to place soft  locks in Tnx Record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
	}
	return nil
}

func EvalDyTSValidationRequestPut(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestPut")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTnxRecord(ctx, s, batch, h, slocks.rslocks, slocks.wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	return nil
}

func EvalDyTSValidationRequestConditionalPut(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestConditionalPut")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTnxRecord(ctx, s, batch, h, slocks.rslocks, slocks.wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	return nil
}
func EvalDyTSValidationRequestInitPut(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestInitPut")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTnxRecord(ctx, s, batch, h, slocks.rslocks, slocks.wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	return nil
}
func EvalDyTSValidationRequestIncrement(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestIncrement")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTnxRecord(ctx, s, batch, h, slocks.rslocks, slocks.wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	return nil
}
func EvalDyTSValidationRequestDelete(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestDelete")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTnxRecord(ctx, s, batch, h, slocks.rslocks, slocks.wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	return nil
}
func EvalDyTSValidationRequestDeleteRange(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestDeleteRange")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTnxRecord(ctx, s, batch, h, slocks.rslocks, slocks.wslocks); err != nil {
			panic("failed to place locks in transaction record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write or Read locks acqurired on Put ")
		}
	}
	return nil
}
func EvalDyTSValidationRequestScan(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestScan")
	}
	if len(slocks.wslocks) != 0 {
		if err := pushSoftLocksOnReadToTnxRecord(ctx, s, batch, h, slocks.wslocks); err != nil {
			panic("failed to place soft  locks in Tnx Record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
	}
	return nil
}
func EvalDyTSValidationRequestReverseScan(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestReverseScan")
	}
	if len(slocks.wslocks) != 0 {
		if err := pushSoftLocksOnReadToTnxRecord(ctx, s, batch, h, slocks.wslocks); err != nil {
			panic("failed to place soft  locks in Tnx Record")
		}
	} else {
		if log.V(2) {
			log.Infof(ctx, " No Write locks acqurired on Get ")
		}
	}
	return nil
}

func EvalDyTSValidationRequestEndTransaction(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	slocks SoftLocks) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestEndTransaction")
	}

	return nil
}

func transactionRecordExists(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header) bool {
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)

	var txnRecord roachpb.Transaction

	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return false
	} else if !ok {
		return false
	}
	return true
}

func updateTransactionRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	rArgs RpcArgs) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In updateTransactionrecord")
	}

	if !transactionRecordExists(ctx, s, batch, h) {
		// Transaction is in different range so making a RPC call
		return sendUpdateTransactionRecordRPC(ctx, s, h, rArgs)
	}

	return updateLocalTransactionRecord(ctx, s, h, rArgs)

}

func updateLocalTransactionRecord(
	ctx context.Context,
	s *Store,
	h roachpb.Header,
	rArgs RpcArgs) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In updateLocalTransactionRecord")
	}
	return nil
}

func sendUpdateTransactionRecordRPC(
	ctx context.Context,
	s *Store,
	h roachpb.Header,
	rArgs RpcArgs) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : sendUpdateTransactionRecordRPC")
	}

	updateTnxReq := &roachpb.UpdateTransactionRecordRequest{
		Span: roachpb.Span{
			Key: h.Txn.Key,
		},
		Tmeta:            h.Txn.TxnMeta,
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

	if err := s.db.Run(ctx, b); err != nil {
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
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
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
	if err := updateTransactionRecord(ctx, s, batch, h, rARgs); err != nil {
		panic("failed to update transaction")
	}

	return nil
}

func pushSoftLocksOnWriteToTnxRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	rslocks []roachpb.ReadSoftLock,
	wslocks []roachpb.WriteSoftLock) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :pushSoftLocksOnWriteToTnxRecord ")
	}
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
	if err := updateTransactionRecord(ctx, s, batch, h, rARgs); err != nil {
		panic("failed to update transaction")
	}
	return nil
}

func manageCommitBeforeQueue(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	mytnxRecord *roachpb.Transaction) {
	if log.V(2) {
		log.Infof(ctx, "Ravi :pushSoftLocksOnWriteToTnxRecord ")
	}
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
	if log.V(2) {
		log.Infof(ctx, "Ravi :manageCommitAfterQueue ")
	}
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
	if log.V(2) {
		log.Infof(ctx, "Ravi :validateTimeStamp ")
	}

	manageCommitBeforeQueue(ctx, batch, cArgs, mytnxRecord)
	manageCommitAfterQueue(ctx, batch, cArgs, mytnxRecord)

	if !isTSIntact(mytnxRecord.DynamicTimestampLowerBound, mytnxRecord.DynamicTimestampUpperBound) {
		mytnxRecord.Status = roachpb.ABORTED
	}
	pickCommitTimeStamp(mytnxRecord.DynamicTimestampLowerBound, mytnxRecord.DynamicTimestampUpperBound)

}
