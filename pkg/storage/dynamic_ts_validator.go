package storage

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type DyTSValidationRequest struct {
	EvalDyTSValidationRequest func(context.Context, *Store, engine.ReadWriter, *TransactionRecordLockCache, roachpb.Header, SoftLocks, roachpb.Response) error
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
	slocks SoftLocks,
	reply roachpb.Response,
) (err error) {

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return nil
	}

	if log.V(2) {
		log.Infof(ctx, "Ravi : In applyDyTSValidation")
	}

	if cmd, ok := DyTSValidationRequests[args.Method()]; ok {
		err = cmd.EvalDyTSValidationRequest(ctx, r.store, batch, r.txnlockcache, h, slocks, reply)
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestGet")
	}
	if len(slocks.wslocks) != 0 {
		if err := pushSoftLocksOnReadToTxnRecord(ctx, s, batch, txncache, h, slocks.wslocks); err != nil {
			panic("failed to place soft  locks in txn Record")
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestPut")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, s, batch, txncache, h, slocks.rslocks, slocks.wslocks); err != nil {
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestConditionalPut")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, s, batch, txncache, h, slocks.rslocks, slocks.wslocks); err != nil {
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestInitPut")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, s, batch, txncache, h, slocks.rslocks, slocks.wslocks); err != nil {
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestIncrement")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, s, batch, txncache, h, slocks.rslocks, slocks.wslocks); err != nil {
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestDelete")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, s, batch, txncache, h, slocks.rslocks, slocks.wslocks); err != nil {
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestDeleteRange")
	}
	if len(slocks.wslocks) != 0 || len(slocks.rslocks) != 0 {
		if err := pushSoftLocksOnWriteToTxnRecord(ctx, s, batch, txncache, h, slocks.rslocks, slocks.wslocks); err != nil {
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestScan")
	}
	if len(slocks.wslocks) != 0 {
		if err := pushSoftLocksOnReadToTxnRecord(ctx, s, batch, txncache, h, slocks.wslocks); err != nil {
			panic("failed to place soft  locks in txn Record")
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestReverseScan")
	}
	if len(slocks.wslocks) != 0 {
		if err := pushSoftLocksOnReadToTxnRecord(ctx, s, batch, txncache, h, slocks.wslocks); err != nil {
			panic("failed to place soft  locks in txn Record")
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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	slocks SoftLocks,
	resp roachpb.Response,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestEndTransaction")
	}
	var err error
	var txnrcd roachpb.Transaction
	reply := resp.(*roachpb.EndTransactionResponse)
	if txnrcd, err = executeValidator(ctx, s, batch, txncache, h); err != nil {
		return err
	}
	if err = executeDyTSDecision(ctx, s, batch, h, txnrcd, slocks); err != nil {
		return err
	}
	// updating the response
	reply.Txn = &txnrcd
	return nil
}

func executeDyTSDecision(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	txnrcd roachpb.Transaction,
	slocks SoftLocks,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In executeDyTSDecision")
	}
	// resolve write intents

	// clear read and write locks
	return nil
}

func transactionRecordExists(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	tkey roachpb.Key,
	txnid uuid.UUID,
) bool {
	key := keys.TransactionKey(tkey, txnid)

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
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	rArgs RpcArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In updateTransactionrecord")
	}

	if !transactionRecordExists(ctx, s, batch, h.Txn.Key, *h.Txn.ID) {
		// Transaction is in different range so making a RPC call
		return sendUpdateTransactionRecordRPC(ctx, s, h, rArgs)
	}

	return updateLocalTransactionRecord(ctx, batch, txncache, h, rArgs)

}

func updateLocalTransactionRecord(
	ctx context.Context,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	rArgs RpcArgs,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In updateLocalTransactionRecord")
	}
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return err
	} else if ok {
		// Update the Transaction record
		txnRecord.DynamicTimestampLowerBound.Forward(rArgs.lowerbound)
		txnRecord.DynamicTimestampUpperBound.Backward(rArgs.upperbound)
		for _, txn := range rArgs.commitAQ {
			txnRecord.CommitAfterThem = append(txnRecord.CommitAfterThem, txn)
		}
		for _, txn := range rArgs.commitBQ {
			txnRecord.CommitBeforeThem = append(txnRecord.CommitBeforeThem, txn)
		}
		// Save the updated Transaction record
		return engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)
	}

	return nil
}

func sendUpdateTransactionRecordRPC(
	ctx context.Context,
	s *Store,
	h roachpb.Header,
	rArgs RpcArgs,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : sendUpdateTransactionRecordRPC")
	}

	updatetxnReq := &roachpb.UpdateTransactionRecordRequest{
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
		log.Infof(ctx, "Ravi : updatetxnReq %v", updatetxnReq)
	}
	b.AddRawRequest(updatetxnReq)

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

func pushSoftLocksOnReadToTxnRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	wslocks []roachpb.WriteSoftLock,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi :pushSoftLocksOnReadToTxnRecord ")
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
	if err := updateTransactionRecord(ctx, s, batch, txncache, h, rARgs); err != nil {
		panic("failed to update transaction")
	}

	return nil
}

func pushSoftLocksOnWriteToTxnRecord(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
	rslocks []roachpb.ReadSoftLock,
	wslocks []roachpb.WriteSoftLock,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :pushSoftLocksOnWriteToTxnRecord ")
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
	if err := updateTransactionRecord(ctx, s, batch, txncache, h, rARgs); err != nil {
		panic("failed to update transaction")
	}
	return nil
}

func manageCommitBeforeQueue(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :manageCommitBeforeQueue ")
	}

	for _, txn := range mytxnRecord.CommitBeforeThem {
		if err := validateCommitBefore(ctx, s, batch, txncache, mytxnRecord, txn); err != nil {
			return err
		}
	}
	return nil
}

// validates the commit before case
func validateCommitBefore(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
	othertxn enginepb.TxnMeta,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In validateCommitBefore")
	}
	var upperBound hlc.Timestamp
	var err error
	if !transactionRecordExists(ctx, s, batch, othertxn.Key, *othertxn.ID) {
		// Transaction is in different range so making a RPC call
		upperBound, err = sendValidateCommitBeforeRPC(ctx, s, mytxnRecord.DynamicTimestampLowerBound, othertxn)
	}

	upperBound, err = executelocalValidateCommitBefore(ctx, s, batch, txncache, mytxnRecord.DynamicTimestampLowerBound, othertxn)

	mytxnRecord.DynamicTimestampUpperBound.Backward(upperBound)

	return err
}

func sendValidateCommitBeforeRPC(
	ctx context.Context,
	s *Store,
	upperBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendValidateCommitBeforeRPC")
	}
	return hlc.ZeroTimestamp, nil
}

func executelocalValidateCommitBefore(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	upperBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In executelocalValidateCommitBefore")
	}
	key := keys.TransactionKey(txn.Key, *txn.ID)

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return hlc.MaxTimestamp, err
	} else if ok {
		// Update the Transaction record
		upperBound.Backward(txnRecord.DynamicTimestampLowerBound)
		// Save the updated Transaction record
		//return engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)
	}
	return upperBound, nil
}

func validateCommitAfter(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
	othertxn enginepb.TxnMeta,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In validateCommitAfter")
	}
	var lowerBound hlc.Timestamp
	var err error
	if !transactionRecordExists(ctx, s, batch, othertxn.Key, *othertxn.ID) {
		// Transaction is in different range so making a RPC call
		lowerBound, err = sendValidateCommitAfterRPC(ctx, s, mytxnRecord.DynamicTimestampLowerBound, othertxn)
	}

	lowerBound, err = executelocalValidateCommitAfter(ctx, s, batch, txncache, mytxnRecord.DynamicTimestampLowerBound, othertxn)

	mytxnRecord.DynamicTimestampLowerBound.Forward(lowerBound)

	return err
}

func sendValidateCommitAfterRPC(
	ctx context.Context,
	s *Store,
	lowerBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendValidateCommitAfterRPC")
	}
	return hlc.ZeroTimestamp, nil
}

func executelocalValidateCommitAfter(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	lowerBound hlc.Timestamp,
	txn enginepb.TxnMeta,
) (hlc.Timestamp, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In executelocalValidateCommitAfter")
	}
	key := keys.TransactionKey(txn.Key, *txn.ID)

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return hlc.ZeroTimestamp, err
	} else if !ok {
		// should never be this case
		return hlc.ZeroTimestamp, roachpb.NewTransactionStatusError("does not exist")
	}
	// Update the Transaction record
	if txnRecord.Status == roachpb.COMMITTED {
		lowerBound.Forward(txnRecord.DynamicTimestampUpperBound)
	} else if txnRecord.Status == roachpb.PENDING {
		txnRecord.DynamicTimestampUpperBound.Backward(lowerBound)
	}
	// Save the updated Transaction record
	err := engine.MVCCPutProto(ctx, batch, nil, key, hlc.ZeroTimestamp, nil /* txn */, &txnRecord)

	return lowerBound, err
}

func manageCommitAfterQueue(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi :manageCommitAfterQueue ")
	}

	for _, txn := range mytxnRecord.CommitAfterThem {
		if err := validateCommitAfter(ctx, s, batch, txncache, mytxnRecord, txn); err != nil {
			return err
		}

	}
	return nil
}

func isTSIntact(
	lowerBound hlc.Timestamp,
	upperBound hlc.Timestamp,
) bool {
	if lowerBound.Less(upperBound) || lowerBound.Equal(upperBound) {
		return true
	}
	return false
}

func pickCommitTimeStamp(
	lowerBound hlc.Timestamp,
	upperBound hlc.Timestamp,
) hlc.Timestamp {

	if upperBound == hlc.MaxTimestamp || upperBound == lowerBound {
		return lowerBound
	}
	return lowerBound
}

func makeDecision(
	ctx context.Context,
	mytxnRecord *roachpb.Transaction,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In makeDecision")
	}
	if isTSIntact(mytxnRecord.DynamicTimestampLowerBound, mytxnRecord.DynamicTimestampUpperBound) {
		mytxnRecord.Status = roachpb.ABORTED

	} else {
		commitTimestamp := pickCommitTimeStamp(mytxnRecord.DynamicTimestampLowerBound, mytxnRecord.DynamicTimestampUpperBound)
		mytxnRecord.OrigTimestamp = commitTimestamp
		mytxnRecord.Status = roachpb.COMMITTED
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi : In decision %v", mytxnRecord.Status)
	}
	return nil
}

func executeLocalValidator(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
) (roachpb.Transaction, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In executeLocalValidator")
	}
	key := keys.TransactionKey(h.Txn.Key, *h.Txn.ID)

	txncache.getAccess(key)
	defer txncache.releaseAccess(key)

	var txnRecord roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txnRecord,
	); err != nil {
		return txnRecord, err
	} else if ok {
		if err := manageCommitBeforeQueue(ctx, s, batch, txncache, &txnRecord); err != nil {
			return txnRecord, err
		}
		if err := manageCommitAfterQueue(ctx, s, batch, txncache, &txnRecord); err != nil {
			return txnRecord, err
		}
		if err := makeDecision(ctx, &txnRecord); err != nil {
			return txnRecord, err
		}
	}
	return txnRecord, nil
}

func sendexecuteValidatorRPC(ctx context.Context,
	s *Store,
	h roachpb.Header,
) (roachpb.Transaction, error) {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendexecuteValidatorRPC")
	}
	var txnRecord roachpb.Transaction
	return txnRecord, nil
}

func executeValidator(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	txncache *TransactionRecordLockCache,
	h roachpb.Header,
) (roachpb.Transaction, error) {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In executeValidator")
	}

	if !transactionRecordExists(ctx, s, batch, h.Txn.Key, *h.Txn.ID) {
		// Transaction is in different range so making a RPC call
		return sendexecuteValidatorRPC(ctx, s, h)
	}

	return executeLocalValidator(ctx, s, batch, txncache, h)
}
