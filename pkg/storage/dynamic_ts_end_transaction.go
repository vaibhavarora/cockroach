package storage

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"golang.org/x/net/context"
)

func (r *Replica) ProcessDyTSEndTransaction(
	ctx context.Context,
	Args roachpb.Request,
	batch engine.ReadWriter,
	h roachpb.Header,
	resp roachpb.Response,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In EvalDyTSValidationRequestEndTransaction")
	}
	var err error

	args := Args.(*roachpb.EndTransactionRequest)
	reply := resp.(*roachpb.EndTransactionResponse)

	if err = sendDyTSEndTranactionRPC(ctx, r.store, batch, h, *reply.Txn, *args); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "Ravi : control back to EvalDyTSValidationRequestEndTransaction")
	}
	/*
		if err = sendOrigEndTransactionRPC(ctx, r.store, batch, h, *reply.Txn, Args); err != nil {
			return err
		}*/
	return nil
}

func sendDyTSEndTranactionRPC(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	txnrcd roachpb.Transaction,
	args roachpb.EndTransactionRequest,
) error {
	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendDyTSEndTranactionRPC")
	}

	dytsendtxnreq := &roachpb.DyTSEndTransactionRequest{
		Span: roachpb.Span{
			Key: txnrcd.Key,
		},
	}
	dytsendtxnreq.ReadSpans = append(dytsendtxnreq.ReadSpans, args.ReadSpans...)
	dytsendtxnreq.WriteSpans = append(dytsendtxnreq.WriteSpans, args.IntentSpans...)
	dytsendtxnreq.ReverseReadSpans = append(dytsendtxnreq.ReverseReadSpans, args.ReverseReadSpans...)
	dytsendtxnreq.Commit = txnrcd.Status == roachpb.COMMITTED
	dytsendtxnreq.Deadline = &txnrcd.OrigTimestamp
	dytsendtxnreq.InternalCommitTrigger = args.InternalCommitTrigger
	dytsendtxnreq.Tmeta = txnrcd.TxnMeta
	h.Txn = &txnrcd

	b := &client.Batch{}
	b.Header = h
	b.Header.Timestamp = hlc.ZeroTimestamp
	if log.V(2) {
		log.Infof(ctx, "Ravi : dytsendtxnreq %v", dytsendtxnreq)
	}
	b.AddRawRequest(dytsendtxnreq)

	if err := s.db.Run(ctx, b); err != nil {
		_ = b.MustPErr()
	} else {

		br := b.RawResponse()
		for _, res := range br.Responses {
			r := res.GetInner().(*roachpb.DyTSEndTransactionResponse)
			if log.V(2) {
				log.Infof(ctx, "DyTSEndTransactionRequest recieved response : %v", r)
			}
		}
	}
	return nil
}

func sendOrigEndTransactionRPC(
	ctx context.Context,
	s *Store,
	batch engine.ReadWriter,
	h roachpb.Header,
	txnrcd roachpb.Transaction,
	args roachpb.Request,
) error {

	if log.V(2) {
		log.Infof(ctx, "Ravi : In sendOrigEndTransactionRPC")
	}

	b := &client.Batch{}
	//b.Header = cArgs.Header
	//b.Header.Timestamp = hlc.ZeroTimestamp
	if log.V(2) {
		log.Infof(ctx, "Ravi : args %v", args)
	}
	b.AddRawRequest(args)

	if err := s.db.Run(ctx, b); err != nil {
		_ = b.MustPErr()
	} else {

		br := b.RawResponse()
		for _, res := range br.Responses {
			r := res.GetInner().(*roachpb.EndTransactionResponse)
			if log.V(2) {
				log.Infof(ctx, "DyTSEndTransactionRequest recieved response : %v", r)
			}
		}
	}
	return nil
}
