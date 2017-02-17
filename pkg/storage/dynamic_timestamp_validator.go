package storage

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
)

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
