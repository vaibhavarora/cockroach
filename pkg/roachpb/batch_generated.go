// Code generated by gen_batch.go; DO NOT EDIT

package roachpb

import (
	"bytes"
	"fmt"
)

type reqCounts [34]int32

// getReqCounts returns the number of times each
// request type appears in the batch.
func (ba *BatchRequest) getReqCounts() reqCounts {
	var counts reqCounts
	for _, r := range ba.Requests {
		switch {
		case r.Get != nil:
			counts[0]++
		case r.Put != nil:
			counts[1]++
		case r.ConditionalPut != nil:
			counts[2]++
		case r.Increment != nil:
			counts[3]++
		case r.Delete != nil:
			counts[4]++
		case r.DeleteRange != nil:
			counts[5]++
		case r.Scan != nil:
			counts[6]++
		case r.BeginTransaction != nil:
			counts[7]++
		case r.EndTransaction != nil:
			counts[8]++
		case r.AdminSplit != nil:
			counts[9]++
		case r.AdminMerge != nil:
			counts[10]++
		case r.AdminTransferLease != nil:
			counts[11]++
		case r.HeartbeatTxn != nil:
			counts[12]++
		case r.Gc != nil:
			counts[13]++
		case r.PushTxn != nil:
			counts[14]++
		case r.RangeLookup != nil:
			counts[15]++
		case r.ResolveIntent != nil:
			counts[16]++
		case r.ResolveIntentRange != nil:
			counts[17]++
		case r.Merge != nil:
			counts[18]++
		case r.TruncateLog != nil:
			counts[19]++
		case r.RequestLease != nil:
			counts[20]++
		case r.ReverseScan != nil:
			counts[21]++
		case r.ComputeChecksum != nil:
			counts[22]++
		case r.DeprecatedVerifyChecksum != nil:
			counts[23]++
		case r.CheckConsistency != nil:
			counts[24]++
		case r.Noop != nil:
			counts[25]++
		case r.InitPut != nil:
			counts[26]++
		case r.ChangeFrozen != nil:
			counts[27]++
		case r.TransferLease != nil:
			counts[28]++
		case r.LeaseInfo != nil:
			counts[29]++
		case r.UpdateTxnRecord != nil:
			counts[30]++
		case r.ResolveWriteSlock != nil:
			counts[31]++
		case r.DyTsEndTransaction != nil:
			counts[32]++
		case r.ValidateCommitAfter != nil:
			counts[33]++
		default:
			panic(fmt.Sprintf("unsupported request: %+v", r))
		}
	}
	return counts
}

var requestNames = []string{
	"Get",
	"Put",
	"CPut",
	"Inc",
	"Del",
	"DelRng",
	"Scan",
	"BeginTxn",
	"EndTxn",
	"AdmSplit",
	"AdmMerge",
	"AdmTransferLease",
	"HeartbeatTxn",
	"Gc",
	"PushTxn",
	"RngLookup",
	"ResolveIntent",
	"ResolveIntentRng",
	"Merge",
	"TruncLog",
	"RequestLease",
	"RevScan",
	"ComputeChksum",
	"DeprecatedVerifyChksum",
	"ChkConsistency",
	"Noop",
	"InitPut",
	"ChangeFrozen",
	"TransferLease",
	"LeaseInfo",
	"UpdateTxnRecord",
	"ResolveWriteSlock",
	"DyTsEndTransaction",
	"ValidateCommitAfter",
}

// Summary prints a short summary of the requests in a batch.
func (ba *BatchRequest) Summary() string {
	if len(ba.Requests) == 0 {
		return "empty batch"
	}
	counts := ba.getReqCounts()
	var buf bytes.Buffer
	for i, v := range counts {
		if v != 0 {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%d %s", v, requestNames[i])
		}
	}
	return buf.String()
}

// CreateReply creates replies for each of the contained requests, wrapped in a
// BatchResponse. The response objects are batch allocated to minimize
// allocation overhead.
func (ba *BatchRequest) CreateReply() *BatchResponse {
	br := &BatchResponse{}
	br.Responses = make([]ResponseUnion, len(ba.Requests))

	counts := ba.getReqCounts()

	var buf0 []GetResponse
	var buf1 []PutResponse
	var buf2 []ConditionalPutResponse
	var buf3 []IncrementResponse
	var buf4 []DeleteResponse
	var buf5 []DeleteRangeResponse
	var buf6 []ScanResponse
	var buf7 []BeginTransactionResponse
	var buf8 []EndTransactionResponse
	var buf9 []AdminSplitResponse
	var buf10 []AdminMergeResponse
	var buf11 []AdminTransferLeaseResponse
	var buf12 []HeartbeatTxnResponse
	var buf13 []GCResponse
	var buf14 []PushTxnResponse
	var buf15 []RangeLookupResponse
	var buf16 []ResolveIntentResponse
	var buf17 []ResolveIntentRangeResponse
	var buf18 []MergeResponse
	var buf19 []TruncateLogResponse
	var buf20 []RequestLeaseResponse
	var buf21 []ReverseScanResponse
	var buf22 []ComputeChecksumResponse
	var buf23 []DeprecatedVerifyChecksumResponse
	var buf24 []CheckConsistencyResponse
	var buf25 []NoopResponse
	var buf26 []InitPutResponse
	var buf27 []ChangeFrozenResponse
	var buf28 []RequestLeaseResponse
	var buf29 []LeaseInfoResponse
	var buf30 []UpdateTransactionRecordResponse
	var buf31 []ResolveWriteSoftLocksResponse
	var buf32 []DyTSEndTransactionResponse
	var buf33 []ValidateCommitAfterResponse

	for i, r := range ba.Requests {
		switch {
		case r.Get != nil:
			if buf0 == nil {
				buf0 = make([]GetResponse, counts[0])
			}
			br.Responses[i].Get = &buf0[0]
			buf0 = buf0[1:]
		case r.Put != nil:
			if buf1 == nil {
				buf1 = make([]PutResponse, counts[1])
			}
			br.Responses[i].Put = &buf1[0]
			buf1 = buf1[1:]
		case r.ConditionalPut != nil:
			if buf2 == nil {
				buf2 = make([]ConditionalPutResponse, counts[2])
			}
			br.Responses[i].ConditionalPut = &buf2[0]
			buf2 = buf2[1:]
		case r.Increment != nil:
			if buf3 == nil {
				buf3 = make([]IncrementResponse, counts[3])
			}
			br.Responses[i].Increment = &buf3[0]
			buf3 = buf3[1:]
		case r.Delete != nil:
			if buf4 == nil {
				buf4 = make([]DeleteResponse, counts[4])
			}
			br.Responses[i].Delete = &buf4[0]
			buf4 = buf4[1:]
		case r.DeleteRange != nil:
			if buf5 == nil {
				buf5 = make([]DeleteRangeResponse, counts[5])
			}
			br.Responses[i].DeleteRange = &buf5[0]
			buf5 = buf5[1:]
		case r.Scan != nil:
			if buf6 == nil {
				buf6 = make([]ScanResponse, counts[6])
			}
			br.Responses[i].Scan = &buf6[0]
			buf6 = buf6[1:]
		case r.BeginTransaction != nil:
			if buf7 == nil {
				buf7 = make([]BeginTransactionResponse, counts[7])
			}
			br.Responses[i].BeginTransaction = &buf7[0]
			buf7 = buf7[1:]
		case r.EndTransaction != nil:
			if buf8 == nil {
				buf8 = make([]EndTransactionResponse, counts[8])
			}
			br.Responses[i].EndTransaction = &buf8[0]
			buf8 = buf8[1:]
		case r.AdminSplit != nil:
			if buf9 == nil {
				buf9 = make([]AdminSplitResponse, counts[9])
			}
			br.Responses[i].AdminSplit = &buf9[0]
			buf9 = buf9[1:]
		case r.AdminMerge != nil:
			if buf10 == nil {
				buf10 = make([]AdminMergeResponse, counts[10])
			}
			br.Responses[i].AdminMerge = &buf10[0]
			buf10 = buf10[1:]
		case r.AdminTransferLease != nil:
			if buf11 == nil {
				buf11 = make([]AdminTransferLeaseResponse, counts[11])
			}
			br.Responses[i].AdminTransferLease = &buf11[0]
			buf11 = buf11[1:]
		case r.HeartbeatTxn != nil:
			if buf12 == nil {
				buf12 = make([]HeartbeatTxnResponse, counts[12])
			}
			br.Responses[i].HeartbeatTxn = &buf12[0]
			buf12 = buf12[1:]
		case r.Gc != nil:
			if buf13 == nil {
				buf13 = make([]GCResponse, counts[13])
			}
			br.Responses[i].Gc = &buf13[0]
			buf13 = buf13[1:]
		case r.PushTxn != nil:
			if buf14 == nil {
				buf14 = make([]PushTxnResponse, counts[14])
			}
			br.Responses[i].PushTxn = &buf14[0]
			buf14 = buf14[1:]
		case r.RangeLookup != nil:
			if buf15 == nil {
				buf15 = make([]RangeLookupResponse, counts[15])
			}
			br.Responses[i].RangeLookup = &buf15[0]
			buf15 = buf15[1:]
		case r.ResolveIntent != nil:
			if buf16 == nil {
				buf16 = make([]ResolveIntentResponse, counts[16])
			}
			br.Responses[i].ResolveIntent = &buf16[0]
			buf16 = buf16[1:]
		case r.ResolveIntentRange != nil:
			if buf17 == nil {
				buf17 = make([]ResolveIntentRangeResponse, counts[17])
			}
			br.Responses[i].ResolveIntentRange = &buf17[0]
			buf17 = buf17[1:]
		case r.Merge != nil:
			if buf18 == nil {
				buf18 = make([]MergeResponse, counts[18])
			}
			br.Responses[i].Merge = &buf18[0]
			buf18 = buf18[1:]
		case r.TruncateLog != nil:
			if buf19 == nil {
				buf19 = make([]TruncateLogResponse, counts[19])
			}
			br.Responses[i].TruncateLog = &buf19[0]
			buf19 = buf19[1:]
		case r.RequestLease != nil:
			if buf20 == nil {
				buf20 = make([]RequestLeaseResponse, counts[20])
			}
			br.Responses[i].RequestLease = &buf20[0]
			buf20 = buf20[1:]
		case r.ReverseScan != nil:
			if buf21 == nil {
				buf21 = make([]ReverseScanResponse, counts[21])
			}
			br.Responses[i].ReverseScan = &buf21[0]
			buf21 = buf21[1:]
		case r.ComputeChecksum != nil:
			if buf22 == nil {
				buf22 = make([]ComputeChecksumResponse, counts[22])
			}
			br.Responses[i].ComputeChecksum = &buf22[0]
			buf22 = buf22[1:]
		case r.DeprecatedVerifyChecksum != nil:
			if buf23 == nil {
				buf23 = make([]DeprecatedVerifyChecksumResponse, counts[23])
			}
			br.Responses[i].DeprecatedVerifyChecksum = &buf23[0]
			buf23 = buf23[1:]
		case r.CheckConsistency != nil:
			if buf24 == nil {
				buf24 = make([]CheckConsistencyResponse, counts[24])
			}
			br.Responses[i].CheckConsistency = &buf24[0]
			buf24 = buf24[1:]
		case r.Noop != nil:
			if buf25 == nil {
				buf25 = make([]NoopResponse, counts[25])
			}
			br.Responses[i].Noop = &buf25[0]
			buf25 = buf25[1:]
		case r.InitPut != nil:
			if buf26 == nil {
				buf26 = make([]InitPutResponse, counts[26])
			}
			br.Responses[i].InitPut = &buf26[0]
			buf26 = buf26[1:]
		case r.ChangeFrozen != nil:
			if buf27 == nil {
				buf27 = make([]ChangeFrozenResponse, counts[27])
			}
			br.Responses[i].ChangeFrozen = &buf27[0]
			buf27 = buf27[1:]
		case r.TransferLease != nil:
			if buf28 == nil {
				buf28 = make([]RequestLeaseResponse, counts[28])
			}
			br.Responses[i].RequestLease = &buf28[0]
			buf28 = buf28[1:]
		case r.LeaseInfo != nil:
			if buf29 == nil {
				buf29 = make([]LeaseInfoResponse, counts[29])
			}
			br.Responses[i].LeaseInfo = &buf29[0]
			buf29 = buf29[1:]
		case r.UpdateTxnRecord != nil:
			if buf30 == nil {
				buf30 = make([]UpdateTransactionRecordResponse, counts[30])
			}
			br.Responses[i].UpdateTxnRecord = &buf30[0]
			buf30 = buf30[1:]
		case r.ResolveWriteSlock != nil:
			if buf31 == nil {
				buf31 = make([]ResolveWriteSoftLocksResponse, counts[31])
			}
			br.Responses[i].ResolveWriteSlock = &buf31[0]
			buf31 = buf31[1:]
		case r.DyTsEndTransaction != nil:
			if buf32 == nil {
				buf32 = make([]DyTSEndTransactionResponse, counts[32])
			}
			br.Responses[i].DyTsEndTransaction = &buf32[0]
			buf32 = buf32[1:]
		case r.ValidateCommitAfter != nil:
			if buf33 == nil {
				buf33 = make([]ValidateCommitAfterResponse, counts[33])
			}
			br.Responses[i].ValidateCommitAfter = &buf33[0]
			buf33 = buf33[1:]
		default:
			panic(fmt.Sprintf("unsupported request: %+v", r))
		}
	}
	return br
}
