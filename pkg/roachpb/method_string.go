// Code generated by "stringer -type=Method"; DO NOT EDIT

package roachpb

import "fmt"

const _Method_name = "GetPutConditionalPutIncrementDeleteDeleteRangeScanReverseScanBeginTransactionEndTransactionAdminSplitAdminMergeAdminTransferLeaseHeartbeatTxnGCPushTxnRangeLookupResolveIntentResolveIntentRangeNoopMergeTruncateLogRequestLeaseTransferLeaseLeaseInfoComputeChecksumDeprecatedVerifyChecksumCheckConsistencyInitPutChangeFrozenUpdateTxnRecordResolveWriteSlockDyTsEndTransactionValidateCommitAfterValidateCommitBeforeGcWriteSoftLock"

var _Method_index = [...]uint16{0, 3, 6, 20, 29, 35, 46, 50, 61, 77, 91, 101, 111, 129, 141, 143, 150, 161, 174, 192, 196, 201, 212, 224, 237, 246, 261, 285, 301, 308, 320, 335, 351, 368, 386, 405, 419}

func (i Method) String() string {
	if i < 0 || i >= Method(len(_Method_index)-1) {
		return fmt.Sprintf("Method(%d)", i)
	}
	return _Method_name[_Method_index[i]:_Method_index[i+1]]
}
