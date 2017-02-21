package main

import (
	"fmt"
)


const _Method_name = "GetPutConditionalPutIncrementDeleteDeleteRangeScanReverseScanBeginTransactionEndTransactionAdminSplitAdminMergeAdminTransferLeaseHeartbeatTxnGCPushTxnRangeLookupResolveIntentResolveIntentRangeNoopMergeTruncateLogRequestLeaseTransferLeaseLeaseInfoComputeChecksumDeprecatedVerifyChecksumCheckConsistencyInitPutChangeFrozenUpdateTxnRecord"

var _Method_index = [...]uint16{0, 3, 6, 20, 29, 35, 46, 50, 61, 77, 91, 101, 111, 129, 141, 143, 150, 161, 174, 192, 196, 201, 212, 224, 237, 246, 261, 285, 301, 308, 320, 335}

func String(i int) string {
    fmt.Println(_Method_index[i], _Method_index[i+1])
    return _Method_name[_Method_index[i]:_Method_index[i+1]]
}

func main() {
    fmt.Println(String(30))

}
