package instrumentation

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var totalParams = 11

// F_ for counting the function is called
// V_ for accumulating the value of a var
const (
	// dist_sender.go params
	F_DistSender_sendPartialBatch                       = "0"
	V_DistSender_sendPartialBatch_numRetriesTotal       = "1"
	V_DistSender_sendPartialBatch_returnedWithoutRetry  = "2"
	V_DistSender_sendPartialBatch_writeIntentErrorCount = "3"

	// store.go params
	F_Store_Send                              = "4"
	V_Store_Send_returnedWithoutRetry         = "5"
	V_Store_Send_numRetriesTotal              = "6"
	V_Store_Send_numRetriesWithoutBackoff     = "7"
	V_Store_Send_processWriteIntentErrorCount = "8"

	// replica.go params
	V_Replica_addReadOnlyCmd_executeBatchCount = "9"
	V_Replica_addReadOnlyCmd_intentFoundCount  = "10"
)

var dumpFile = "data.dump"
var ticker = time.NewTicker(2 * time.Second /* record interval */)
var data = make(map[string]int)
var mutex = &sync.Mutex{}

func Start() {
	fmt.Println("instrumentation started...")
	go func() {
		for t := range ticker.C {
			result := []string{strconv.Itoa(int(t.Unix()))}

			// Prepare result
			mutex.Lock()
			for i := 0; i < totalParams; i++ {
				key := strconv.Itoa(i)
				result = append(result, strconv.Itoa(data[key]))
			}

			// Reset map
			data = make(map[string]int)
			mutex.Unlock()

			// Dump to file
			f, _ := os.OpenFile(dumpFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			f.WriteString(strings.Join(result, ",") + "\n")
			f.Sync()
			f.Close()
		}
	}()
}

func Stop() {
	ticker.Stop()
	fmt.Println("ticker stopped")
}

func IncrementParam(param string, val int) {
	mutex.Lock()
	data[param] = data[param] + val
	mutex.Unlock()
}
