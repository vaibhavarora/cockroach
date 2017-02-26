package instrumentation

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var totalParams = 10

// F_ for counting the function is called
// V_ for accumulating the value of a var
const (
	// dist_sender.go params
	F_DistSender_sendToReplicas              = "0"
	F_DistSender_sendToAllReplicas           = "1"
	F_DistSender_sendPartialBatch            = "2"
	F_DistSender_sendRPC                     = "3"
	V_DistSender_sendPartialBatch_numTries   = "4"
	V_DistSender_sendRPC_specialRequestCount = "5"

	// store.go params
	F_Store_Send          = "6"
	V_Store_Send_numTries = "7"

	// replica.go params
	F_updateResponseWithIntentTimestamp       = "8"
	V_Replica_addReadOnlyCmd_intentFoundCount = "9"
)

var dumpFile = "data.dump"
var ticker = time.NewTicker(2 * time.Second /* record interval */)
var data = make(map[string]int)
var mutex = &sync.Mutex{}

func Start() {
	fmt.Println("ticker started")
	go func() {
		f, err := os.OpenFile(dumpFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			fmt.Println("Failed to open/create file for instrumentation")
			panic(err)
		}
		defer f.Close()

		for t := range ticker.C {
			mutex.Lock()
			// Prepare data
			var str string
			str += strconv.Itoa(int(t.Unix())) + ", "
			for i := 0; i < totalParams; i++ {
				key := strconv.Itoa(i)
				val := data[key]
				str += strconv.Itoa(val) + ", "
			}

			// Reset map
			data = make(map[string]int)
			mutex.Unlock()

			f.WriteString(str + "\n")
			f.Sync()
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
