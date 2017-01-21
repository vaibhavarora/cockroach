package stats

import (
	"time"
)

type Data struct {
	Concurrency, Success, Retries    int
	Contention                       string
	Avgread, Avgwrite                time.Duration
	Trxwithretries, Tnxwithoutreties time.Duration
	Transactionrate                  float64
}
