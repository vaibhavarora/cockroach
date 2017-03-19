// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/cockroachdb/cockroach/benchmark/pkg/stats"
	"log"
	"math/rand"
	"net/rpc"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	// Import postgres driver.
	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

const systemAccountID = 0
const initialBalance = 1000

var maxTransfer = flag.Int("max-transfer", 100, "Maximum amount to transfer in one transaction.")
var numTransfers = flag.Int("num-transfers", 100000, "Number of transfers (0 to continue indefinitely).")
var numAccounts = flag.Int("num-accounts", 100000, "Number of accounts.")
var concurrency = flag.Int("concurrency", 50, "Number of concurrent actors moving money.")
var contention = flag.String("contention", "low", "Contention model {low | high}.")
var balanceCheckInterval = flag.Duration("balance-check-interval", time.Second, "Interval of balance check.")
var contentionratio = flag.String("contention-ratio", "50:50", "AccountPercentage:Contention percentage")
var reportConcurrency = flag.Bool("report-concurrency", false, "{ true | false }")
var clearentries = flag.Bool("new-entries", false, "{ true | false }")
var warmuptnxs = flag.Int("warm-up-tnx", 0, "Number of Transactions(2 reads each) for warming up")

var txnCount int32
var successCount int32
var initialSystemBalance int

var contentionAccounts int
var contentionPercentage int

var warmupcounts int32

type measurement struct {
	read, write, total, totalWithRetries, commit int64
	retries                                      int32
	//aborts             int32
}

func transfersComplete() bool {
	return *numTransfers > 0 && atomic.LoadInt32(&successCount) >= int32(*numTransfers)
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func getAccount() int {

	dice := random(1, 100)
	if dice <= contentionPercentage {
		return random(1, contentionAccounts)
	} else {
		return random(contentionAccounts, *numAccounts)
	}
}

// Reads to warm up the database cache( if there is any) to elemitate the effect of cache on bechmark
func do_warm_up_tnxs(db *sql.DB) {

	for atomic.LoadInt32(&warmupcounts) <= int32(*warmuptnxs) {

		//log.Printf("Performing Warm up reads count %v", atomic.LoadInt32(&warmupcounts))
		account1 := random(1, *numAccounts)
		account2 := random(1, *numAccounts)
		for account1 == account2 {
			account2 = random(1, *numAccounts)
		}
		//log.Printf("account1 : %v, account2 : %v", account1, account2)
		if err, _ := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			rows, err := tx.Query(`SELECT id, balance FROM account WHERE id IN ($1, $2)`, account1, account2)
			if err != nil {
				log.Printf("read error %v , tnx %v", err, tx)
				//atomic.AddInt32(&aggr.aborts, 1)
				return err
			}
			for rows.Next() {
				var id, balance int
				if err = rows.Scan(&id, &balance); err != nil {
					log.Printf("here is the error")
					log.Fatal(err)
				}
				switch id {
				case account1:
					//log.Printf("account1 balance : %v", balance)
				case account2:
					//log.Printf("account2 balance : %v", balance)
				default:
					panic(fmt.Sprintf("got unexpected account %d"))
				}
			}
			return nil
			// we dont bother with the content of the response
		}); err != nil {
			log.Printf("  failed transaction: %v", err)

			continue
		}
		atomic.AddInt32(&warmupcounts, 1)
	}
	//fmt.Printf("Done with Warm up reads")

}

func randomMoney(db *sql.DB, aggr *measurement) {
	//log.Printf("In movemoney")
	useSystemAccount := *contention == "high"
	for !transfersComplete() {
		var readDuration, writeDuration time.Duration
		var fromBalance, toBalance int
		from := getAccount()
		to := getAccount()
		//from, to := rand.Intn(*numAccounts)+1, rand.Intn(*numAccounts)+1
		//log.Printf("from %v to %v", from, to)
		if from == to {
			continue
		}
		if useSystemAccount {
			// Use the first account number we generated as a coin flip to
			// determine whether we're transferring money into or out of
			// the system account.
			if from > *numAccounts/2 {
				from = systemAccountID
			} else {
				to = systemAccountID
			}
		}
		//amount := rand.Intn(*maxTransfer)
		start := time.Now()
		startTransaction := time.Now()
		attempts := 0
		var commitDuration int64

		if err, committimetaken := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			attempts++

			if attempts > 1 {
				//log.Printf("retry attempt %d for tnx %v", attempts, tx)
				atomic.AddInt32(&aggr.retries, 1)
				startTransaction = time.Now()
			}

			startRead := time.Now()
			rows1, err := tx.Query(`SELECT id, balance FROM account WHERE id IN ($1)`, from)
			if err != nil {
				//log.Printf("read error %v , tnx %v", err, tx)
				//atomic.AddInt32(&aggr.aborts, 1)
				return err
			}
			readDuration = time.Since(startRead)
			for rows1.Next() {
				var id, balance int
				if err = rows1.Scan(&id, &balance); err != nil {
					log.Printf("here is th error")
					log.Fatal(err)
				}
				fromBalance = balance
			}
			dice := random(0, 100)
			if dice > 50 {
				return nil
			}
			startRead = time.Now()
			rows2, err := tx.Query(`SELECT id, balance FROM account WHERE id IN ($1)`, to)
			if err != nil {
				//log.Printf("read error %v , tnx %v", err, tx)
				//atomic.AddInt32(&aggr.aborts, 1)
				return err
			}
			readDuration += time.Since(startRead)
			for rows2.Next() {
				var id, balance int
				if err = rows2.Scan(&id, &balance); err != nil {
					log.Printf("here is th error")
					log.Fatal(err)
				}
				fromBalance = balance
				toBalance = balance
			}

			startWrite := time.Now()

			update := `UPDATE account SET balance = $1 WHERE id = $2;`
			if _, err = tx.Exec(update, toBalance, to); err != nil {
				//atomic.AddInt32(&aggr.aborts, 1)
				//log.Printf("write error %v, tnx %v", err, tx)
				return err
			}
			writeDuration = time.Since(startWrite)
			startWrite = time.Now()
			if _, err = tx.Exec(update, fromBalance, from); err != nil {
				//atomic.AddInt32(&aggr.aborts, 1)
				//log.Printf("write error %v, tnx %v", err, tx)
				return err
			}
			writeDuration += time.Since(startWrite)
			return nil
		}); err != nil {
			log.Printf("failed transaction: %v", err)

			continue
		} else {
			atomic.AddInt64(&commitDuration, committimetaken)
		}
		atomic.AddInt32(&successCount, 1)

		atomic.AddInt64(&aggr.read, readDuration.Nanoseconds())
		atomic.AddInt64(&aggr.write, writeDuration.Nanoseconds())
		atomic.AddInt64(&aggr.commit, commitDuration)
		atomic.AddInt64(&aggr.totalWithRetries, time.Since(start).Nanoseconds())
		atomic.AddInt64(&aggr.total, time.Since(startTransaction).Nanoseconds())

	}
}

func moveMoney(db *sql.DB, aggr *measurement) {
	//log.Printf("In movemoney")
	useSystemAccount := *contention == "high"
	for !transfersComplete() {
		var readDuration, writeDuration time.Duration
		var fromBalance, toBalance int
		from := getAccount()
		to := getAccount()
		//from, to := rand.Intn(*numAccounts)+1, rand.Intn(*numAccounts)+1
		//log.Printf("from %v to %v", from, to)
		if from == to {
			continue
		}
		if useSystemAccount {
			// Use the first account number we generated as a coin flip to
			// determine whether we're transferring money into or out of
			// the system account.
			if from > *numAccounts/2 {
				from = systemAccountID
			} else {
				to = systemAccountID
			}
		}
		amount := rand.Intn(*maxTransfer)
		start := time.Now()
		startTransaction := time.Now()
		attempts := 0
		var commitDuration int64

		if err, committimetaken := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			attempts++

			if attempts > 1 {
				//log.Printf("retry attempt %d for tnx %v", attempts, tx)
				atomic.AddInt32(&aggr.retries, 1)
				startTransaction = time.Now()
			}

			startRead := time.Now()
			rows, err := tx.Query(`SELECT id, balance FROM account WHERE id IN ($1, $2)`, from, to)
			if err != nil {
				//log.Printf("read error %v , tnx %v", err, tx)
				//atomic.AddInt32(&aggr.aborts, 1)
				return err
			}
			readDuration = time.Since(startRead)
			for rows.Next() {
				var id, balance int
				if err = rows.Scan(&id, &balance); err != nil {
					log.Printf("here is th error")
					log.Fatal(err)
				}
				switch id {
				case from:
					fromBalance = balance
				case to:
					toBalance = balance
				default:
					panic(fmt.Sprintf("got unexpected account %d", id))
				}
			}
			startWrite := time.Now()
			if fromBalance < amount {
				return nil
			}

			update := `UPDATE account SET balance = $1 WHERE id = $2;`
			if _, err = tx.Exec(update, toBalance+amount, to); err != nil {
				//atomic.AddInt32(&aggr.aborts, 1)
				//log.Printf("write error %v, tnx %v", err, tx)
				return err
			}
			if _, err = tx.Exec(update, fromBalance-amount, from); err != nil {
				//atomic.AddInt32(&aggr.aborts, 1)
				//log.Printf("write error %v, tnx %v", err, tx)
				return err
			}
			writeDuration = time.Since(startWrite)
			return nil
		}); err != nil {
			log.Printf("failed transaction: %v", err)

			continue
		} else {
			atomic.AddInt64(&commitDuration, committimetaken)
		}
		atomic.AddInt32(&successCount, 1)
		if fromBalance >= amount {
			atomic.AddInt64(&aggr.read, readDuration.Nanoseconds())
			atomic.AddInt64(&aggr.write, writeDuration.Nanoseconds())
			atomic.AddInt64(&aggr.commit, commitDuration)
			atomic.AddInt64(&aggr.totalWithRetries, time.Since(start).Nanoseconds())
			atomic.AddInt64(&aggr.total, time.Since(startTransaction).Nanoseconds())
		}
	}
}

func verifyTotalBalance(db *sql.DB) {
	var sum int
	if err := db.QueryRow("SELECT SUM(balance) FROM account").Scan(&sum); err != nil {
		log.Fatal(err)
	}
	if sum != *numAccounts*initialBalance+initialSystemBalance {
		log.Printf("The total balance is incorrect: %d.", sum)
		os.Exit(1)
	}
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	//dbURL := "postgresql://root@localhost:26257/bank2?sslmode=disable"
	//dbURL := "postgresql://root@ip-172-31-4-97:26257?sslmode=disable"
	dbURL := "postgresql://root@ip-172-31-15-117:26257?sslmode=disable"
	//dbURL := "postgresql://root@gediz:26257/bank2?sslmode=disable"
	//dbURL := "postgresql://root@pacific:26257?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	parsedURL.Path = "bank2"

	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("connected to db")
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS bank2"); err != nil {
		log.Fatal(err)
	}
	log.Printf("database created")
	// concurrency + 1, for this thread and the "concurrency" number of
	// goroutines that move money
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if _, err = db.Exec(`
CREATE TABLE IF NOT EXISTS account (
  id INT,
  balance INT NOT NULL,
  
  PRIMARY KEY (id)
); 

`); err != nil {
		log.Fatal(err)
	}
	log.Printf("table created")
	EntriesExists := false
	// Check if the entries in table exists
	balance := -1
	err = db.QueryRow("select balance from account where id = $1", 1).Scan(&balance)
	if err != nil {
		log.Printf("Emtpy table")
	}
	if balance != -1 {
		log.Printf("There are entries in table")
		EntriesExists = true
	}
	log.Printf("check for table entries done")
	if *clearentries {
		log.Printf("clearing db")
		if _, err = db.Exec("TRUNCATE TABLE account"); err != nil {
			log.Fatal(err)
		}
		EntriesExists = false
	}

	// Insert initialSystemBalance into the system account.
	initialSystemBalance = *numAccounts * initialBalance

	if EntriesExists == false {
		log.Printf("Inserting entries")
		insertSQL := "INSERT INTO account (id, balance) VALUES ($1, $2)"

		if _, err = db.Exec(insertSQL, systemAccountID, initialSystemBalance); err != nil {
			log.Fatal(err)
		}
		// Insert initialBalance into all user accounts.
		for i := 1; i <= *numAccounts; i++ {
			if _, err = db.Exec(insertSQL, i, initialBalance); err != nil {
				log.Fatal(err)
			}
		}

	}
	log.Printf("done Inserting ")
	verifyTotalBalance(db)
	contentioninfo := strings.Split(*contentionratio, ":")
	accountpercent, err := strconv.Atoi(contentioninfo[0])

	if err != nil {
		log.Fatal(err)
	}
	contentionAccounts = int((float64(accountpercent) / 100) * float64(*numAccounts))

	contentionPer, err := strconv.Atoi(contentioninfo[1])
	if err != nil {
		log.Fatal(err)
	}
	contentionPercentage = contentionPer

	if *warmuptnxs > 0 {
		log.Printf("Performing warm up reads")
		log.Printf("warm up txns %v", *warmuptnxs)
		for i := 0; i < *concurrency; i++ {
			go do_warm_up_tnxs(db)
		}

	}

	if *warmuptnxs > 0 {
		for atomic.LoadInt32(&warmupcounts) <= int32(*warmuptnxs) {

			time.Sleep(5 * time.Second)
			// waiting for warming up to finish
		}
		log.Printf("Done with warm up reads : %v", atomic.LoadInt32(&warmupcounts))
	}

	verifyTotalBalance(db)

	var aggr measurement
	//var lastSuccesses int32
	for i := 0; i < *concurrency; i++ {
		go randomMoney(db, &aggr)
	}

	start := time.Now()
	lastTime := start
	//lastretries := time.Duration(0)
	totaltime := time.Duration(0)
	for range time.NewTicker(*balanceCheckInterval).C {
		/*now := time.Now()
		elapsed := now.Sub(lastTime)
		lastTime = now
		totaltime += elapsed
		successes := atomic.LoadInt32(&successCount)
		newSuccesses := (successes - lastSuccesses)
		log.Printf("%d transfers were executed in last 1 s", newSuccesses)
		log.Printf("Average rate of transactions %.1f/s", float64(successes)/totaltime.Seconds())
		lastSuccesses = successes

		d := time.Duration(successes)
		read := time.Duration(atomic.LoadInt64(&aggr.read))
		write := time.Duration(atomic.LoadInt64(&aggr.write))
		totalWithRetries := time.Duration(atomic.LoadInt64(&aggr.totalWithRetries))
		total := time.Duration(atomic.LoadInt64(&aggr.total))
		//aborts := time.Duration(atomic.LoadInt32(&aggr.aborts))
		retries := time.Duration(atomic.LoadInt32(&aggr.retries))
		commit := time.Duration(atomic.LoadInt64(&aggr.commit))
		log.Printf("Average time taken for read: %v", read/d)
		log.Printf("Average time taken for write: %v", write/d)
		log.Printf("Average time taken for commit: %v", commit/d)
		log.Printf("Average time taken for a transaction(Including retries): %v", totalWithRetries/d)
		log.Printf("Average time taken for a transaction(Excluding retries): %v", total/d)
		log.Printf("Retries / succesful Transactions in last %v = %d / %d", elapsed.Seconds(), (retries - lastretries), newSuccesses)
		log.Printf("Total Retries / Total Succesful Transactions = %d / %d ", retries, successes)

		lastretries = retries

		verifyTotalBalance(db) */
		if transfersComplete() {
			break
		}
	}
	//log.Printf("completed %d transfers in %s with %d retries", atomic.LoadInt32(&successCount),
	//	time.Since(start), atomic.LoadInt32(&aggr.retries))

	now := time.Now()
	elapsed := now.Sub(lastTime)
	totaltime += elapsed

	if *reportConcurrency {
		client, err := rpc.Dial("tcp", "localhost:42586")
		if err != nil {
			log.Fatal(err)
		}
		successes := atomic.LoadInt32(&successCount)
		d := time.Duration(successes)
		read := time.Duration(atomic.LoadInt64(&aggr.read))
		write := time.Duration(atomic.LoadInt64(&aggr.write))
		totalWithRetries := time.Duration(atomic.LoadInt64(&aggr.totalWithRetries))
		total := time.Duration(atomic.LoadInt64(&aggr.total))

		stat := &stats.Data{*concurrency, int(atomic.LoadInt32(&successCount)), int(atomic.LoadInt32(&aggr.retries)), *contentionratio, time.Duration(read / d), time.Duration(write / d), time.Duration(totalWithRetries / d), time.Duration(total / d), float64(atomic.LoadInt32(&successCount)) / totaltime.Seconds()}

		var reply bool
		err = client.Call("Listener.CollectStats", stat, &reply)
		if err != nil {
			log.Fatal(err)
		}
	}
}
