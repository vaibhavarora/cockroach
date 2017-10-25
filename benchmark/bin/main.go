package main

import (
    "database/sql"
    "fmt"
    "log"
    "encoding/json" 
    "io/ioutil"
    "math/rand"
    "net/url"
    "os"
    "flag"
    "strconv"
    "strings"
    "sync/atomic"
    "time"
    "github.com/cockroachdb/cockroach-go/crdb"
    "github.com/cockroachdb/cockroach/benchmark/bin/constants"
    //"github.com/lib/pq"
)

///////////////////// Variable declarations /////////////////////
var CONFIG_FILE string = "../conf/conf.json"
var RESULTS_DIR string = "./benchmark_results/"
var MODE os.FileMode = 0777 
const EMPTY = ""

var txnCount int32
var successCount int32
var warmupcounts int32
var readRatio, writeRatio int
var readOnlyRatio, readWriteRatio int
var contentionPercent, contentiousData int
var start time.Time
var readcount, writecount int
var txnCompletionCheckInterval = flag.Duration("txn_completion", time.Second, "Interval to check if txns are complete.")

type configuration struct {
    NumItems int
    TableName string
    DBUrl string
    DBname string
    NumTransactions int
    Concurrency int
    ContentionRatio string
    ReportConcurrency bool
    Clearentries bool
    Warmuptnxs int
    ReadWriteRatio string
    ReadOnlyRatio string
    MaxWriteValue int
    OperationsPerTxn int
}

type measurement struct {
    read, write, totalTime, totalTimeWoRetries, commit int64
    retries                                      int32
}

type stats struct {
    Concurrency, Success, Retries    int
    Contention                       string
    Trxwithretries, Tnxwithoutreties time.Duration
    Transactionrate                  float64
}

var conf configuration
var db *sql.DB


///////////////////// Misc functions /////////////////////

func loadConfig() {
    fmt.Println(CONFIG_FILE)
    file, err := ioutil.ReadFile(CONFIG_FILE)
    errCheck(err)
    json.Unmarshal(file, &conf)
    rand.Seed(time.Now().Unix())
}


func errCheck(e error) {
    if e != nil {
        log.Fatal("error: ", e)
    }
}


func random(min, max int) int {
    return rand.Intn(max-min) + min
}

func txnsComplete() bool {
    /* Check is enough successful transactions have been performed */
    return conf.NumTransactions > 0 && atomic.LoadInt32(&successCount) >= int32(conf.NumTransactions)
}

///////////////////// SQL Query construction helpers /////////////////////

func constructInsertStatement(dbKeyValue string) ( insertSQL string) {
    /* Construct insert SQL statement based on the key-value passed */
    insertSQL += constants.INSERT + constants.INTO + conf.TableName + dbKeyValue
    return insertSQL
}


func constructUpdateStatement(dbKeyValue, condition string) (updateSQL string) {
    /* Construct update SQL statement based on the key-value passed */

    updateSQL += constants.UPDATE + conf.TableName + constants.SET 
    updateSQL += dbKeyValue + constants.WHERE + condition
    return updateSQL
}


func constructSelectStatement(dbKeyValue, condition string) ( selectSQL string) {
    /* Construct select SQL statement based on the key-value passed
    If not key is passed, select all from that table */

    if(dbKeyValue != "" && condition != ""){
        selectSQL += constants.SELECT + dbKeyValue + constants.FROM + conf.TableName 
        selectSQL += constants.WHERE + condition
    } else if(condition != "") {
        selectSQL += constants.SELECT + constants.ALL_OPERATOR + constants.FROM + conf.TableName 
        selectSQL += constants.WHERE + condition
    } else {
        selectSQL += constants.SELECT + constants.ALL_OPERATOR + constants.FROM + conf.TableName
    }
    return selectSQL
}


///////////////////// Initial operation helpers /////////////////////


func createDBConnection( ) (db *sql.DB) {
    /* Load DB URL specified in conf file; connect to DB.
    Set maximum connections and Idle Connections based on config.
    Return the DB connection to caller */

    parsedURL, err := url.Parse(conf.DBUrl)
    if err != nil {
        log.Fatal(err)
    }
    parsedURL.Path = conf.DBname

    db, err = sql.Open("postgres", parsedURL.String())
    errCheck(err)
    if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + conf.DBname); err != nil {
        log.Fatal(err)
    }
    log.Println("Database created.")

    db.SetMaxOpenConns(conf.Concurrency + 1)
    db.SetMaxIdleConns(conf.Concurrency + 1)
    return db
}


func addEntriesToTable() {
    /* Check if table and entries exist; based on config clear old entries 
    and add new entries to the table */

    if _, err := db.Exec(
        "CREATE TABLE IF NOT EXISTS " + conf.TableName + " (id INT PRIMARY KEY, value INT)"); err != nil {
        log.Fatal(err)
    }

    EntriesExists := false
    /* Perform SELECT * to verify if there are any entries in the table */
    selectSQL := constructSelectStatement(EMPTY, EMPTY)
    rows, _ := db.Query(selectSQL)
    defer rows.Close()
    if (rows != nil && rows.Next()){
        log.Println("There are entries in table.")
        EntriesExists = true
    }
    
    if conf.Clearentries {
        log.Println("Clearing the database.")
        if _, err := db.Exec("TRUNCATE TABLE " + conf.TableName); err != nil {
            log.Fatal(err)
        }
        EntriesExists = false
    }

    if EntriesExists == false {

        log.Println("Inserting entries")
        for i := 1; i <= conf.NumItems; i++ {

            /* Choose some random value to write */
            val := random(1, conf.MaxWriteValue)

            dbKeyValue := " (id, value) " + constants.VALUES + " (" + strconv.Itoa(i) + "," + strconv.Itoa(val) + ")" 
            insertSQL := constructInsertStatement(dbKeyValue)
            if _, err := db.Exec(insertSQL); err != nil {
                log.Fatal(err)
            }
        }
    }
}


func doWarmUpTxns(db *sql.DB) {
    /* Reads to warm up the database cache( if there is any) to elemitate 
    the effect of cache on bechmark */

    for atomic.LoadInt32(&warmupcounts) <= int32(conf.Warmuptnxs) {
   
        key1 := random(1, conf.NumItems)
        key2 := random(1, conf.NumItems)
        for key1 == key2 {
            key2 = random(1, conf.NumItems)
        }
        if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
            rows, err := tx.Query(`SELECT id, value FROM `+ conf.TableName + ` WHERE id IN ($1, $2)`, key1, key2)
            if err != nil {
                log.Println("read error %v , tnx %v", err, tx)
                return err
            }

            for rows.Next() {
                var id, value int
                if err = rows.Scan(&id, &value); err != nil {
                    log.Println("Error: ")
                    log.Fatal(err)
                }
            }
            return nil
        }); err != nil {
            log.Println("  failed transaction: %v", err)
            continue
        }
        atomic.AddInt32(&warmupcounts, 1)
    }

}

func performWarmUp() {
    /* Based on config, perform Warmuptnxs * 2 (2 reads) number of reads
    to fill up read cache. Perform the reads concurrently */

    if conf.Warmuptnxs > 0 {
        log.Println("Performing warm up reads")
        log.Println("warm up txns ", conf.Warmuptnxs)
        for i := 0; i < conf.Concurrency; i++ {
            go doWarmUpTxns(db)
        }
    }

    if conf.Warmuptnxs > 0 {
        for atomic.LoadInt32(&warmupcounts) <= int32(conf.Warmuptnxs) {
            /* Waiting for warming up to finish */
            time.Sleep(time.Second)
        }
        log.Println("Done with warm up reads : ", atomic.LoadInt32(&warmupcounts))
    }
}


func performTransactions(db *sql.DB, aggr *measurement) {

    for !txnsComplete() {
        var readDuration, writeDuration time.Duration
        start := time.Now()
        startTransaction := time.Now()
        attempts := 0
        var commitDuration int64

        /* Set how many operations should be performed in every transaction */
        totalOps := conf.OperationsPerTxn

        /* Choose randomly if this trasaction will be a read-only txn or
        read-write txn */
        readOnly := false
        if random(1, 100) <= readOnlyRatio {
            readOnly = true
        } 

        if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
            var readTime, writeTime time.Duration

            attempts++
            if attempts > 1 {
                atomic.AddInt32(&aggr.retries, 1)
                startTransaction = time.Now()
            }

            for i := 0; i < totalOps; i++ {
                /* Based on random choice, decide if the operation should be read or write */
                // readOp := false

                // randNum := random(1, 100)
                // if randNum <= readRatio {
                //     readOp = true
                // }

                /* Based on the contention ratio, choose what key to use for this operation.
                If contention ratio is 90:10 ==> 90% of ops work on 10% of data. */
                var id int
                dataItems := int(float64(contentiousData)/100 * float64(conf.NumItems))
                if random(1, 100) <= contentionPercent {
                    id = random(1, dataItems)
                } else {
                    /* Choose id from the less contentious data */
                    id = random(dataItems+1, conf.NumItems)
                }

                if (readOnly || i < 3) {
                    startRead := time.Now()
                    condition :=  "id=" + strconv.Itoa(id)
                    selectSQL := constructSelectStatement("id, value ", condition)
                    rows, err := tx.Query(selectSQL)
                    if err != nil {
                        fmt.Println(err)
                        return err
                    }
                    readTime += time.Since(startRead)
                    for rows.Next() {
                        var id, value int
                        if err := rows.Scan(&id, &value); err != nil {
                            log.Printf("There is an error")
                            log.Fatal(err)
                        }
                    }
                    readcount += 1
                    
                } else {
                    startWrite := time.Now()
                    value := random(1, conf.MaxWriteValue)

                    dbValue := "value=" + strconv.Itoa(value)
                    dbCondition := "id=" + strconv.Itoa(id)

                    updateSQL := constructUpdateStatement(dbValue, dbCondition)
                    
                    if _, err := tx.Exec(updateSQL); err != nil {
                        return err
                    }
                    writeTime += time.Since(startWrite)
                    writecount += 1
                    
                }
                readDuration, writeDuration = readTime, writeTime
            }
            return nil
        }); err != nil {
            fmt.Printf("failed transaction: %v", err)
            continue
        } else {
            atomic.AddInt64(&commitDuration, time.Since(startTransaction).Nanoseconds())
        }
            
        atomic.AddInt32(&successCount, 1)
        atomic.AddInt64(&aggr.read, readDuration.Nanoseconds())
        atomic.AddInt64(&aggr.write, writeDuration.Nanoseconds())
        atomic.AddInt64(&aggr.commit, commitDuration)
        atomic.AddInt64(&aggr.totalTime, time.Since(start).Nanoseconds())
        atomic.AddInt64(&aggr.totalTimeWoRetries, time.Since(startTransaction).Nanoseconds())
    }
}


func runTest() {
    ratios := strings.Split(conf.ReadWriteRatio, ":")
    readRatio, _ = strconv.Atoi(ratios[0])
    writeRatio, _ = strconv.Atoi(ratios[1])

    ratios = strings.Split(conf.ReadOnlyRatio, ":")
    readOnlyRatio, _ = strconv.Atoi(ratios[0])
    readWriteRatio, _ = strconv.Atoi(ratios[1])

    ratios = strings.Split(conf.ContentionRatio, ":")
    contentionPercent, _ = strconv.Atoi(ratios[0])
    contentiousData, _ = strconv.Atoi(ratios[1])

    var aggr measurement
    
    for i := 0; i < conf.Concurrency; i++ {
        go performTransactions(db, &aggr)
    }
    start := time.Now()
    
    totalTestTime := time.Duration(0)
    for range time.NewTicker(*txnCompletionCheckInterval).C {
        /* Wait till all trasactions complete */
        if txnsComplete(){
            break
        }
    }
    totalTestTime = time.Since(start)

    successes := atomic.LoadInt32(&successCount)
    d := time.Duration(successes)
    totalTime := time.Duration(atomic.LoadInt64(&aggr.totalTime))
    totalTimeWoRetries := time.Duration(atomic.LoadInt64(&aggr.totalTimeWoRetries))

    txnRate := float64(successes)/totalTestTime.Seconds()

    log.Println("totalTimeWithRetries = " + strconv.FormatFloat(totalTestTime.Seconds(), 'f', -1, 64))
    log.Println("RC: " + strconv.Itoa(readcount) + "  WC: " + strconv.Itoa(writecount) )

    logMsg := "\nContention=" + conf.ContentionRatio
    logMsg += ":readOnlyRatio=" + conf.ReadOnlyRatio
    logMsg += ":readWriteRatio=" + conf.ReadWriteRatio
    logMsg += ":Concurrency=" + strconv.Itoa(conf.Concurrency)
    logMsg += ":Transaction rate=" + strconv.FormatFloat(txnRate, 'f', -1, 64)
    logMsg += ":Total Success=" + strconv.Itoa(int(successCount))
    logMsg += ":Total Retries=" + strconv.Itoa(int(aggr.retries))
    logMsg += ":Avg time(w/o retires)="+time.Duration( totalTimeWoRetries/ d).String()
    logMsg += ":Avg time (w/ retries)=" + time.Duration(totalTime / d).String()

    log.Println(logMsg)
    /* Log the results of the test */
    logResults(logMsg)
}



func logResults(logMsg string) {
    parts := strings.Split(CONFIG_FILE, ".")
    varyingFactor := parts[len(parts)-2]
    
    path := RESULTS_DIR + varyingFactor + "/"
    if _, err := os.Stat(path); err != nil {
        if os.IsNotExist(err) {
            os.MkdirAll(path, os.ModePerm)
        }
    }

    filePath := path + varyingFactor + ".log"
    if _, err := os.Stat(filePath); err != nil {
        if os.IsNotExist(err) {
            _, err := os.Create(filePath)
            errCheck(err)
        }
        
    }

    f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
    if err != nil {
        panic(err)
    }
    defer f.Close()

    if _, err = f.WriteString(logMsg); err != nil {
        panic(err)
    }
    
}
///////////////////// Main function /////////////////////

func main() {

    if (len(os.Args[1]) > 1) {
        CONFIG_FILE = os.Args[1]
    }

    /* Load the configuration into struct variable conf */
    loadConfig()
    /* Connect to DB and use it to perform all DB operations */
    db = createDBConnection()
    
    /* Fill the DB with some entries */
    addEntriesToTable()
    
    /*Perform some warm up reads to fill read cache */
    performWarmUp()

    /* Run the benchmarking tests */
    runTest()

}