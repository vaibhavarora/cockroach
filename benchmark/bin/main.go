package main

import (
    "database/sql"
    "fmt"
    "log"
    "encoding/json" 
    "io/ioutil"
    "math/rand"
    //"net/rpc"
    "net/url"
    //"os"
    "strconv"
    //"strings"
    "sync/atomic"
    "time"
    "github.com/cockroachdb/cockroach-go/crdb"
    "github.com/cockroachdb/cockroach/benchmark/bin/constants"
    _ "github.com/lib/pq"
)

///////////////////// Variable declarations /////////////////////
const CONFIG_FILE="../conf/conf.json"
const SELECT_ALL = ""

var warmupcounts int32

type configuration struct {
    NumItems int
    TableName string
    DBUrl string
    DBname string
    NumTransactions int
    Concurrency int
    Contention string
    Contentionratio string
    ReportConcurrency bool
    Clearentries bool
    Warmuptnxs int
}

type measurement struct {
    read, write, total, totalWithRetries, commit int64
    retries                                      int32
}

var conf configuration
var db *sql.DB


///////////////////// Misc functions /////////////////////

func loadConfig() {
    
    file, err := ioutil.ReadFile(CONFIG_FILE)
    errCheck(err)
    json.Unmarshal(file, &conf)
    
}


func errCheck(e error) {
    if e != nil {
        log.Fatal("error: ", e)
    }
}


func random(min, max int) int {
    return rand.Intn(max-min) + min
}

///////////////////// SQL Query construction helpers /////////////////////

func constructInsertStatement(dbKeyValue string) ( insertSQL string) {
    //Construct insert SQL statement based on the key-value passed
    insertSQL += constants.INSERT + constants.INTO + conf.TableName + dbKeyValue
    return insertSQL
}


func constructSelectStatement(dbKeyValue string) ( selectSQL string) {
    /*Construct select SQL statement based on the key-value passed
    If not key is passed, select all from that table*/

    if(len(dbKeyValue) > 0){
        selectSQL += constants.SELECT + dbKeyValue + constants.FROM + conf.TableName
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
    log.Printf("Database created.")

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
    selectSQL := constructSelectStatement(SELECT_ALL)
    rows, _ := db.Query(selectSQL)
    
    if rows != nil {
        log.Printf("There are entries in table.")
        EntriesExists = true
    }
    
    if conf.Clearentries {
        log.Printf("Clearing the database.")
        if _, err := db.Exec("TRUNCATE TABLE " + conf.TableName); err != nil {
            log.Fatal(err)
        }
        EntriesExists = false
    }

    if EntriesExists == false {

        log.Printf("Inserting entries")
        for i := 1; i <= conf.NumItems; i++ {

            /* Choose some random value to write */
            val := random(1, constants.MAX_WRITE_VALUE)

            dbKeyValue := " (id, value) " + constants.VALUES + " (" + strconv.Itoa(i) + "," + strconv.Itoa(val) + ")" 
            insertSQL := constructInsertStatement(dbKeyValue)
            fmt.Printf(insertSQL)
            if _, err := db.Exec(insertSQL); err != nil {
                log.Fatal(err)
            }

        }
    }

}


func do_warm_up_tnxs(db *sql.DB) {
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
                log.Printf("read error %v , tnx %v", err, tx)
                return err
            }

            for rows.Next() {
                var id, value int
                if err = rows.Scan(&id, &value); err != nil {
                    log.Printf("Error: ")
                    log.Fatal(err)
                }

                //fmt.Printf("ID: " + strconv.Itoa(id) + " Value: " + strconv.Itoa(value))
            }
            return nil
            // we dont bother with the content of the response
        }); err != nil {
            log.Printf("  failed transaction: %v", err)
            continue
        }
        atomic.AddInt32(&warmupcounts, 1)
    }

}

func performWarmUp() {
    /* Based on config, perform Warmuptnxs * 2 (2 reads) number of reads
    to fill up read cache. Perform the reads concurrently */

    if conf.Warmuptnxs > 0 {
        log.Printf("Performing warm up reads")
        log.Printf("warm up txns %v", conf.Warmuptnxs)
        for i := 0; i < conf.Concurrency; i++ {
            go do_warm_up_tnxs(db)
        }
    }

    if conf.Warmuptnxs > 0 {
        for atomic.LoadInt32(&warmupcounts) <= int32(conf.Warmuptnxs) {
            /* aiting for warming up to finish */
            time.Sleep(5 * time.Second)
        }
        log.Printf("Done with warm up reads : %v", atomic.LoadInt32(&warmupcounts))
    }
}


///////////////////// Main function /////////////////////

func main() {
    
    /* Load the configuration into struct variable conf */
    loadConfig()

    /* Connect to DB and use it to perform all DB operations */
    db = createDBConnection()

    /* Fill the DB with some entries */
    addEntriesToTable()
    
    /* Perform some warm up reads to fill read cache */
    performWarmUp()
}