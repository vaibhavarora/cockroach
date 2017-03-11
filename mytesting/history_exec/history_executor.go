package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	//	"strconv"
	"strings"
	// Import postgres driver.
	//"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

var history = flag.String("history", "b1 b2 r1(x) r2(x) w1(x) w2(x) c1 c2", "history to execute.")

func read(key int, tx *sql.Tx) int {

	var balance int
	//log.Printf(" key is %v, tnx is %v", key, tx)

	if err := tx.QueryRow(
		"SELECT balance FROM account WHERE id = $1", key).Scan(&balance); err != nil {
		log.Println(err)
	}

	/*for rows.Next() {
		if err = rows.Scan(&id, &balance); err != nil {
			log.Println(err)
		}
		fmt.Println("id ", id, "balance ", balance)
	}*/
	fmt.Println(balance)

	return balance
}

func write(key int, value int, tx *sql.Tx) {
	update := `UPDATE account SET balance = $1 WHERE id = $2;`

	if _, err := tx.Exec(update, value, key); err != nil {
		log.Println(err)
	}
	fmt.Println(value)
}

func commit_tnx(tx *sql.Tx) {
	if err := tx.Commit(); err != nil {
		log.Println(err)
	}

}

func begin_tnx(db *sql.DB) *sql.Tx {
	//log.Printf("db %v", db)
	tx, err := db.Begin()
	if err != nil {
		log.Println(err)
	}

	return tx

}

func execute_history(history string, db *sql.DB) {
	//func execute_history(history string) {
	h := strings.Split(history, " ")
	var tx1 *sql.Tx
	var tx2 *sql.Tx
	x := 1
	y := 2

	for _, element := range h {

		switch {
		case element == "b1":
			fmt.Println("begin of t1")
			tx1 = begin_tnx(db)
		case element == "b2":
			fmt.Println("begin of t2")
			tx2 = begin_tnx(db)
		case element == "r1(x)":
			fmt.Println("tnx1 reading x")
			_ = read(x, tx1)
		case element == "r1(y)":
			fmt.Println("tnx1 reading y")
			_ = read(y, tx1)
		case element == "r2(x)":
			fmt.Println("tnx2 reading x")
			_ = read(x, tx2)
		case element == "w1(x)":
			fmt.Println("tnx1 writing x")
			write(x, 104, tx1)
		case element == "w1(y)":
			fmt.Println("tnx1 writing y")
			write(y, 102, tx1)
		case element == "w2(x)":
			fmt.Println("tnx2 writing x")
			write(x, 102, tx2)
		case element == "c1":
			fmt.Println("commiting tnx1")
			commit_tnx(tx1)
		case element == "c2":
			fmt.Println("commiting tnx2")
			commit_tnx(tx2)
		case element == "r2(y)":
			fmt.Println("tnx2 reading y")
			_ = read(y, tx2)
		}
	}

}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

func initialize_db() *sql.DB {
	//dbURL := "postgresql://root@pacific:26257/bank2?sslmode=disable"
	dbURL := "postgresql://root@localhost:26257/bank2?sslmode=disable"

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

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS bank2"); err != nil {
		log.Fatal(err)
	}
	log.Printf("database created")

	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

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

	if EntriesExists == false {
		log.Printf("Inserting entries")
		insertSQL := "INSERT INTO account (id, balance) VALUES ($1, $2)"

		if _, err = db.Exec(insertSQL, 1, 100); err != nil {
			log.Fatal(err)
		}
		if _, err = db.Exec(insertSQL, 2, 100); err != nil {
			log.Fatal(err)
		}
	}

	return db

}

func main() {
	flag.Usage = usage
	flag.Parse()

	db := initialize_db()
	defer func() { _ = db.Close() }()
	//log.Printf("db %v", db)
	execute_history(*history, db)
	//execute_history(*history)
}
