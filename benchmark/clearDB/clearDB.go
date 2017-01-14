package main

import (
	"database/sql"

	_ "github.com/lib/pq"
	"log"

	"net/url"
)

func main() {
	//dbURL := "postgresql://root@localhost:26257/bank2?sslmode=disable"
	dbURL := "postgresql://root@pacific:26257/bank2?sslmode=disable"
	//dbURL := "postgresql://root@pacific:26257?sslmode=disable"

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

	if _, err = db.Exec("TRUNCATE TABLE account"); err != nil {
		log.Fatal(err)
	}

	log.Printf("databse cleared")
}
