package main

import (
	"github.com/cockroachdb/cockroach/benchmark/concurrency/shared"
	"log"
	"net/rpc"
)

func main() {
	client, err := rpc.Dial("tcp", "localhost:42586")
	if err != nil {
		log.Fatal(err)
	}

	stat := &shared.Data{10, 1}
	var reply bool
	err = client.Call("Listener.CollectStats", stat, &reply)
	if err != nil {
		log.Fatal(err)
	}
}
