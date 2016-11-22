package main

import (
    "log"
    "net/rpc"
    "github.com/cockroachdb/cockroach/benchmark/dynamicConcurrencyServer/shared"
)

type Data struct {
    success, retries int
}

func main() {
    client, err := rpc.Dial("tcp", "localhost:42586")
    if err != nil {
        log.Fatal(err)
    }

    stat := &shared.Data{10,1}
    var reply bool
    err = client.Call("Listener.CollectStats", stat, &reply)
    if err != nil {
        log.Fatal(err)
    }
}
