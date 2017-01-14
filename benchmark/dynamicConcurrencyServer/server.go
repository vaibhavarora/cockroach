package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/cockroachdb/cockroach/benchmark/dynamicConcurrencyServer/shared"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type data struct {
	Success, Retries int
	Transactionrate  float64
}

type Listener int

var maxconcurrency = flag.Int("max", 200, "Maximum concurrency level to test.")
var concurrencystep = flag.Int("step", 5, "Incremental for concurrency")
var startconcurrency = flag.Int("start", 1, "start of concurrency")

var stats = make(map[int]data)
var optimalConcurrency = 1
var optimaltransactionrate float64

func (l *Listener) CollectStats(info *shared.Data, ack *bool) error {
	stat := data{info.Success, info.Retries, info.Transactionrate}
	stats[info.Concurrency] = stat

	if info.Transactionrate > optimaltransactionrate {
		optimaltransactionrate = info.Transactionrate
		optimalConcurrency = info.Concurrency
	}
	log.Printf("Concurrency %v : Transaction rate %v ,Total Success %v , Total Retries %v", info.Concurrency, info.Transactionrate, info.Success, info.Retries)
	return nil
}

func callbenchmark() {

	concurrency := *startconcurrency
	// everything is set to its default values as in main program
	maxTransfer := 100
	numTransfers := 50000
	numAccounts := 50000
	contention := "low"
	contentionratio := "50:50"
	err := os.Chdir("../")
	if err != nil {
		log.Fatal(err)
	}
	path, err := exec.Command("pwd").Output()
	if err != nil {
		log.Fatal(err)
	}
	binary := strings.TrimSpace(string(path)) + "/benchmark"

	for concurrency <= *maxconcurrency {

		arg1 := "-max-transfer=" + strconv.Itoa(maxTransfer)
		arg2 := "-num-transfers=" + strconv.Itoa(numTransfers)
		arg3 := "-num-accounts=" + strconv.Itoa(numAccounts)
		arg4 := "-contention=" + contention
		arg5 := "-contention-ratio=" + contentionratio
		arg6 := "-report-concurrency=true"
		arg7 := "-concurrency=" + strconv.Itoa(concurrency)

		cmd := exec.Command(binary, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
		//log.Printf("cmd is %s", strings.Join(cmd.Args, " "))
		var out bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		time.Sleep(100 * time.Millisecond)
		err = cmd.Run()
		if err != nil {
			fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
			log.Fatal(err)
		}

		//log.Printf("done with concurrency %d", concurrency)
		concurrency += *concurrencystep
		time.Sleep(1 * time.Second)
	}
	time.Sleep(1 * time.Second)
	//log.Printf("The optimal concurrency level is %v with average success of %v and average abort of %v", stats[optimalConcurrency][0], stats[optimalConcurrency][1])
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Printf("Optimal Cncurrency is %d where Transaction rate was %v, suucessful transactions were %d and retires were %d", optimalConcurrency, optimaltransactionrate, stats[optimalConcurrency].Success, stats[optimalConcurrency].Retries)
		os.Exit(0)
	}()

	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:42586")
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}

	listener := new(Listener)
	rpc.Register(listener)
	go callbenchmark()
	//log.Printf("sever accepting")
	rpc.Accept(inbound)
}
