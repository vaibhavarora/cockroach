package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/cockroachdb/cockroach/benchmark/pkg/stats"
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

var concurrency = flag.Int("concurrency", 200, "Concurrency level to test.")
var contention_start = flag.String("contention-start", "50:50", "Start of concurrency ratio")
var contention_end = flag.String("contention-end", "90:10", "End of concurrency ratio")

//var concurrencystep = flag.Int("step", 5, "Incremental for concurrency")
//var startconcurrency = flag.Int("start", 1, "start of concurrency")

//var stats = make(map[int]data)

//var optimalConcurrency = 1
//var optimaltransactionrate float64

func (l *Listener) CollectStats(info *stats.Data, ack *bool) error {
	//stat := data{info.Success, info.Retries, info.Transactionrate}
	//stats[info.Concurrency] = stat

	//	if info.Transactionrate > optimaltransactionrate {
	//		optimaltransactionrate = info.Transactionrate
	//		optimalConcurrency = info.Concurrency
	//	}
	//log.Printf("Contention %s : Transaction rate %v ,Total Success %v , Total Retries %v", info.Contention, info.Transactionrate, info.Success, info.Retries)
	log.Printf("Contention %s : Transaction rate %v ,Total Success %v , Total Retries %v, Average read time %v, Average write time %v, Average time for transaction(without retires) %v Average time for transaction ( with retries ) %v", info.Contention, info.Transactionrate, info.Success, info.Retries, info.Avgread, info.Avgwrite, info.Tnxwithoutreties, info.Trxwithretries)
	return nil
}

func callbenchmark() {

	// everything is set to its default values as in main program
	maxTransfer := 100
	numTransfers := 100000
	numAccounts := 100000
	contention := "low"
	c_s := strings.Split(*contention_start, ":")
	c_e := strings.Split(*contention_end, ":")
	contention1, _ := strconv.Atoi(c_s[0])
	contention2, _ := strconv.Atoi(c_s[1])
	contention_max, _ := strconv.Atoi(c_e[0])
	contentiona := strconv.Itoa(contention1)
	contentionb := strconv.Itoa(contention2)
	contentionratio := contentiona + ":" + contentionb
	warm_up_tnxs := 25000

	err := os.Chdir("../")
	if err != nil {
		log.Fatal(err)
	}
	path, err := exec.Command("pwd").Output()
	if err != nil {
		log.Fatal(err)
	}
	binary := strings.TrimSpace(string(path)) + "/base/base"
	//log.Printf("binary %s", binary)
	for contention1 <= contention_max {

		contentiona = strconv.Itoa(contention1)
		contentionb = strconv.Itoa(contention2)
		contentionratio = contentiona + ":" + contentionb

		arg1 := "-max-transfer=" + strconv.Itoa(maxTransfer)
		arg2 := "-num-transfers=" + strconv.Itoa(numTransfers)
		arg3 := "-num-accounts=" + strconv.Itoa(numAccounts)
		arg4 := "-contention=" + contention
		arg5 := "-contention-ratio=" + contentionratio
		arg6 := "-report-concurrency=true"
		arg7 := "-concurrency=" + strconv.Itoa(*concurrency)
		arg8 := "-warm-up-tnx=" + strconv.Itoa(warm_up_tnxs)

		cmd := exec.Command(binary, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
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
		contention1 += 10
		contention2 -= 10
		time.Sleep(1 * time.Second)
	}
	time.Sleep(1 * time.Second)
	log.Printf("done!")
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
		//	log.Printf("Optimal Cncurrency is %d where Transaction rate was %v, suucessful transactions were %d and retires were %d", optimalConcurrency, optimaltransactionrate, stats[optimalConcurrency].Success, stats[optimalConcurrency].Retries)
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
