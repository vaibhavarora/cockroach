package main

import (
	"crypto/rand"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"time"
)

func getRandomKey() []byte {
	c := 10
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("error:", err)
		return b
	}

	return b
}

func main() {
	tc := NewTransactionRecordLockCache()
	k1 := getRandomKey()
	k2 := getRandomKey()

	for i := 0; i < 1; i++ {
		fmt.Println("calling")
		go updaterecord(tc, k1, k2, 6)
	}
	//time.Sleep(100 * time.Millisecond)
	for i := 0; i < 1; i++ {
		fmt.Println("calling")
		go updaterecord(tc, k2, k1, 3)
	}

	time.Sleep(25 * time.Second)
	fmt.Println("Dieing")
}

func updaterecord(tc *TransactionRecordLockCache, my_k roachpb.Key, o_k roachpb.Key, t time.Duration) {
	fmt.Println("trying to get access to my record ")
	if !tc.getAccess(my_k) {
		fmt.Println("access to my record: Aborting")
		return
	}
	defer func() {
		tc.releaseAccess(my_k)
		fmt.Println("released my record ")
	}()
	fmt.Println("got access to my record ")
	time.Sleep(100 * time.Millisecond)
	fmt.Println("trying to get access to other record ")
	if !tc.getAccess(o_k) {
		fmt.Println("access to others record: Aborting")
		return
	}
	defer func() {
		tc.releaseAccess(o_k)
		fmt.Println("released other record ")
	}()
	fmt.Println("got access to other record ")
	fmt.Println("sleeping ")
	time.Sleep(t * time.Second)
}
