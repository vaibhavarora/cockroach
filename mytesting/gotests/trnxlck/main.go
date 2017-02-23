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

	for i := 0; i < 5; i++ {
		fmt.Println("calling")
		go updaterecord(tc, k1, 2)
	}

	for i := 0; i < 5; i++ {
		fmt.Println("calling")
		go updaterecord(tc, k2, 1)
	}

	time.Sleep(25 * time.Second)
	fmt.Println("Dieing")
}

func updaterecord(tc *TransactionRecordLockCache, k roachpb.Key, t time.Duration) {
	tc.getAccess(k)
	time.Sleep(t * time.Second)
	tc.releaseAccess(k)
}
