package main

import (
	//"bytes"
	"crypto/rand"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	c := NewSoftLockCache()
	k1 := getRandomKey()
	threads := 5

	ch := make(chan uuid.UUID, 10)
	for i := 0; i < threads; i++ {
		fmt.Printf("calling transaction\n")
		go transaction(c, k1, ch)
	}

	count := 0
	for i := range ch {
		fmt.Printf("Transaction id %v done\n", i)
		count += 1
		if count == threads {
			break
		}
	}

	fmt.Println(c.getAllWriteSoftLocksOnKey(k1))
	fmt.Println(c.getAllReadSoftLocksOnKey(k1))

}

func transaction(c *SoftLockCache, k1 []byte, ch chan uuid.UUID) {
	id := uuid.MakeV4()
	fmt.Println("Tnx id :", id)

	time.Sleep(1)

	addReadLock(c, k1, id)
	fmt.Printf("id %v placed read lock \n", id)

	q := c.getAllReadSoftLocksOnKey(k1)
	for _, l := range q {
		fmt.Printf("id %v :retrived read lock placed tnx id %v \n", id, l.TransactionMeta.ID)
		//c.removeFromReadLockCache(l)
	}

	addWriteLock(c, k1, id)
	fmt.Printf("id %v placed write lock \n", id)

	qw := c.getAllWriteSoftLocksOnKey(k1)
	for _, l := range qw {
		fmt.Printf("id %v :retrived write lock placed tnx id %v \n", id, l.TransactionMeta.ID)
		//c.removeFromWriteLockCache(l)
	}

	tmeta := enginepb.TxnMeta{ID: &id}
	wl := c.getWriteSoftLock(k1, tmeta)

	fmt.Printf("id %v :retrived own write lock %v \n", id, wl)

	c.removeFromWriteLockCache(wl)

	ch <- id

}

func addReadLock(c *SoftLockCache, k []byte, id uuid.UUID) {
	tmeta := enginepb.TxnMeta{ID: &id}
	readlk := ReadSoftLock{TransactionMeta: tmeta, key: k}
	c.addToSoftReadLockCache(readlk)
}

func addWriteLock(c *SoftLockCache, k []byte, id uuid.UUID) {

	tmeta := enginepb.TxnMeta{ID: &id}
	value := roachpb.Value{}
	writelk := WriteSoftLock{TransactionMeta: tmeta, key: k, value: value}
	c.addToSoftWriteLockCache(writelk)

}
