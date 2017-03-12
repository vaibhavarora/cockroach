package main

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"time"
)

const (
	BUSY          = true
	FREE          = false
	Wait_Duration = 2 * time.Second
)

type DyTSMutex struct {
	mu     syncutil.Mutex
	access bool
}

func NewDyTSMutex() *DyTSMutex {
	return &DyTSMutex{access: FREE}
}

func (m *DyTSMutex) testAndSet() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println("testset")
	if m.access == FREE {
		m.access = BUSY
		return true
	}
	return false
}

func (m *DyTSMutex) Lock() bool {
	if m.testAndSet() {
		return true
	} else {
		fmt.Println("testset failed, waiting")
		time.Sleep(Wait_Duration)
		if m.testAndSet() {
			return true
		}
	}
	return false
}

func (m *DyTSMutex) Unlock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.access = FREE
	fmt.Println("unlocked dytsmutex")
}
