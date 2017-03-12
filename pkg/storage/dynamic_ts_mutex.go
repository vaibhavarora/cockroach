package storage

import (
	//"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"time"
)

const (
	BUSY          = true
	FREE          = false
	Wait_Duration = 50 * time.Millisecond
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
	if m.access == FREE {
		m.access = BUSY
		return true
	}
	return false
}

func (m *DyTSMutex) Lock(timed bool) bool {
	if timed {
		if m.testAndSet() {
			return true
		} else {
			time.Sleep(Wait_Duration)
			if m.testAndSet() {
				return true
			}
		}
	} else { // wait untill you get lock
		for !m.testAndSet() {
			time.Sleep(Wait_Duration)
		}
		return true
	}
	return false
}

func (m *DyTSMutex) Unlock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.access = FREE
}
