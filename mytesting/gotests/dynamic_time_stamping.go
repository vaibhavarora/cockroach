package main

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	//"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

type DynanicTimeStamper struct {
	softlockcache *SoftLockCache
}

func NewDynamicTImeStamper(s *SoftLockCache) *DynanicTimeStamper {
	d := &DynanicTimeStamper{
		softlockcache: s,
	}
	return d
}

var consultsSoftLockCacheMethods = [...]bool{
	roachpb.Put:            true,
	roachpb.ConditionalPut: true,
	roachpb.Increment:      true,
	roachpb.Delete:         true,
	roachpb.DeleteRange:    true,
}

func consultsSoftLockCache(r roachpb.Request) bool {
	m := r.Method()
	if m < 0 || m >= roachpb.Method(len(consultsSoftLockCacheMethods)) {
		return false
	}
	return consultsSoftLockCacheMethods[m]
}

func (d *DynanicTimeStamper) applySoftLockCache(ba *roachpb.BatchRequest) {
	for _, union := range ba.Requests {
		args := union.GetInner()
		if consultsSoftLockCache(args) {
			header := args.Header()
			m := args.Method()
			fmt.Println(header, m)
		}
	}
}
