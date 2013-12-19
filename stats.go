package rgo

import (
	"strconv"
	"sync/atomic"
)

type Stats struct {
	ServerRequests AtomicInt
	Pings          AtomicInt
	Gets           AtomicInt
	Sets           AtomicInt
	Posts          AtomicInt
	Puts           AtomicInt
	PingErrors     AtomicInt
	GetErrors      AtomicInt
	SetErrors      AtomicInt
	PostErrors     AtomicInt
	PutsErrors     AtomicInt
}

type AtomicInt int64

func (i *AtomicInt) Add(val int64) {
	atomic.AddInt64((*int64)(i), val)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}
