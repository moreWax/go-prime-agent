package rlm

import (
	"sync"
	"testing"
)

// Coordination-primitive costs in the shapes this kernel uses, so the
// "channels over mutexes" design is backed by numbers, not vibes.
// (Design rule stays: channels for ownership/select-composition, WaitGroup
// for lifecycle — none of these costs are hot-path material next to JSON,
// fd writes, and interpretation.)

func fanWaitGroup(n int) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done() }()
	}
	wg.Wait()
}

func fanChannel(n int) {
	ch := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() { ch <- struct{}{} }()
	}
	for i := 0; i < n; i++ {
		<-ch
	}
}

func mutexN(n int) {
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

func chanN(n int) {
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	for i := 0; i < n; i++ {
		<-ch
	}
}

func BenchmarkFanoutWaitGroup8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fanWaitGroup(8)
	}
}
func BenchmarkFanoutChannel8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fanChannel(8)
	}
}
func BenchmarkMutexLockUnlock(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mutexN(1)
	}
}
func BenchmarkChanSendRecv(b *testing.B) {
	for i := 0; i < b.N; i++ {
		chanN(1)
	}
}
func BenchmarkGoroutineSpawn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fanWaitGroup(1)
	}
}
