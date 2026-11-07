/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lbcache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/loadbalance"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// ---------- fake clock ----------

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// ---------- blocking resolver ----------

type blockingResolver struct {
	name string

	mu sync.Mutex
	fn func(call int) (discovery.Result, error)

	calls   int32
	active  int32
	gateMu  sync.Mutex
	gates   []chan struct{}
	entered chan struct{}
}

var _ discovery.Resolver = (*blockingResolver)(nil)

func (r *blockingResolver) Target(ctx context.Context, target rpcinfo.EndpointInfo) string {
	return "target"
}

func (r *blockingResolver) Resolve(ctx context.Context, desc string) (discovery.Result, error) {
	call := int(atomic.AddInt32(&r.calls, 1))
	atomic.AddInt32(&r.active, 1)
	defer atomic.AddInt32(&r.active, -1)

	gate := make(chan struct{})
	r.gateMu.Lock()
	r.gates = append(r.gates, gate)
	r.gateMu.Unlock()
	if r.entered != nil {
		select {
		case r.entered <- struct{}{}:
		default:
		}
	}

	select {
	case <-gate:
	case <-ctx.Done():
		// managed context canceled (factory close / refresh timeout): unwind
		return discovery.Result{}, ctx.Err()
	}

	r.mu.Lock()
	fn := r.fn
	r.mu.Unlock()
	return fn(call)
}

func (r *blockingResolver) Diff(cacheKey string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(cacheKey, prev, next)
}

func (r *blockingResolver) Name() string { return r.name }

func (r *blockingResolver) setFunc(fn func(call int) (discovery.Result, error)) {
	r.mu.Lock()
	r.fn = fn
	r.mu.Unlock()
}

// releaseNext unblocks the oldest currently blocked Resolve call.
func (r *blockingResolver) releaseNext() {
	r.gateMu.Lock()
	defer r.gateMu.Unlock()
	for _, g := range r.gates {
		select {
		case <-g:
			continue
		default:
			close(g)
			return
		}
	}
}

// releaseAll unblocks every blocked Resolve call (including future ones already gated).
func (r *blockingResolver) releaseAll() {
	r.gateMu.Lock()
	defer r.gateMu.Unlock()
	for _, g := range r.gates {
		select {
		case <-g:
		default:
			close(g)
		}
	}
}

func (r *blockingResolver) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-r.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("resolver.Resolve was not invoked in time")
	}
}

// drainEntered discards stale entry signals, e.g. from attempts whose entry
// was observed by other synchronization instead of waitEntered.
func (r *blockingResolver) drainEntered() {
	for {
		select {
		case <-r.entered:
		default:
			return
		}
	}
}

func (r *blockingResolver) waitActive(t *testing.T, want int32) {
	t.Helper()
	waitUntil(t, func() bool { return atomic.LoadInt32(&r.active) == want },
		fmt.Sprintf("active resolver calls == %d, got %d", want, atomic.LoadInt32(&r.active)))
}

func (r *blockingResolver) waitCalls(t *testing.T, want int32) {
	t.Helper()
	waitUntil(t, func() bool { return atomic.LoadInt32(&r.calls) == want },
		fmt.Sprintf("resolver calls == %d, got %d", want, atomic.LoadInt32(&r.calls)))
}

// ---------- recording rebalancer ----------

type recordingRebalancer struct {
	mu      sync.Mutex
	rebal   []discovery.Change
	deletes []discovery.Change
	pickers map[string]loadbalance.Picker
}

var (
	_ loadbalance.Loadbalancer = (*recordingRebalancer)(nil)
	_ loadbalance.Rebalancer   = (*recordingRebalancer)(nil)
)

func newRecordingRebalancer() *recordingRebalancer {
	return &recordingRebalancer{pickers: map[string]loadbalance.Picker{}}
}

type staticPicker struct {
	instances []discovery.Instance
	idx       int
	mu        sync.Mutex
}

func (p *staticPicker) Next(ctx context.Context, request interface{}) discovery.Instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.instances) == 0 {
		return nil
	}
	ins := p.instances[p.idx%len(p.instances)]
	p.idx++
	return ins
}

func (r *recordingRebalancer) GetPicker(res discovery.Result) loadbalance.Picker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pickers[res.CacheKey]; ok {
		return p
	}
	p := &staticPicker{instances: res.Instances}
	r.pickers[res.CacheKey] = p
	return p
}

func (r *recordingRebalancer) Name() string { return "recording" }

func (r *recordingRebalancer) Rebalance(ch discovery.Change) {
	r.mu.Lock()
	r.rebal = append(r.rebal, ch)
	r.pickers[ch.Result.CacheKey] = &staticPicker{instances: ch.Result.Instances}
	r.mu.Unlock()
}

func (r *recordingRebalancer) Delete(ch discovery.Change) {
	r.mu.Lock()
	r.deletes = append(r.deletes, ch)
	delete(r.pickers, ch.Result.CacheKey)
	r.mu.Unlock()
}

func (r *recordingRebalancer) snapshot() (rebal, deletes []discovery.Change) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]discovery.Change(nil), r.rebal...), append([]discovery.Change(nil), r.deletes...)
}

// ---------- helpers ----------

func freshnessOptions() Options {
	opts := Options{
		RefreshInterval:  time.Second,
		ExpireInterval:   time.Minute,
		FreshDuration:    10 * time.Second,
		MaxStaleDuration: 20 * time.Second,
		RefreshTimeout:   5 * time.Second,
	}
	opts.check()
	return opts
}

func newFreshnessFactory(t *testing.T, r discovery.Resolver, lb loadbalance.Loadbalancer, fc clock) *BalancerFactory {
	t.Helper()
	bf := newBalancerFactory(r, lb, freshnessOptions())
	bf.clk = fc
	return bf
}

func genResult(cacheKey string, addrs ...string) discovery.Result {
	inss := make([]discovery.Instance, 0, len(addrs))
	for _, a := range addrs {
		inss = append(inss, discovery.NewInstance("tcp", a, 10, nil))
	}
	return discovery.Result{Cacheable: true, CacheKey: cacheKey, Instances: inss}
}

func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}

type getResult struct {
	bl  *Balancer
	err error
}

func asyncGet(bf *BalancerFactory, ctx context.Context) <-chan getResult {
	ch := make(chan getResult, 1)
	go func() {
		bl, err := bf.Get(ctx, nil)
		ch <- getResult{bl, err}
	}()
	return ch
}

func assertNoResult(t *testing.T, ch <-chan getResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("expected Get to stay blocked, got result=%v err=%v", r.bl, r.err)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertGetOK(t *testing.T, ch <-chan getResult) *Balancer {
	t.Helper()
	select {
	case r := <-ch:
		test.Assert(t, r.err == nil, r.err)
		test.Assert(t, r.bl != nil)
		return r.bl
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not return in time")
		return nil
	}
}

func assertGetErr(t *testing.T, ch <-chan getResult) error {
	t.Helper()
	select {
	case r := <-ch:
		test.Assert(t, r.err != nil)
		return r.err
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not return in time")
		return nil
	}
}

func pickerAddrs(bl *Balancer, n int) map[string]struct{} {
	got := map[string]struct{}{}
	p := bl.GetPicker()
	for i := 0; i < n; i++ {
		ins := p.Next(context.Background(), nil)
		if ins == nil {
			got["<nil>"] = struct{}{}
			continue
		}
		got[ins.Address().String()] = struct{}{}
	}
	return got
}

func assertPickerAddrs(t *testing.T, bl *Balancer, n int, want ...string) {
	t.Helper()
	wantSet := make(map[string]struct{}, len(want))
	for _, w := range want {
		wantSet[w] = struct{}{}
	}
	test.DeepEqual(t, pickerAddrs(bl, n), wantSet)
}

func assertGoroutinesSettled(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, runtime.NumGoroutine())
}

func cachedBalancer(t *testing.T, bf *BalancerFactory) *Balancer {
	t.Helper()
	v, ok := bf.cache.Load("target")
	test.Assert(t, ok)
	return v.(*Balancer)
}

// waitNoInflight blocks until the cached balancer has no in-flight refresh.
// resolver active==0 only proves Resolve returned; the commit that clears
// inflight and publishes the snapshot happens afterwards.
func waitNoInflight(t *testing.T, bf *BalancerFactory) {
	t.Helper()
	waitBalancerNoInflight(t, cachedBalancer(t, bf))
}

// waitBalancerNoInflight waits on a specific Balancer instance; it still works
// after the Balancer was removed from the factory cache by cleanup.
func waitBalancerNoInflight(t *testing.T, bl *Balancer) {
	t.Helper()
	waitUntil(t, func() bool {
		bl.mu.Lock()
		s := bl.inflight == nil
		bl.mu.Unlock()
		return s
	}, "balancer inflight cleared")
}

// ---------- tests ----------

// fresh snapshot is served directly without any extra Resolve.
func TestFreshnessFresh(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	assertNoResult(t, ch)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 1)
	r.releaseNext()
	bl := assertGetOK(t, ch)
	waitNoInflight(t, bf)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 1)
	assertPickerAddrs(t, bl, 4, "a")

	// inside the fresh window: direct cache hit, no Resolve
	fc.Advance(9 * time.Second)
	bl2, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	test.Assert(t, bl2 == bl)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 1)
	assertPickerAddrs(t, bl2, 4, "a")

	// exactly at the fresh boundary it is still fresh
	fc.Advance(time.Second)
	_, err = bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 1)
}

// stale snapshot is served immediately while exactly one background refresh runs.
func TestFreshnessStale(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl := assertGetOK(t, ch)
	waitNoInflight(t, bf)

	// enter the stale window (age 15s, fresh=10s, hard boundary=30s)
	fc.Advance(15 * time.Second)
	bl2, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	test.Assert(t, bl2 == bl)
	// stale call returns the old generation immediately; a background refresh is merged
	r.waitEntered(t)
	r.waitActive(t, 1)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	assertPickerAddrs(t, bl2, 4, "a")

	// many concurrent stale callers: none blocks, no extra Resolve is issued
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, e := bf.Get(context.Background(), nil)
			test.Assert(t, e == nil)
			assertPickerAddrs(t, b, 2, "a")
		}()
	}
	wg.Wait()
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	test.Assert(t, atomic.LoadInt32(&r.active) == 1)

	// complete the shared refresh with a new generation
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k2", "b"), nil })
	r.releaseNext()
	waitNoInflight(t, bf)

	// successAt restarted from the fake clock: snapshot is fresh again
	b3, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	assertPickerAddrs(t, b3, 6, "b")
}

// beyond the hard boundary callers wait for the bounded refresh; merged waiters
// share the single in-flight Resolve.
func TestFreshnessExpired(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	assertGetOK(t, ch)
	waitNoInflight(t, bf)

	// cross the hard boundary (age 31s > 10s + 20s)
	fc.Advance(31 * time.Second)

	// a caller with a short deadline gives up waiting but the shared refresh stays
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err := bf.Get(shortCtx, nil)
	cancel()
	test.Assert(t, errors.Is(err, context.DeadlineExceeded), err)
	r.waitActive(t, 1)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)

	// two more callers must wait and merge onto the very same refresh
	g1 := asyncGet(bf, context.Background())
	g2 := asyncGet(bf, context.Background())
	waitUntil(t, func() bool {
		bl := cachedBalancer(t, bf)
		bl.mu.Lock()
		n := bl.inflight != nil
		bl.mu.Unlock()
		return n
	}, "inflight refresh exists")
	assertNoResult(t, g1)
	assertNoResult(t, g2)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)

	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k3", "c"), nil })
	r.releaseAll()
	bl1 := assertGetOK(t, g1)
	bl2 := assertGetOK(t, g2)
	test.Assert(t, bl1 == bl2)
	waitNoInflight(t, bf)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	assertPickerAddrs(t, bl1, 6, "c")

	// the new generation is fresh: no further Resolve
	_, err = bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
}

// failed refreshes never extend freshness; hard-expired failures are reported
// distinctly from the refresh error while errors.Is semantics are preserved.
func TestRefreshFailureDoesNotExtend(t *testing.T) {
	errBoom := errors.New("boom resolver")
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	assertGetOK(t, ch)
	waitNoInflight(t, bf)

	bl := cachedBalancer(t, bf)
	bl.mu.Lock()
	successAt := bl.snap.successAt
	bl.mu.Unlock()

	// stale window: refresh fails, the call still gets the stale snapshot
	fc.Advance(15 * time.Second)
	r.setFunc(func(call int) (discovery.Result, error) { return discovery.Result{}, errBoom })
	_, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	r.waitEntered(t)
	r.releaseNext()
	waitNoInflight(t, bf)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)

	// freshness timestamp must not move and the state remains stale, not fresh
	bl.mu.Lock()
	test.Assert(t, bl.snap.successAt.Equal(successAt))
	test.Assert(t, bl.classifyLocked() == stateStale)
	bl.mu.Unlock()

	// cross the hard boundary against the ORIGINAL success time: explicit failure
	fc.Advance(20 * time.Second)
	expCh := asyncGet(bf, context.Background())
	r.waitEntered(t)
	assertNoResult(t, expCh)
	r.releaseNext()
	expErr := assertGetErr(t, expCh)
	test.Assert(t, errors.Is(expErr, ErrDiscoveryCacheExpired), expErr)
	test.Assert(t, errors.Is(expErr, errBoom), expErr)
	waitNoInflight(t, bf)

	bl.mu.Lock()
	test.Assert(t, bl.classifyLocked() == stateExpired)
	bl.mu.Unlock()

	// a later successful refresh recovers the target
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k2", "b"), nil })
	okCh := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl2 := assertGetOK(t, okCh)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl2, 4, "b")
}

// a successful empty result means "no instances now": it replaces the previous
// non-empty set instead of keeping the old list for availability.
func TestSuccessfulEmptyResult(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl := assertGetOK(t, ch)
	assertPickerAddrs(t, bl, 2, "a")

	// refresh returns a successful, cacheable, empty result
	fc.Advance(15 * time.Second)
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("empty"), nil })
	_, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	r.waitEntered(t)
	r.releaseNext()
	waitNoInflight(t, bf)

	// old non-empty set is gone: picker yields nil (DummyPicker), never "a"
	assertPickerAddrs(t, cachedBalancer(t, bf), 3, "<nil>")

	// still within the fresh window of the empty snapshot: no Resolve, still empty
	fc.Advance(time.Second)
	bl2, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	assertPickerAddrs(t, bl2, 3, "<nil>")

	// instances come back: a new generation serves them again
	fc.Advance(15 * time.Second)
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k3", "b"), nil })
	back := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl3 := assertGetOK(t, back)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl3, 4, "b")
}

// canceling one waiting caller only stops its own wait; the shared refresh
// continues and serves the remaining waiter with a single Resolve.
func TestCancelOneWaiterKeepsSharedRefresh(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	assertGetOK(t, ch)
	waitNoInflight(t, bf)

	fc.Advance(31 * time.Second) // hard expired

	ctx1, cancel1 := context.WithCancel(context.Background())
	g1 := asyncGet(bf, ctx1)
	g2 := asyncGet(bf, context.Background())
	r.waitEntered(t)
	waitUntil(t, func() bool { return atomic.LoadInt32(&r.active) == 1 }, "single shared refresh")
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)

	// caller 1 gives up: its context error surfaces, but the refresh is not canceled
	cancel1()
	err1 := assertGetErr(t, g1)
	test.Assert(t, errors.Is(err1, context.Canceled), err1)
	assertNoResult(t, g2)
	test.Assert(t, atomic.LoadInt32(&r.active) == 1)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)

	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k2", "b"), nil })
	r.releaseNext()
	bl2 := assertGetOK(t, g2)
	waitNoInflight(t, bf)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	assertPickerAddrs(t, bl2, 4, "b")
}

// factory close cancels blocked refreshes, rejects further Gets and reaps
// every goroutine it owned (refresh worker + idle watcher).
func TestCloseCancelsRefreshAndReapsGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	assertGetOK(t, ch)
	waitNoInflight(t, bf)

	// block a background refresh, then close the factory with it in flight
	fc.Advance(15 * time.Second)
	_, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	r.waitEntered(t)
	r.waitActive(t, 1)

	bf.Close()
	r.waitActive(t, 0) // managed context cancellation unwinds the blocked Resolve
	test.Assert(t, bf.ctx.Err() != nil)

	_, err = bf.Get(context.Background(), nil)
	test.Assert(t, errors.Is(err, ErrBalancerFactoryClosed), err)

	// close must be idempotent
	test.Assert(t, bf.Close() == nil)
	assertGoroutinesSettled(t, baseline)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
}

// a refresh of an old generation that lands after cleanup must not touch the
// rebalancer/picker cache or overwrite a target rebuilt with the same key.
func TestCleanupRebuildGeneration(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	rb := newRecordingRebalancer()
	bf := newFreshnessFactory(t, r, rb, fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl1 := assertGetOK(t, ch)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl1, 2, "a")

	// start a refresh on bl1 and block it inside the resolver
	fc.Advance(15 * time.Second)
	_, err := bf.Get(context.Background(), nil)
	test.Assert(t, err == nil)
	r.waitEntered(t)
	r.waitActive(t, 1)

	// watcher-style cleanup while the refresh is still in flight
	bf.cache.Delete("target")
	bl1.close()
	_, deletes := rb.snapshot()
	test.Assert(t, len(deletes) == 1 && deletes[0].Result.CacheKey == t.Name()+":k1")

	// late result of the dead generation arrives: it must be discarded
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k2", "b"), nil })
	r.releaseNext()
	waitBalancerNoInflight(t, bl1)
	rebal, _ := rb.snapshot()
	test.Assert(t, len(rebal) == 0, rebal) // no Rebalance after close
	bl1.mu.Lock()
	test.Assert(t, bl1.closed)
	test.Assert(t, bl1.snap.gen == 1)
	assertPickerAddrs(t, bl1, 2, "a")
	bl1.mu.Unlock()

	// same target is rebuilt as a brand new Balancer; old goroutine can't reach it
	ch2 := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl2 := assertGetOK(t, ch2)
	waitNoInflight(t, bf)
	test.Assert(t, bl2 != bl1)
	test.Assert(t, cachedBalancer(t, bf) == bl2)
	assertPickerAddrs(t, bl2, 4, "b")

	// a further refresh on the new generation couples Rebalance with the new picker
	fc.Advance(15 * time.Second)
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k3", "c"), nil })
	ch3 := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl3 := assertGetOK(t, ch3)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl3, 4, "c")

	rebal, deletes = rb.snapshot()
	test.Assert(t, len(rebal) == 1)
	test.Assert(t, rebal[0].Result.CacheKey == t.Name()+":k3")
	test.Assert(t, len(rebal[0].Added) == 1 && rebal[0].Added[0].Address().String() == "c")
	test.Assert(t, len(rebal[0].Removed) == 1 && rebal[0].Removed[0].Address().String() == "b")
	test.Assert(t, len(deletes) == 1) // the discarded generation never deleted the new one
}

// factory reuse keys must include the freshness configuration.
func TestCacheKeyIncludesFreshness(t *testing.T) {
	legacy := Options{RefreshInterval: 5 * time.Second, ExpireInterval: 15 * time.Second}
	test.Assert(t, cacheKey("r", "b", legacy) == "r|b|{5s 15s}")

	r := &blockingResolver{name: t.Name()}
	lb := loadbalance.NewWeightedBalancer()
	resolverName, balancerName := r.Name(), lb.Name()

	o1 := freshnessOptions()
	o2 := freshnessOptions()
	o2.FreshDuration = 20 * time.Second
	o2.check()
	o3 := freshnessOptions()
	o3.MaxStaleDuration = 30 * time.Second
	o3.check()

	k1 := cacheKey(resolverName, balancerName, o1)
	k2 := cacheKey(resolverName, balancerName, o2)
	k3 := cacheKey(resolverName, balancerName, o3)
	test.Assert(t, k1 != k2 && k1 != k3 && k2 != k3, k1, k2, k3)
	for _, k := range []string{k1, k2, k3} {
		test.Assert(t, strings.Contains(k, "freshness{"), k)
	}

	// shared factories with different freshness configs never reuse one cache
	f1 := NewBalancerFactory(r, lb, withCacheable(o1))
	f2 := NewBalancerFactory(r, lb, withCacheable(o2))
	test.Assert(t, f1 != f2)
	_, ok1 := balancerFactories.Load(k1)
	_, ok2 := balancerFactories.Load(k2)
	test.Assert(t, ok1 && ok2)

	// same config reuses the same factory and bumps the reference count
	f1Again := NewBalancerFactory(r, lb, withCacheable(o1))
	test.Assert(t, f1Again == f1)
	test.Assert(t, atomic.LoadInt32(&f1.refs) == 2)

	// first release of f1 only drops a reference; the factory stays alive
	test.Assert(t, f1.Close() == nil)
	_, stillThere := balancerFactories.Load(k1)
	test.Assert(t, stillThere)
	test.Assert(t, f1.ctx.Err() == nil)
	// last releases tear the factories down and remove them from the global map
	test.Assert(t, f1Again.Close() == nil)
	test.Assert(t, f2.Close() == nil)
	_, ok1 = balancerFactories.Load(k1)
	_, ok2 = balancerFactories.Load(k2)
	test.Assert(t, !ok1 && !ok2)
	test.Assert(t, f1.ctx.Err() != nil && f2.ctx.Err() != nil)
}

func withCacheable(o Options) Options {
	o.Cacheable = true
	return o
}

// Cacheable=false results must never become reusable snapshots: every Get
// re-resolves even when no time has elapsed.
func TestNonCacheableResultNotReused(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) {
		res := genResult("nc", "a")
		res.Cacheable = false
		return res, nil
	})
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl := assertGetOK(t, ch)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl, 2, "a")

	// stored for the current caller but immediately classified as hard-expired
	bl.mu.Lock()
	test.Assert(t, bl.classifyLocked() == stateExpired)
	bl.mu.Unlock()

	ch2 := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl2 := assertGetOK(t, ch2)
	waitNoInflight(t, bf)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	assertPickerAddrs(t, bl2, 2, "a")
}

// the picker handed out always belongs to the currently published generation.
func TestPickerMatchesPublishedGeneration(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	bf := newFreshnessFactory(t, r, loadbalance.NewWeightedBalancer(), fc)
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl := assertGetOK(t, ch)
	waitNoInflight(t, bf)

	// generation 2 replaces the instance list and its picker atomically
	fc.Advance(15 * time.Second)
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k2", "b", "c"), nil })
	ch2 := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	bl2 := assertGetOK(t, ch2)
	waitNoInflight(t, bf)

	test.Assert(t, bl2 == bl)
	test.DeepEqual(t, pickerAddrs(bl2, 8), map[string]struct{}{"b": {}, "c": {}})

	// repeated GetPicker calls keep serving the same new generation
	for i := 0; i < 5; i++ {
		test.DeepEqual(t, pickerAddrs(bl2, 4), map[string]struct{}{"b": {}, "c": {}})
	}
}

// a resolver that never answers must be unblocked by the managed refresh
// deadline: no unbounded context.Background may hang a refresh forever.
func TestRefreshTimeoutBoundsHungResolver(t *testing.T) {
	baseline := runtime.NumGoroutine()
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	opts := freshnessOptions()
	opts.RefreshTimeout = 100 * time.Millisecond
	bf := newBalancerFactory(r, loadbalance.NewWeightedBalancer(), opts)
	bf.clk = fc
	defer bf.Close()

	start := time.Now()
	_, err := bf.Get(context.Background(), nil)
	elapsed := time.Since(start)
	test.Assert(t, errors.Is(err, ErrDiscoveryCacheExpired), err)
	test.Assert(t, errors.Is(err, context.DeadlineExceeded), err)
	// the bounded deadline, not the test timeout, must have ended the wait
	test.Assert(t, elapsed < 2*time.Second, elapsed)
	// the timed-out resolver unwinds and no Balancer is cached on failure
	r.waitActive(t, 0)
	_, cached := bf.cache.Load("target")
	test.Assert(t, !cached)
	test.Assert(t, atomic.LoadInt32(&r.calls) == 1)

	// the factory is still usable after a hard timeout: a later refresh recovers
	r.drainEntered()
	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseAll()
	bl := assertGetOK(t, ch)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl, 2, "a")
	assertGoroutinesSettled(t, baseline)
}

// with MaxStaleDuration=0 there is no stale window: once FreshDuration elapses
// the snapshot is immediately hard-expired and callers wait for a refresh.
func TestZeroMaxStaleSkipsStaleWindow(t *testing.T) {
	fc := &fakeClock{}
	r := &blockingResolver{name: t.Name(), entered: make(chan struct{}, 16)}
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k1", "a"), nil })
	opts := freshnessOptions()
	opts.MaxStaleDuration = 0
	bf := newBalancerFactory(r, loadbalance.NewWeightedBalancer(), opts)
	bf.clk = fc
	defer bf.Close()

	ch := asyncGet(bf, context.Background())
	r.waitEntered(t)
	r.releaseNext()
	assertGetOK(t, ch)
	waitNoInflight(t, bf)

	// one nanosecond past the fresh duration is already hard-expired
	fc.Advance(opts.FreshDuration + time.Nanosecond)
	bl := cachedBalancer(t, bf)
	bl.mu.Lock()
	test.Assert(t, bl.classifyLocked() == stateExpired)
	bl.mu.Unlock()

	expCh := asyncGet(bf, context.Background())
	r.waitEntered(t)
	assertNoResult(t, expCh) // must wait, not serve the stale snapshot
	test.Assert(t, atomic.LoadInt32(&r.calls) == 2)
	r.setFunc(func(call int) (discovery.Result, error) { return genResult("k2", "b"), nil })
	r.releaseNext()
	bl2 := assertGetOK(t, expCh)
	waitNoInflight(t, bf)
	assertPickerAddrs(t, bl2, 2, "b")
}
