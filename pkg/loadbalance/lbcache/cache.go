/*
 * Copyright 2021 CloudWeGo Authors
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

// Package lbcache combine balancer with resolver and cache the resolve result
package lbcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cloudwego/kitex/pkg/diagnosis"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/loadbalance"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/utils"
)

const (
	defaultRefreshInterval = 5 * time.Second
	defaultExpireInterval  = 15 * time.Second
)

var (
	balancerFactories    sync.Map // key: resolver name + loadbalance name
	balancerFactoriesSfg singleflight.Group
)

// Options for create builder
type Options struct {
	// refresh discovery result timely
	RefreshInterval time.Duration

	// Balancer expire check interval
	// we need remove idle Balancers for resource saving
	ExpireInterval time.Duration

	// DiagnosisService is used register info for diagnosis
	DiagnosisService diagnosis.Service

	// Cacheable is used to indicate that if the factory could be shared between multi clients
	Cacheable bool

	// FreshDuration enables the optional discovery freshness strategy when
	// positive: a snapshot produced by a successful Resolve is served directly
	// while its age is within FreshDuration. A zero value disables the
	// strategy and keeps the legacy periodic-refresh behavior.
	FreshDuration time.Duration

	// MaxStaleDuration is the extra window after FreshDuration during which the
	// last successful snapshot may still be served while a background refresh is
	// merged in. Beyond FreshDuration + MaxStaleDuration the snapshot hits the
	// hard boundary: callers must wait for a bounded refresh or fail explicitly.
	// Only effective when FreshDuration is positive.
	MaxStaleDuration time.Duration

	// RefreshTimeout bounds every refresh Resolve call issued by the freshness
	// strategy. Refreshes never use an unbounded context.Background(): the
	// managed refresh context is also canceled when the factory is closed.
	// Defaults to defaultRefreshTimeout when the strategy is enabled.
	RefreshTimeout time.Duration
}

func (v *Options) check() {
	if v.RefreshInterval <= 0 {
		v.RefreshInterval = defaultRefreshInterval
	}
	if v.ExpireInterval <= 0 {
		v.ExpireInterval = defaultExpireInterval
	}
	if v.FreshDuration > 0 {
		if v.MaxStaleDuration < 0 {
			v.MaxStaleDuration = 0
		}
		if v.RefreshTimeout <= 0 {
			v.RefreshTimeout = defaultRefreshTimeout
		}
	}
}

// Hookable add hook for rebalancer events
type Hookable interface {
	// register loadbalance rebalance hook for Rebalance events
	RegisterRebalanceHook(func(ch *discovery.Change)) (index int)
	DeregisterRebalanceHook(index int)
	// register loadbalance delete hook for Delete events
	RegisterDeleteHook(func(ch *discovery.Change)) (index int)
	DeregisterDeleteHook(index int)
}

// BalancerFactory get or create a balancer with given target
// if it has the same key(resolver.Target(target)), we will cache and reuse the Balance
type BalancerFactory struct {
	Hookable
	opts       Options
	cache      sync.Map // key -> LoadBalancer
	resolver   discovery.Resolver
	balancer   loadbalance.Loadbalancer
	rebalancer loadbalance.Rebalancer
	sfg        singleflight.Group

	// ctx is the managed parent context of every freshness refresh.
	// It is canceled by Close so that no refresh can outlive the factory.
	ctx    context.Context
	cancel context.CancelFunc
	// stopCh stops the idle-balancer watcher on Close.
	stopCh chan struct{}
	// closeOnce makes Close idempotent.
	closeOnce sync.Once
	// refs counts shared (Cacheable) factories reused across clients; the
	// factory is only torn down when the last reference is released.
	refs int32
	// key is the global cache key of a shared factory, empty otherwise.
	key string
	// clk is overridable in tests to drive the freshness state machine.
	clk clock
}

func cacheKey(resolver, balancer string, opts Options) string {
	// keep the legacy key format untouched when the freshness strategy is off
	key := fmt.Sprintf("%s|%s|{%s %s}", resolver, balancer, opts.RefreshInterval, opts.ExpireInterval)
	if opts.freshnessEnabled() {
		// factories with different freshness configs must never share a cache
		key += fmt.Sprintf("|freshness{%s %s %s}", opts.FreshDuration, opts.MaxStaleDuration, opts.RefreshTimeout)
	}
	return key
}

func newBalancerFactory(resolver discovery.Resolver, balancer loadbalance.Loadbalancer, opts Options) *BalancerFactory {
	ctx, cancel := context.WithCancel(context.Background())
	b := &BalancerFactory{
		opts:     opts,
		resolver: resolver,
		balancer: balancer,
		ctx:      ctx,
		cancel:   cancel,
		stopCh:   make(chan struct{}),
		refs:     1,
		clk:      realClock{},
	}
	if rb, ok := balancer.(loadbalance.Rebalancer); ok {
		hrb := newHookRebalancer(rb)
		b.rebalancer = hrb
		b.Hookable = hrb
	} else {
		b.Hookable = noopHookRebalancer{}
	}
	go b.watcher()
	return b
}

// NewBalancerFactory get or create a balancer factory for balancer instance
// cache key with resolver name, balancer name and options
func NewBalancerFactory(resolver discovery.Resolver, balancer loadbalance.Loadbalancer, opts Options) *BalancerFactory {
	opts.check()
	if !opts.Cacheable {
		return newBalancerFactory(resolver, balancer, opts)
	}
	uniqueKey := cacheKey(resolver.Name(), balancer.Name(), opts)
	val, ok := balancerFactories.Load(uniqueKey)
	if ok {
		f := val.(*BalancerFactory)
		atomic.AddInt32(&f.refs, 1)
		return f
	}
	val, _, _ = balancerFactoriesSfg.Do(uniqueKey, func() (interface{}, error) {
		b := newBalancerFactory(resolver, balancer, opts)
		b.key = uniqueKey
		balancerFactories.Store(uniqueKey, b)
		return b, nil
	})
	return val.(*BalancerFactory)
}

// Close releases all resources held by the factory: it cancels in-flight
// refreshes, stops the idle watcher, deletes every per-target Balancer and
// notifies the rebalancer. Shared (Cacheable) factories are reference counted
// and are only closed when the last client releases them.
func (b *BalancerFactory) Close() error {
	if b.opts.Cacheable && atomic.AddInt32(&b.refs, -1) > 0 {
		return nil
	}
	b.closeOnce.Do(func() {
		// cancel managed refresh contexts first, so blocked resolvers unwind
		b.cancel()
		close(b.stopCh)
		b.cache.Range(func(key, value interface{}) bool {
			b.cache.Delete(key)
			value.(*Balancer).close()
			return true
		})
		if b.key != "" {
			balancerFactories.CompareAndDelete(b.key, b)
		}
	})
	return nil
}

// watch expired balancer
func (b *BalancerFactory) watcher() {
	ticker := time.NewTicker(b.opts.ExpireInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.cache.Range(func(key, value interface{}) bool {
				bl := value.(*Balancer)
				if atomic.CompareAndSwapInt32(&bl.expire, 0, 1) {
					// 1. set expire flag
					// 2. wait next ticker for collect, maybe the balancer is used again
					// (avoid being immediate delete the balancer which had been created recently)
				} else {
					b.cache.Delete(key)
					bl.close()
				}
				return true
			})
		}
	}
}

// cache key with resolver name prefix avoid conflict for balancer
func renameResultCacheKey(res *discovery.Result, resolverName string) {
	res.CacheKey = resolverName + ":" + res.CacheKey
}

// Get create a new balancer if not exists
func (b *BalancerFactory) Get(ctx context.Context, target rpcinfo.EndpointInfo) (*Balancer, error) {
	if err := b.ctx.Err(); err != nil {
		return nil, ErrBalancerFactoryClosed
	}
	desc := b.resolver.Target(ctx, target)
	bl, created, err := b.getOrCreate(ctx, desc)
	if err != nil {
		return nil, err
	}
	// a freshly constructed Balancer already waited on its first successful
	// refresh; only cache hits need the freshness state machine re-evaluated
	if !b.opts.freshnessEnabled() || created {
		return bl, nil
	}
	return bl.getFresh(ctx)
}

func (b *BalancerFactory) getOrCreate(ctx context.Context, desc string) (*Balancer, bool, error) {
	if val, ok := b.cache.Load(desc); ok {
		return val.(*Balancer), false, nil
	}
	val, err, _ := b.sfg.Do(desc, func() (interface{}, error) {
		if v, ok := b.cache.Load(desc); ok {
			// cache may be set already
			return v.(*Balancer), nil
		}
		bl := &Balancer{
			b:      b,
			target: desc,
		}
		if b.opts.freshnessEnabled() {
			// no snapshot yet: perform the first bounded refresh on a managed
			// context and wait for it; failure must not leave a dead entry
			rc := bl.startRefresh()
			if err := bl.waitRefresh(ctx, rc); err != nil {
				return nil, fmt.Errorf("%w: target=%s: %w", ErrDiscoveryCacheExpired, desc, err)
			}
		} else {
			res, err := b.resolver.Resolve(ctx, desc)
			if err != nil {
				return nil, err
			}
			renameResultCacheKey(&res, b.resolver.Name())
			bl.res.Store(res)
			bl.sharedTicker = getSharedTicker(bl, b.opts.RefreshInterval)
		}
		b.cache.Store(desc, bl)
		return bl, nil
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*Balancer), true, nil
}

// snapshot is an immutable generation of a successfully resolved result.
type snapshot struct {
	res       discovery.Result
	gen       uint64
	successAt time.Time // completion time of the successful Resolve
	cacheable bool
}

// refreshCall represents the single in-flight refresh of a target. Multiple
// callers merge onto the same call instead of issuing parallel Resolves.
type refreshCall struct {
	done chan struct{}
	err  error
}

// Balancer same with loadbalance.Loadbalancer but without resolver.Result that
// has been cached
type Balancer struct {
	b            *BalancerFactory
	target       string       // a description returned from the resolver's Target method
	res          atomic.Value // newest and previous discovery result
	expire       int32        // 0 = normal, 1 = expire and collect next ticker
	sharedTicker *utils.SharedTicker

	// freshness policy state (guarded by mu; legacy mode leaves them zero)
	mu       sync.Mutex
	snap     *snapshot
	inflight *refreshCall
	closed   bool
	// gen counts published snapshot generations. A refresh born in an old
	// generation can never commit after the Balancer was closed or rebuilt.
	gen uint64
}

// classifyLocked evaluates the current snapshot against the freshness windows.
// Caller must hold bl.mu.
func (bl *Balancer) classifyLocked() freshnessState {
	snap := bl.snap
	// no snapshot, or a non-cacheable result that must not be reused
	if snap == nil || !snap.cacheable {
		return stateExpired
	}
	age := bl.b.clk.Now().Sub(snap.successAt)
	if age <= bl.b.opts.FreshDuration {
		return stateFresh
	}
	if age <= bl.b.opts.FreshDuration+bl.b.opts.MaxStaleDuration {
		return stateStale
	}
	return stateExpired
}

// getFresh implements the freshness state machine for a cached target.
func (bl *Balancer) getFresh(ctx context.Context) (*Balancer, error) {
	bl.mu.Lock()
	if bl.closed {
		bl.mu.Unlock()
		return nil, ErrBalancerFactoryClosed
	}
	state := bl.classifyLocked()
	var rc *refreshCall
	if state == stateStale || state == stateExpired {
		// at most one refresh per target; concurrent callers merge onto it
		rc = bl.ensureRefreshLocked()
	}
	bl.mu.Unlock()

	switch state {
	case stateFresh, stateStale:
		// fresh: serve directly. stale: serve the last successful snapshot now,
		// the merged refresh keeps running in the background.
		return bl, nil
	default:
		if err := bl.waitRefresh(ctx, rc); err != nil {
			// a caller giving up keeps its own context semantics; the shared
			// refresh is not canceled while other waiters may still need it
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil, err
			}
			if errors.Is(err, ErrBalancerFactoryClosed) {
				return nil, err
			}
			return nil, fmt.Errorf("%w: target=%s: %w", ErrDiscoveryCacheExpired, bl.target, err)
		}
		return bl, nil
	}
}

// startRefresh kicks off the first refresh of a newly created Balancer.
func (bl *Balancer) startRefresh() *refreshCall {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.ensureRefreshLocked()
}

// ensureRefreshLocked returns the running refresh or starts exactly one.
// Caller must hold bl.mu.
func (bl *Balancer) ensureRefreshLocked() *refreshCall {
	if bl.inflight != nil {
		return bl.inflight
	}
	rc := &refreshCall{done: make(chan struct{})}
	bl.inflight = rc
	gen := bl.gen
	go bl.runRefresh(rc, gen)
	return rc
}

// runRefresh executes one Resolve on a managed, bounded context. Refresh
// failures are reported to waiters but never extend the snapshot's freshness.
func (bl *Balancer) runRefresh(rc *refreshCall, gen uint64) {
	ctx, cancel := context.WithTimeout(bl.b.ctx, bl.b.opts.RefreshTimeout)
	defer cancel()
	res, err := bl.b.resolver.Resolve(ctx, bl.target)
	if err == nil {
		renameResultCacheKey(&res, bl.b.resolver.Name())
		err = bl.publish(res, gen)
	}
	if err != nil {
		klog.Warnf("KITEX: discovery refresh failed, key=%s error=%s", bl.target, err.Error())
	}
	bl.mu.Lock()
	rc.err = err
	if bl.inflight == rc {
		bl.inflight = nil
	}
	close(rc.done)
	bl.mu.Unlock()
}

// publish commits a successful Resolve result. The picker cache is updated via
// Rebalance before the new snapshot is published, so a published instance list
// can never be paired with a picker of the previous generation.
func (bl *Balancer) publish(res discovery.Result, gen uint64) error {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.closed {
		return ErrBalancerFactoryClosed
	}
	if gen != bl.gen {
		// late result of an older generation, the target was closed or rebuilt
		return errStaleGeneration
	}
	if res.Cacheable && bl.b.rebalancer != nil {
		if prev, ok := bl.res.Load().(discovery.Result); ok {
			if ch, changed := bl.b.resolver.Diff(res.CacheKey, prev, res); changed {
				bl.b.rebalancer.Rebalance(ch)
			}
		}
	}
	// publish the new generation only after its picker is in place.
	// A successful empty result replaces any previous non-empty set as well:
	// it means "currently no instances", not "keep serving the old list".
	bl.res.Store(res)
	bl.gen++
	bl.snap = &snapshot{
		res:       res,
		gen:       bl.gen,
		successAt: bl.b.clk.Now(),
		// non-cacheable results are stored for the current caller but never
		// turned into a reusable snapshot; the next Get expires immediately
		cacheable: res.Cacheable,
	}
	return nil
}

// waitRefresh waits for the shared refresh. A caller context cancel only stops
// this waiter; the refresh itself runs on the factory context and continues for
// other waiters.
func (bl *Balancer) waitRefresh(ctx context.Context, rc *refreshCall) error {
	// fast path: the refresh may already be done
	select {
	case <-rc.done:
		return rc.err
	default:
	}
	select {
	case <-rc.done:
		return rc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-bl.b.ctx.Done():
		// factory is closing; still prefer a result that landed concurrently
		select {
		case <-rc.done:
			return rc.err
		default:
			return ErrBalancerFactoryClosed
		}
	}
}

func (bl *Balancer) Refresh() {
	res, err := bl.b.resolver.Resolve(context.Background(), bl.target)
	if err != nil {
		klog.Warnf("KITEX: resolver refresh failed, key=%s error=%s", bl.target, err.Error())
		return
	}
	renameResultCacheKey(&res, bl.b.resolver.Name())
	prev := bl.res.Load().(discovery.Result)
	if bl.b.rebalancer != nil {
		if ch, ok := bl.b.resolver.Diff(res.CacheKey, prev, res); ok {
			bl.b.rebalancer.Rebalance(ch)
		}
	}
	// replace previous result
	bl.res.Store(res)
}

// Tick implements the interface utils.TickerTask.
func (bl *Balancer) Tick() {
	bl.Refresh()
}

// GetResult returns the discovery result that the Balancer holds.
func (bl *Balancer) GetResult() (res discovery.Result, ok bool) {
	if v := bl.res.Load(); v != nil {
		return v.(discovery.Result), true
	}
	return
}

// GetPicker equal to loadbalance.Balancer without pass discovery.Result, because we cache the result
func (bl *Balancer) GetPicker() loadbalance.Picker {
	atomic.StoreInt32(&bl.expire, 0)
	res := bl.res.Load().(discovery.Result)
	return bl.b.balancer.GetPicker(res)
}

func (bl *Balancer) close() {
	bl.mu.Lock()
	if bl.closed {
		bl.mu.Unlock()
		return
	}
	// mark closed before touching anything else: a refresh that lands after
	// cleanup must not Rebalance the shared picker cache or overwrite a
	// rebuilt target's generation
	bl.closed = true
	var cacheKey string
	if v, ok := bl.res.Load().(discovery.Result); ok {
		cacheKey = v.CacheKey
	}
	bl.mu.Unlock()

	// notice the under rebalancer to drop the picker of the current generation
	if rb, ok := bl.b.balancer.(loadbalance.Rebalancer); ok {
		// notice to rebalancing
		rb.Delete(discovery.Change{
			Result: discovery.Result{
				Cacheable: true,
				CacheKey:  cacheKey,
			},
		})
	}
	// delete from sharedTicker (legacy mode only)
	if bl.sharedTicker != nil {
		bl.sharedTicker.Delete(bl)
	}
}

const unknown = "unknown"

func Dump() interface{} {
	type instInfo struct {
		Address string
		Weight  int
	}
	cacheDump := make(map[string]interface{})
	balancerFactories.Range(func(key, val interface{}) bool {
		cacheKey := key.(string)
		if bf, ok := val.(*BalancerFactory); ok {
			routeMap := make(map[string]interface{})
			cacheDump[cacheKey] = routeMap
			bf.cache.Range(func(k, v interface{}) bool {
				routeKey := k.(string)
				if bl, ok := v.(*Balancer); ok {
					if dr, ok := bl.res.Load().(discovery.Result); ok {
						insts := make([]instInfo, 0, len(dr.Instances))
						for i := range dr.Instances {
							inst := dr.Instances[i]
							addr := fmt.Sprintf("%s://%s", inst.Address().Network(), inst.Address().String())
							insts = append(insts, instInfo{Address: addr, Weight: inst.Weight()})
						}
						routeMap[routeKey] = insts
					} else {
						routeMap[routeKey] = unknown
					}
				} else {
					routeMap[routeKey] = unknown
				}
				return true
			})
		} else {
			cacheDump[cacheKey] = unknown
		}
		return true
	})
	return cacheDump
}
