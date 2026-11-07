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
	"errors"
	"time"
)

const (
	// defaultRefreshTimeout bounds every refresh Resolve call driven by the
	// freshness policy, so a stuck resolver can never hang a refresh forever.
	defaultRefreshTimeout = 3 * time.Second
)

var (
	// ErrDiscoveryCacheExpired is returned when the cached discovery result has
	// passed the hard staleness boundary (fresh duration + max stale duration)
	// and the latest bounded refresh could not provide a usable result.
	// The underlying refresh error is wrapped and remains reachable via errors.Is.
	ErrDiscoveryCacheExpired = errors.New("discovery cache hard expired: no usable instance snapshot")

	// ErrBalancerFactoryClosed is returned when the BalancerFactory (or the
	// client that owns it) has been closed.
	ErrBalancerFactoryClosed = errors.New("balancer factory has been closed")

	// errStaleGeneration is reported internally when a refresh of an old
	// generation reaches the commit point after the Balancer was closed or
	// rebuilt; its result must never overwrite the current generation.
	errStaleGeneration = errors.New("stale refresh generation discarded")
)

// clock abstracts time reading so the freshness state machine can be driven
// by a fake clock in tests.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// freshnessEnabled reports whether the optional discovery freshness strategy
// is turned on. The zero value (FreshDuration <= 0) keeps the legacy
// periodic-refresh behavior untouched.
func (o Options) freshnessEnabled() bool {
	return o.FreshDuration > 0
}

// freshnessState is the lifecycle state of a cached discovery snapshot,
// evaluated against the time elapsed since the last successful Resolve.
type freshnessState int

const (
	// stateFresh: the snapshot is within FreshDuration since the last successful
	// Resolve and may be served directly.
	stateFresh freshnessState = iota
	// stateStale: FreshDuration has elapsed but the hard boundary
	// (FreshDuration + MaxStaleDuration) has not. The last successful snapshot
	// is still served while a single background refresh is merged in.
	stateStale
	// stateExpired: the hard boundary has been reached (or no cacheable
	// snapshot exists at all). Callers must wait for a bounded refresh or get
	// an explicit failure; long-dead snapshots are never served.
	stateExpired
)
