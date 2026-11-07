/*
 * Copyright 2026 CloudWeGo Authors
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

package retry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/retry/attempttrace"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
)

// attemptMock is a scripted RPCCallFunc. It counts real physical calls,
// attaches a discoverable instance to every RI that reaches execution and
// blocks on the context when a delay is configured.
type attemptMock struct {
	calls int32
	mu    sync.Mutex
	ris   []rpcinfo.RPCInfo
	// fn maps the 1-based physical call number to (delay, error).
	fn func(n int32, ctx context.Context) (time.Duration, error)
}

func (m *attemptMock) rpcCall() RPCCallFunc {
	return func(ctx context.Context, r Retryer, request, response interface{}) (rpcinfo.RPCInfo, error) {
		n := atomic.AddInt32(&m.calls, 1)
		ri := genRPCInfo()
		m.mu.Lock()
		m.ris = append(m.ris, ri)
		m.mu.Unlock()

		delay, err := m.fn(n, ctx)
		if !errors.Is(err, kerrors.ErrNoMoreInstance) && !errors.Is(err, kerrors.ErrLoadbalance) {
			remoteinfo.AsRemoteInfo(ri.To()).SetInstance(
				discovery.NewInstance("tcp", fmt.Sprintf("10.0.0.%d:8888", n), 10, nil))
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ri, ctx.Err()
			}
		}
		return ri, err
	}
}

func (m *attemptMock) collectedRIs() []rpcinfo.RPCInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]rpcinfo.RPCInfo, len(m.ris))
	copy(out, m.ris)
	return out
}

func traceCtx(ctx context.Context, col *attempttrace.Collector) context.Context {
	ctx = rpcinfo.NewCtxWithRPCInfo(ctx, genRPCInfo())
	if col != nil {
		ctx = attempttrace.NewCtxWithCollector(ctx, col)
	}
	return ctx
}

func traceRetryContainer(t *testing.T, p Policy) *Container {
	rc := NewRetryContainer()
	err := rc.Init(map[string]Policy{Wildcard: p}, nil)
	test.Assert(t, err == nil, err)
	return rc
}

// assertEveryAttemptSettled checks that every attempt is either explicitly
// unfinished or terminal (exactly one terminal transition guaranteed by
// Finish's once-only CAS).
func assertEveryAttemptSettled(t *testing.T, snap *attempttrace.Snapshot, expected int) {
	test.Assert(t, len(snap.Attempts) == expected, "attempt count")
	for _, a := range snap.Attempts {
		if a.Status == attempttrace.StatusUnfinished {
			continue
		}
		test.Assert(t, a.Status.Terminal(), "terminal state")
		test.Assert(t, !a.FinishedAt.IsZero(), "terminal attempt must have finish time")
		test.Assert(t, !a.ScheduledAt.IsZero())
		test.Assert(t, !a.StartedAt.IsZero())
	}
}

func TestAttemptTraceFailureRetry(t *testing.T) {
	col := attempttrace.NewCollector()
	rc := traceRetryContainer(t, BuildFailurePolicy(NewFailurePolicy()))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		if n < 3 {
			return 0, kerrors.ErrRPCTimeout
		}
		return 0, nil
	}}
	ctx := traceCtx(context.Background(), col)
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil, err)
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 3, "real RPCCallFunc count")

	test.Assert(t, col.Wait(context.Background()) == nil)
	snap := col.Snapshot()
	test.Assert(t, snap.LogicalReturned)
	test.Assert(t, !snap.Truncated)
	assertEveryAttemptSettled(t, snap, 3)

	test.Assert(t, snap.Attempts[0].Trigger == attempttrace.TriggerInitial)
	test.Assert(t, snap.Attempts[1].Trigger == attempttrace.TriggerFailureRetry)
	test.Assert(t, snap.Attempts[2].Trigger == attempttrace.TriggerFailureRetry)
	test.Assert(t, snap.Attempts[1].TriggeredBy == snap.Attempts[0].ID)
	test.Assert(t, snap.Attempts[2].TriggeredBy == snap.Attempts[1].ID)
	// start sequence reflects launch order, not completion ordering
	for i, a := range snap.Attempts {
		test.Assert(t, a.Seq == i)
		test.Assert(t, a.ID == attempttrace.AttemptID(i+1))
	}
	test.Assert(t, snap.Attempts[0].Status == attempttrace.StatusTimedOut)
	test.Assert(t, snap.Attempts[1].Status == attempttrace.StatusTimedOut)
	test.Assert(t, snap.Attempts[2].Status == attempttrace.StatusSucceeded)
	test.Assert(t, snap.Winner == snap.Attempts[2].ID)
	test.Assert(t, snap.Attempts[2].ResultAdopted)
	test.Assert(t, !snap.Attempts[0].ResultAdopted && !snap.Attempts[1].ResultAdopted)
	for _, a := range snap.Attempts {
		test.Assert(t, a.InstanceSelected && a.Instance != nil)
		test.Assert(t, a.Instance.Address == fmt.Sprintf("10.0.0.%d:8888", a.ID))
	}
}

func TestAttemptTraceBackupWinnerEarlyReturn(t *testing.T) {
	col := attempttrace.NewCollector()
	rc := traceRetryContainer(t, BuildBackupRequest(NewBackupPolicy(20)))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		if n == 1 {
			return 250 * time.Millisecond, nil // slow original
		}
		return 0, nil // fast backup wins
	}}
	ctx := traceCtx(context.Background(), col)
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil, err)
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 2)

	// logical return must NOT be treated as all attempts finished
	snap := col.Snapshot()
	test.Assert(t, snap.LogicalReturned)
	test.Assert(t, len(snap.Attempts) == 2)
	test.Assert(t, snap.Winner == snap.Attempts[1].ID)
	test.Assert(t, snap.Attempts[0].Status == attempttrace.StatusUnfinished,
		"loser still running must be explicitly unfinished")
	test.Assert(t, snap.Attempts[0].Abandoned, "loser flagged abandoned while running")
	test.Assert(t, !snap.Attempts[0].ResultAdopted)
	test.Assert(t, snap.Attempts[1].Status == attempttrace.StatusSucceeded)
	test.Assert(t, snap.Attempts[1].ResultAdopted)
	test.Assert(t, snap.Attempts[1].Trigger == attempttrace.TriggerBackupRequest)
	test.Assert(t, snap.Attempts[1].TriggeredBy == snap.Attempts[0].ID, "causal parent")

	// waitable trace closure: the late attempt reaches a real terminal state
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	test.Assert(t, col.Wait(waitCtx) == nil)
	snap = col.Snapshot()
	assertEveryAttemptSettled(t, snap, 2)
	test.Assert(t, snap.Attempts[0].Status == attempttrace.StatusSucceeded,
		"real wire outcome is preserved")
	test.Assert(t, snap.Attempts[0].Abandoned && !snap.Attempts[0].ResultAdopted)
}

func TestAttemptTraceMixedFailureTrigger(t *testing.T) {
	col := attempttrace.NewCollector()
	rc := traceRetryContainer(t, BuildMixedPolicy(NewMixedPolicy(50)))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		if n == 1 {
			return 10 * time.Millisecond, kerrors.ErrRPCTimeout
		}
		return 0, nil
	}}
	ctx := traceCtx(context.Background(), col)
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil, err)
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 2)
	test.Assert(t, col.Wait(context.Background()) == nil)

	snap := col.Snapshot()
	assertEveryAttemptSettled(t, snap, 2)
	test.Assert(t, snap.Attempts[0].Trigger == attempttrace.TriggerInitial)
	test.Assert(t, snap.Attempts[1].Trigger == attempttrace.TriggerMixedFailure)
	test.Assert(t, snap.Attempts[1].TriggeredBy == snap.Attempts[0].ID)
	test.Assert(t, snap.Attempts[0].Status == attempttrace.StatusTimedOut)
	test.Assert(t, snap.Attempts[1].Status == attempttrace.StatusSucceeded)
	test.Assert(t, snap.Winner == snap.Attempts[1].ID)
}

func TestAttemptTraceInstanceSelectionFailure(t *testing.T) {
	col := attempttrace.NewCollector()
	// generic errors are not retried by the default policy, so opt into
	// error retry to exercise repeated instance-selection failures
	rc := traceRetryContainer(t, BuildFailurePolicy(NewFailurePolicyWithResultRetry(AllErrorRetry())))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		return 0, kerrors.ErrNoMoreInstance
	}}
	ctx := traceCtx(context.Background(), col)
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err != nil)
	test.Assert(t, errors.Is(err, kerrors.ErrNoMoreInstance))
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 3, "all retries exhausted")
	test.Assert(t, col.Wait(context.Background()) == nil)

	snap := col.Snapshot()
	assertEveryAttemptSettled(t, snap, 3)
	for _, a := range snap.Attempts {
		test.Assert(t, a.Status == attempttrace.StatusNoInstance)
		test.Assert(t, !a.InstanceSelected)
		test.Assert(t, a.Instance == nil)
	}
	// the final attempt's error is what the caller receives
	test.Assert(t, snap.Winner == snap.Attempts[2].ID)
	test.Assert(t, snap.Attempts[2].ResultAdopted)
	test.Assert(t, snap.LogicalErr != "")
}

func TestAttemptTraceContextCanceled(t *testing.T) {
	col := attempttrace.NewCollector()
	rc := traceRetryContainer(t, BuildBackupRequest(NewBackupPolicy(20)))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		return time.Hour, nil // unblocked only by ctx cancellation
	}}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = traceCtx(ctx, col)
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, errors.Is(err, context.Canceled), err)
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 2)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	test.Assert(t, col.Wait(waitCtx) == nil)
	snap := col.Snapshot()
	assertEveryAttemptSettled(t, snap, 2)
	for _, a := range snap.Attempts {
		test.Assert(t, a.Status == attempttrace.StatusCanceled, a.Status)
	}
}

func TestAttemptTraceParityWithAndWithout(t *testing.T) {
	// failure scenario
	buildFailure := func() (*Container, *attemptMock) {
		rc := traceRetryContainer(t, BuildFailurePolicy(NewFailurePolicy()))
		m := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
			if n < 3 {
				return 0, kerrors.ErrRPCTimeout
			}
			return 0, nil
		}}
		return rc, m
	}
	rc1, m1 := buildFailure()
	_, _, err1 := rc1.WithRetryIfNeeded(traceCtx(context.Background(), nil), &Policy{}, m1.rpcCall(), genRPCInfo(), nil, nil)
	rc2, m2 := buildFailure()
	col := attempttrace.NewCollector()
	_, _, err2 := rc2.WithRetryIfNeeded(traceCtx(context.Background(), col), &Policy{}, m2.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, (err1 == nil) == (err2 == nil))
	test.Assert(t, atomic.LoadInt32(&m1.calls) == atomic.LoadInt32(&m2.calls))
	test.Assert(t, atomic.LoadInt32(&m2.calls) == 3)
	test.Assert(t, col.Wait(context.Background()) == nil)

	// backup scenario
	buildBackup := func() (*Container, *attemptMock) {
		rc := traceRetryContainer(t, BuildBackupRequest(NewBackupPolicy(10)))
		m := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
			if n == 1 {
				return 100 * time.Millisecond, nil
			}
			return 0, nil
		}}
		return rc, m
	}
	rc3, m3 := buildBackup()
	_, _, err3 := rc3.WithRetryIfNeeded(traceCtx(context.Background(), nil), &Policy{}, m3.rpcCall(), genRPCInfo(), nil, nil)
	rc4, m4 := buildBackup()
	col2 := attempttrace.NewCollector()
	_, _, err4 := rc4.WithRetryIfNeeded(traceCtx(context.Background(), col2), &Policy{}, m4.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, (err3 == nil) == (err4 == nil))
	test.Assert(t, atomic.LoadInt32(&m3.calls) == atomic.LoadInt32(&m4.calls))
	test.Assert(t, atomic.LoadInt32(&m4.calls) == 2)
	test.Assert(t, col2.Wait(context.Background()) == nil)
}

func TestAttemptTraceTruncationKeepsControlFlow(t *testing.T) {
	col := attempttrace.NewCollector(
		attempttrace.WithMaxEvents(1),
		attempttrace.WithMaxBytes(120),
	)
	rc := traceRetryContainer(t, BuildFailurePolicy(NewFailurePolicy()))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		if n < 3 {
			return 0, kerrors.ErrRPCTimeout
		}
		return 0, nil
	}}
	ctx := traceCtx(context.Background(), col)
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil, err, "truncation must not change retry judgment")
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 3)
	test.Assert(t, col.Wait(context.Background()) == nil)

	snap := col.Snapshot()
	test.Assert(t, snap.Truncated)
	// compact attempt records survive truncation and every attempt still settles
	assertEveryAttemptSettled(t, snap, 3)
	test.Assert(t, snap.Winner == snap.Attempts[2].ID)
}

func TestAttemptTracePoolRecyclingSafety(t *testing.T) {
	col := attempttrace.NewCollector()
	rc := traceRetryContainer(t, BuildBackupRequest(NewBackupPolicy(20)))
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		if n == 1 {
			return 120 * time.Millisecond, nil
		}
		return 0, nil
	}}
	ctx := traceCtx(context.Background(), col)
	_, _, err := rc.WithRetryIfNeeded(ctx, &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil, err)
	test.Assert(t, col.Wait(context.Background()) == nil)

	want := make([]string, 0)
	for _, ri := range mock.collectedRIs() {
		if re := remoteinfo.AsRemoteInfo(ri.To()); re != nil && re.GetInstance() != nil {
			want = append(want, re.GetInstance().Address().String())
		}
		// recycle pooled RPCInfo / MutableEndpointInfo after the physical calls
		rpcinfo.PutRPCInfo(ri)
	}
	test.Assert(t, len(want) == 2)

	// the trace remains valid and keeps owned immutable data
	snap := col.Snapshot()
	assertEveryAttemptSettled(t, snap, 2)
	for i, a := range snap.Attempts {
		test.Assert(t, a.Instance != nil && a.Instance.Address == want[i])
	}
	// repeated reads stay stable after the pool memory has been reused logically
	snap2 := col.Snapshot()
	test.Assert(t, snap2.Attempts[0].Instance.Address == snap.Attempts[0].Instance.Address)
}

func TestAttemptTraceRequestIsolationAndPolicyUpdate(t *testing.T) {
	rc := traceRetryContainer(t, BuildBackupRequest(NewBackupPolicy(20)))
	run := func(col *attempttrace.Collector) {
		mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
			if n == 1 {
				return 80 * time.Millisecond, nil
			}
			return 0, nil
		}}
		_, _, err := rc.WithRetryIfNeeded(traceCtx(context.Background(), col), &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
		test.Assert(t, err == nil, err)
		test.Assert(t, col.Wait(context.Background()) == nil)
	}
	col1, col2 := attempttrace.NewCollector(), attempttrace.NewCollector()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); run(col1) }()
	go func() { defer wg.Done(); run(col2) }()
	wg.Wait()

	s1, s2 := col1.Snapshot(), col2.Snapshot()
	test.Assert(t, s1.CallID != s2.CallID)
	test.Assert(t, len(s1.Attempts) == 2 && len(s2.Attempts) == 2)
	for _, a := range s1.Attempts {
		test.Assert(t, a.ID <= 2, "ids must be per-collector")
	}
	for _, a := range s2.Attempts {
		test.Assert(t, a.ID <= 2, "ids must be per-collector")
	}

	// policy update on the shared retryer must not corrupt the finished traces
	rc.NotifyPolicyChange(Wildcard, BuildBackupRequest(NewBackupPolicy(30)))
	s1Again, s2Again := col1.Snapshot(), col2.Snapshot()
	test.Assert(t, len(s1Again.Attempts) == 2 && len(s2Again.Attempts) == 2)
	test.Assert(t, s1Again.Winner == s1.Winner && s2Again.Winner == s2.Winner)
}

func TestAttemptTraceFastPaths(t *testing.T) {
	// case 1: no retry policy at all -> one physical attempt
	col := attempttrace.NewCollector()
	rc := NewRetryContainer()
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) { return 0, nil }}
	_, recycle, err := rc.WithRetryIfNeeded(traceCtx(context.Background(), col), &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil && recycle)
	test.Assert(t, atomic.LoadInt32(&mock.calls) == 1)
	test.Assert(t, col.Wait(context.Background()) == nil)
	snap := col.Snapshot()
	assertEveryAttemptSettled(t, snap, 1)
	test.Assert(t, snap.Winner == snap.Attempts[0].ID)
	test.Assert(t, snap.Attempts[0].Trigger == attempttrace.TriggerInitial)

	// case 2: retry policy present but AllowRetry false (MaxRetryTimes=0)
	col2 := attempttrace.NewCollector()
	bp := NewBackupPolicy(10)
	bp.StopPolicy.MaxRetryTimes = 0
	rc2 := traceRetryContainer(t, BuildBackupRequest(bp))
	mock2 := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		return 0, kerrors.ErrRPCTimeout
	}}
	_, _, err = rc2.WithRetryIfNeeded(traceCtx(context.Background(), col2), &Policy{}, mock2.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err != nil)
	test.Assert(t, atomic.LoadInt32(&mock2.calls) == 1, "retry must not be launched")
	test.Assert(t, col2.Wait(context.Background()) == nil)
	snap2 := col2.Snapshot()
	assertEveryAttemptSettled(t, snap2, 1)
	var hasSuppressed bool
	for _, ev := range snap2.Events {
		if ev.Kind == attempttrace.EventRetrySuppressed {
			hasSuppressed = true
		}
	}
	test.Assert(t, hasSuppressed, "retry suppression should be recorded")
}

func TestAttemptTraceConcurrentRace(t *testing.T) {
	// per-goroutine containers keep each circuit-breaker panel below its
	// minimum sample count, so retry decisions stay deterministic under race
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rc := traceRetryContainer(t, BuildFailurePolicy(NewFailurePolicy()))
			col := attempttrace.NewCollector()
			mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
				if n < 3 {
					return 0, kerrors.ErrRPCTimeout
				}
				return 0, nil
			}}
			_, _, err := rc.WithRetryIfNeeded(traceCtx(context.Background(), col), &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
			test.Assert(t, err == nil, err)
			test.Assert(t, atomic.LoadInt32(&mock.calls) == 3)
			test.Assert(t, col.Wait(context.Background()) == nil)
			snap := col.Snapshot()
			assertEveryAttemptSettled(t, snap, 3)
			test.Assert(t, snap.Winner == snap.Attempts[2].ID)
			// concurrent reads must be safe
			_ = col.Truncated()
		}()
	}
	wg.Wait()
}

func TestAttemptTraceSubscribeBoundedIncremental(t *testing.T) {
	col := attempttrace.NewCollector()
	rc := traceRetryContainer(t, BuildFailurePolicy(NewFailurePolicy()))
	ch, cancel := col.Subscribe(32)
	defer cancel()
	mock := &attemptMock{fn: func(n int32, _ context.Context) (time.Duration, error) {
		if n < 3 {
			return 0, kerrors.ErrRPCTimeout
		}
		return 0, nil
	}}
	_, _, err := rc.WithRetryIfNeeded(traceCtx(context.Background(), col), &Policy{}, mock.rpcCall(), genRPCInfo(), nil, nil)
	test.Assert(t, err == nil, err)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	var logicalReturns int
loop:
	for {
		select {
		case ev := <-ch:
			if ev.Kind == attempttrace.EventLogicalReturn {
				logicalReturns++
				break loop
			}
		case <-waitCtx.Done():
			t.Fatal("did not receive logical return event")
		}
	}
	test.Assert(t, logicalReturns == 1)
}
