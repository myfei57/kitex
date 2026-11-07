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

package attempttrace

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func assert(t *testing.T, cond bool, msg ...interface{}) {
	t.Helper()
	if !cond {
		t.Fatal(append([]interface{}{"assertion failed: "}, msg...)...)
	}
}

func TestNilCollectorIsNoop(t *testing.T) {
	var c *Collector
	c.MarkLogicalStart("none", "m")
	h := c.ReserveAttempt(TriggerInitial, 0, "")
	assert(t, h == nil)
	h.MarkStarted()
	h.MarkAbandoned()
	h.AbortBeforeStart()
	h.Finish(FinishInfo{Status: StatusSucceeded})
	c.MarkWinner(1)
	c.MarkLogicalReturn(nil)
	c.AppendEvent(EventRetrySuppressed, 0, "x")
	assert(t, c.Snapshot() == nil)
	assert(t, c.Wait(context.Background()) == nil)
	assert(t, c.Truncated() == false)
	assert(t, c.CallID() == 0)
	ch, cancel := c.Subscribe(4)
	_, ok := <-ch
	assert(t, !ok, "nil collector channel should be closed")
	cancel()
}

func TestSequentialLifecycleAndSnapshot(t *testing.T) {
	c := NewCollector()
	c.MarkLogicalStart("Failure", "method")

	h1 := c.ReserveAttempt(TriggerInitial, 0, "initial")
	h1.MarkStarted()
	h1.Finish(FinishInfo{
		Instance: &InstanceInfo{Network: "tcp", Address: "1.1.1.1:8888", Weight: 10},
		Status:   StatusTimedOut,
		ErrMsg:   "timeout",
	})

	h2 := c.ReserveAttempt(TriggerFailureRetry, h1.ID(), "previous attempt timed out")
	h2.MarkStarted()
	h2.Finish(FinishInfo{
		Instance:      &InstanceInfo{Network: "tcp", Address: "2.2.2.2:8888", Weight: 10},
		Status:        StatusSucceeded,
		ResultAdopted: true,
	})
	c.MarkWinner(h2.ID())
	c.MarkLogicalReturn(nil)

	assert(t, c.Wait(context.Background()) == nil)
	snap := c.Snapshot()
	assert(t, snap.LogicalReturned)
	assert(t, snap.Winner == h2.ID())
	assert(t, !snap.Truncated)
	assert(t, len(snap.Attempts) == 2)
	assert(t, snap.Attempts[0].ID == 1 && snap.Attempts[0].Seq == 0)
	assert(t, snap.Attempts[1].ID == 2 && snap.Attempts[1].Seq == 1)
	assert(t, snap.Attempts[0].Status == StatusTimedOut)
	assert(t, snap.Attempts[1].Status == StatusSucceeded)
	assert(t, snap.Attempts[1].ResultAdopted)
	assert(t, !snap.Attempts[0].ResultAdopted)
	assert(t, snap.Attempts[1].TriggeredBy == 1)
	assert(t, snap.Attempts[0].Instance.Address == "1.1.1.1:8888")

	// snapshot must be a deep copy: mutating it does not affect the collector
	snap.Attempts[0].Instance.Address = "changed"
	assert(t, c.Snapshot().Attempts[0].Instance.Address == "1.1.1.1:8888")

	// every attempt reached exactly one terminal state
	for _, a := range snap.Attempts {
		assert(t, a.Status.Terminal(), "attempt should be terminal")
	}
}

func TestFinishIsExactlyOnce(t *testing.T) {
	c := NewCollector()
	h := c.ReserveAttempt(TriggerInitial, 0, "")
	h.MarkStarted()
	h.Finish(FinishInfo{Status: StatusFailed, ErrMsg: "first"})
	h.Finish(FinishInfo{Status: StatusSucceeded, ErrMsg: "second"})
	h.AbortBeforeStart()
	snap := c.Snapshot()
	assert(t, snap.Attempts[0].Status == StatusFailed)
	assert(t, snap.Attempts[0].ErrMsg == "first")
}

func TestUnfinishedVsTerminalAndWait(t *testing.T) {
	c := NewCollector()
	h1 := c.ReserveAttempt(TriggerInitial, 0, "")
	h1.MarkStarted()
	h2 := c.ReserveAttempt(TriggerBackupRequest, h1.ID(), "backup delay")
	h2.MarkStarted()

	// winner returns while h1 is still running
	h2.Finish(FinishInfo{Status: StatusSucceeded, ResultAdopted: true})
	c.MarkWinner(h2.ID())
	c.MarkLogicalReturn(nil)

	snap := c.Snapshot()
	assert(t, snap.LogicalReturned)
	assert(t, snap.Attempts[0].Status == StatusUnfinished, "loser must stay unfinished")
	assert(t, snap.Attempts[0].Abandoned, "loser must be marked abandoned but not terminal")
	assert(t, snap.Attempts[1].Status == StatusSucceeded)

	// Wait must block until the loser finishes
	waitErr := make(chan error, 1)
	go func() { waitErr <- c.Wait(context.Background()) }()
	select {
	case <-waitErr:
		t.Fatal("Wait returned before the in-flight attempt terminated")
	case <-time.After(30 * time.Millisecond):
	}

	h1.Finish(FinishInfo{Status: StatusSucceeded, Abandoned: true})
	select {
	case err := <-waitErr:
		assert(t, err == nil)
	case <-time.After(time.Second):
		t.Fatal("Wait did not unblock after terminal transition")
	}

	snap = c.Snapshot()
	assert(t, snap.Attempts[0].Status == StatusSucceeded)
	assert(t, snap.Attempts[0].Abandoned)
	assert(t, !snap.Attempts[0].ResultAdopted)
}

func TestWaitContextCancel(t *testing.T) {
	c := NewCollector()
	h := c.ReserveAttempt(TriggerInitial, 0, "")
	h.MarkStarted()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := c.Wait(ctx)
	assert(t, errors.Is(err, context.DeadlineExceeded), err)
	h.Finish(FinishInfo{Status: StatusCanceled})
	assert(t, c.Wait(context.Background()) == nil)
}

func TestAbortBeforeStartIsTerminal(t *testing.T) {
	c := NewCollector()
	h := c.ReserveAttempt(TriggerBackupRequest, 1, "")
	h.AbortBeforeStart()
	assert(t, c.Wait(context.Background()) == nil)
	snap := c.Snapshot()
	assert(t, snap.Attempts[0].Status == StatusAborted)
	assert(t, snap.Attempts[0].Status.Terminal())
}

func TestEventTruncationByCount(t *testing.T) {
	c := NewCollector(WithMaxEvents(5), WithMaxBytes(1<<20))
	for i := 0; i < 20; i++ {
		h := c.ReserveAttempt(TriggerInitial, 0, "")
		h.MarkStarted()
		h.Finish(FinishInfo{Status: StatusSucceeded})
	}
	snap := c.Snapshot()
	assert(t, snap.Truncated, "trace should be truncated")
	assert(t, snap.DroppedEvents > 0)
	// structural attempt records are retained regardless of truncation
	assert(t, len(snap.Attempts) == 20)
	// the truncation marker is present
	var hasMarker bool
	for _, ev := range snap.Events {
		if ev.Kind == EventTruncated {
			hasMarker = true
		}
	}
	assert(t, hasMarker)
}

func TestEventTruncationByBytes(t *testing.T) {
	c := NewCollector(WithMaxEvents(10000), WithMaxBytes(400))
	for i := 0; i < 10; i++ {
		h := c.ReserveAttempt(TriggerInitial, 0, "some reasonably long reason string")
		h.MarkStarted()
		h.Finish(FinishInfo{Status: StatusFailed, ErrMsg: "some reasonably long error message"})
	}
	snap := c.Snapshot()
	assert(t, snap.Truncated)
	assert(t, snap.RetainedBytes > 400 || len(snap.Events) < 40)
	// compact status data survives even when verbose strings are blanked
	assert(t, len(snap.Attempts) == 10)
	for _, a := range snap.Attempts {
		assert(t, a.Status.Terminal())
	}
}

func TestSubscribeIncremental(t *testing.T) {
	c := NewCollector()
	ch, cancel := c.Subscribe(8)
	h := c.ReserveAttempt(TriggerInitial, 0, "")
	h.MarkStarted()
	h.Finish(FinishInfo{Status: StatusSucceeded})
	c.MarkWinner(h.ID())
	c.MarkLogicalReturn(nil)

	var kinds []EventKind
	for ev := range ch {
		kinds = append(kinds, ev.Kind)
		if ev.Kind == EventLogicalReturn {
			break
		}
	}
	cancel()
	expect := map[EventKind]bool{
		EventAttemptReserved: false,
		EventAttemptStarted:  false,
		EventAttemptFinished: false,
		EventWinnerSelected:  false,
		EventLogicalReturn:   false,
	}
	for _, k := range kinds {
		if _, ok := expect[k]; ok {
			expect[k] = true
		}
	}
	for k, seen := range expect {
		assert(t, seen, "missing event kind %v", k)
	}
}

func TestSlowWatcherNeverBlocks(t *testing.T) {
	c := NewCollector(WithMaxEvents(50))
	// a one-slot buffer that is never drained: every send after the first
	// must be dropped instead of blocking the producer
	_, cancel := c.Subscribe(1)
	defer cancel()
	stop := make(chan struct{})
	var done int32
	go func() {
		for i := 0; i < 200; i++ {
			c.AppendEvent(EventRetrySuppressed, 0, "flood")
		}
		atomic.StoreInt32(&done, 1)
		close(stop)
	}()
	select {
	case <-stop:
	case <-time.After(time.Second):
		t.Fatal("slow watcher blocked the producer")
	}
	assert(t, atomic.LoadInt32(&done) == 1)
	assert(t, c.Truncated())
}

func TestObserverIsolatedAndRecovered(t *testing.T) {
	// a panicking observer must not break the logical call path
	cPanic := NewCollector(WithObserverWait(time.Second), WithObserver(func(s *Snapshot) {
		panic("boom")
	}))
	h := cPanic.ReserveAttempt(TriggerInitial, 0, "")
	h.Finish(FinishInfo{Status: StatusSucceeded, ResultAdopted: true})
	cPanic.MarkWinner(h.ID())
	cPanic.MarkLogicalReturn(nil) // must not panic

	// a normal observer on another collector still gets delivered
	var wg sync.WaitGroup
	wg.Add(1)
	c := NewCollector(WithObserverWait(time.Second), WithObserver(func(s *Snapshot) {
		defer wg.Done()
		if s.Winner == 0 {
			t.Errorf("winner missing in observer snapshot")
		}
	}))
	h2 := c.ReserveAttempt(TriggerInitial, 0, "")
	h2.Finish(FinishInfo{Status: StatusSucceeded, ResultAdopted: true})
	c.MarkWinner(h2.ID())
	c.MarkLogicalReturn(nil)
	wg.Wait()
}

func TestObserverDeliveredWithInFlightAttempt(t *testing.T) {
	delivered := make(chan struct{})
	c := NewCollector(WithObserverWait(time.Second), WithObserver(func(s *Snapshot) {
		// snapshot taken after wait: all attempts terminal
		for _, a := range s.Attempts {
			if !a.Status.Terminal() {
				t.Errorf("observer saw unfinished attempt")
			}
		}
		close(delivered)
	}))
	h1 := c.ReserveAttempt(TriggerInitial, 0, "")
	h1.MarkStarted()
	h2 := c.ReserveAttempt(TriggerBackupRequest, h1.ID(), "")
	h2.MarkStarted()
	h2.Finish(FinishInfo{Status: StatusSucceeded, ResultAdopted: true})
	c.MarkWinner(h2.ID())
	c.MarkLogicalReturn(nil)
	go func() {
		time.Sleep(20 * time.Millisecond)
		h1.Finish(FinishInfo{Status: StatusSucceeded, Abandoned: true})
	}()
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("observer not delivered")
	}
}

func TestContextPropagation(t *testing.T) {
	c := NewCollector()
	ctx := NewCtxWithCollector(context.Background(), c)
	assert(t, CollectorFromContext(ctx) == c)
	assert(t, CollectorFromContext(context.Background()) == nil)
	assert(t, CollectorFromContext(nil) == nil)
	assert(t, NewCtxWithCollector(ctx, nil) == ctx)
}

func TestConcurrentReserveOrderAndRace(t *testing.T) {
	c := NewCollector(WithMaxEvents(100000), WithMaxBytes(1<<20))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				h := c.ReserveAttempt(TriggerFailureRetry, 1, "r")
				h.MarkStarted()
				time.Sleep(time.Millisecond)
				h.Finish(FinishInfo{Status: StatusFailed, ErrMsg: "e"})
				_ = c.Snapshot()
			}
		}()
	}
	wg.Wait()
	snap := c.Snapshot()
	assert(t, len(snap.Attempts) == 400)
	ids := map[AttemptID]bool{}
	for _, a := range snap.Attempts {
		assert(t, !ids[a.ID], "duplicate attempt id")
		ids[a.ID] = true
		assert(t, a.Status.Terminal())
	}
	assert(t, c.Wait(context.Background()) == nil)
}

func TestTwoCollectorsDoNotInterfere(t *testing.T) {
	c1, c2 := NewCollector(), NewCollector()
	h1 := c1.ReserveAttempt(TriggerInitial, 0, "")
	h1.Finish(FinishInfo{Status: StatusSucceeded})
	c1.MarkLogicalReturn(nil)

	s2 := c2.Snapshot()
	assert(t, len(s2.Attempts) == 0)
	assert(t, !s2.LogicalReturned)
	s1 := c1.Snapshot()
	assert(t, len(s1.Attempts) == 1)
	assert(t, s1.CallID != s2.CallID)
}
