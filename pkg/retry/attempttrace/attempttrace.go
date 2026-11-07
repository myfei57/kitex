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

// Package attempttrace provides an optional, per-logical-call trace of the
// physical RPC attempts performed by the kitex retry engine (failure retry,
// backup request and mixed retry).
//
// Design contracts:
//
//   - The trace is produced from the real execution path only. Enabling it
//     never triggers an extra selector pick, result-retry callback or RPC
//     send, and never changes the retry decision or the logical result.
//   - Request and response business values are never recorded. Only stable
//     identifiers, instance coordinates, timestamps, completion classes and
//     error messages are retained.
//   - Every retained value is owned by the Collector as immutable data.
//     Pooled objects (RPCInfo, MutableEndpointInfo, discovery.Instance) are
//     never referenced, so recycling the RPCInfo pool cannot corrupt a
//     trace that is still being read.
//   - A Collector is request scoped and travels through the context, so
//     context cancellation and retry-policy updates on the shared retryer
//     can never leak events into another request's trace.
//   - The logical call returning does not imply that every attempt has
//     finished. Backup attempts that lose the race stay in StatusUnfinished
//     until they actually terminate; use Wait or the incremental Subscribe
//     protocol to observe the final terminal states.
//   - Retained events and bytes are bounded. When a bound is reached the
//     trace is marked Truncated and further verbose data is dropped, while
//     the compact, policy-bounded attempt records and the retry control
//     flow are unaffected.
//   - User observers are never invoked inline on the network/retry
//     goroutines: delivery happens on an isolated goroutine with panic
//     recovery, and incremental watchers use non-blocking sends so a slow
//     consumer can never block critical network tasks.
package attempttrace

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/klog"
)

// AttemptID is the stable identifier of a physical attempt within one
// logical call. IDs are assigned in attempt start order (the first physical
// attempt is 1) and never reused, regardless of the order in which
// concurrent backup attempts finish.
type AttemptID uint32

// Trigger describes why a physical attempt was launched.
type Trigger uint8

const (
	// TriggerUnknown is the zero value, it must not be produced by the engine.
	TriggerUnknown Trigger = iota
	// TriggerInitial is the first physical attempt of the logical call.
	TriggerInitial
	// TriggerFailureRetry is launched after a previous attempt failed or
	// returned a retryable business result.
	TriggerFailureRetry
	// TriggerBackupRequest is launched by the backup timer while another
	// attempt is still in flight.
	TriggerBackupRequest
	// TriggerMixedFailure is a failure-driven launch inside the mixed retry
	// policy (which may also produce TriggerBackupRequest attempts).
	TriggerMixedFailure
)

// String implements fmt.Stringer.
func (t Trigger) String() string {
	switch t {
	case TriggerInitial:
		return "initial"
	case TriggerFailureRetry:
		return "failure_retry"
	case TriggerBackupRequest:
		return "backup_request"
	case TriggerMixedFailure:
		return "mixed_failure_retry"
	}
	return "unknown"
}

// Status is the lifecycle state of an attempt.
type Status uint8

const (
	// StatusUnfinished is the explicit non-terminal state: the attempt has
	// been reserved (and possibly started) but has not reached a terminal
	// state when the snapshot is taken.
	StatusUnfinished Status = iota
	// StatusSucceeded means the physical RPC returned without error.
	StatusSucceeded
	// StatusFailed means the physical RPC returned a regular error.
	StatusFailed
	// StatusTimedOut means the physical RPC returned a timeout error.
	StatusTimedOut
	// StatusCanceled means the context was canceled while the attempt ran.
	StatusCanceled
	// StatusPanicked means the attempt goroutine panicked.
	StatusPanicked
	// StatusNoInstance means instance selection failed before any RPC was
	// sent (no instance available / loadbalance error).
	StatusNoInstance
	// StatusAborted means the attempt was reserved but canceled before its
	// physical RPC ever started, because a winner had already been adopted.
	StatusAborted
)

// String implements fmt.Stringer.
func (s Status) String() string {
	switch s {
	case StatusUnfinished:
		return "unfinished"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusTimedOut:
		return "timed_out"
	case StatusCanceled:
		return "canceled"
	case StatusPanicked:
		return "panicked"
	case StatusNoInstance:
		return "no_instance"
	case StatusAborted:
		return "aborted"
	}
	return "unknown"
}

// Terminal reports whether the status is a terminal state.
func (s Status) Terminal() bool {
	return s != StatusUnfinished
}

// EventKind enumerates the fine-grained trace events.
type EventKind uint8

const (
	// EventLogicalStart marks the beginning of a unary logical call.
	EventLogicalStart EventKind = iota
	// EventLogicalReturn marks the return of the logical call; in-flight
	// attempts may still be running afterwards.
	EventLogicalReturn
	// EventAttemptReserved marks the reservation (stable ID assignment,
	// queue point) of a physical attempt.
	EventAttemptReserved
	// EventAttemptStarted marks the physical RPC entering execution.
	EventAttemptStarted
	// EventAttemptFinished marks the terminal transition of an attempt.
	EventAttemptFinished
	// EventAttemptAbandoned marks that a winner was adopted while the
	// attempt was still running; the attempt itself is not terminal yet.
	EventAttemptAbandoned
	// EventWinnerSelected marks which attempt result was adopted.
	EventWinnerSelected
	// EventRetrySuppressed marks that another retry was considered but not
	// launched (circuit breaker, DDL stop, max duration, policy exhausted).
	EventRetrySuppressed
	// EventTruncated marks that an event/byte bound was reached.
	EventTruncated
	// EventWatchDropped notifies a slow incremental watcher that events
	// were dropped for it.
	EventWatchDropped
)

// String implements fmt.Stringer.
func (k EventKind) String() string {
	switch k {
	case EventLogicalStart:
		return "logical_start"
	case EventLogicalReturn:
		return "logical_return"
	case EventAttemptReserved:
		return "attempt_reserved"
	case EventAttemptStarted:
		return "attempt_started"
	case EventAttemptFinished:
		return "attempt_finished"
	case EventAttemptAbandoned:
		return "attempt_abandoned"
	case EventWinnerSelected:
		return "winner_selected"
	case EventRetrySuppressed:
		return "retry_suppressed"
	case EventTruncated:
		return "truncated"
	case EventWatchDropped:
		return "watch_dropped"
	}
	return "unknown"
}

// InstanceInfo is an owned, immutable snapshot of the selected discovery
// instance. It never references the pooled MutableEndpointInfo or the
// discovery.Instance backing the live RPCInfo.
type InstanceInfo struct {
	Network string
	Address string
	Weight  int
}

// TimeSegments captures the queue/connection/execution timeline of an
// attempt. Zero-valued timestamps mean the information was not available
// for that attempt.
type TimeSegments struct {
	// ScheduledAt is when the attempt was reserved by the retry engine
	// (queue point, before any goroutine scheduling).
	ScheduledAt time.Time
	// StartedAt is when the physical RPC call entered execution.
	StartedAt time.Time
	// ConnStart / ConnFinish bracket connection establishment.
	ConnStart  time.Time
	ConnFinish time.Time
	// WriteStart is when the request write began.
	WriteStart time.Time
	// ReadStart is when the response read began.
	ReadStart time.Time
	// FinishedAt is when the physical RPC call returned.
	FinishedAt time.Time
}

// Attempt is an immutable view of one physical attempt.
type Attempt struct {
	// ID is the stable, unique attempt identifier within the logical call.
	ID AttemptID
	// Seq is the zero-based start sequence. It reflects reservation order
	// only, never completion order.
	Seq int
	// Trigger is why this attempt was launched.
	Trigger Trigger
	// TriggeredBy is the causal parent attempt: the previous attempt for a
	// failure retry, the in-flight attempt for a backup request, or zero
	// for the initial attempt.
	TriggeredBy AttemptID
	// TriggerReason is the human-readable reason captured from the real
	// decision (error text, backup delay, suppression message, ...).
	TriggerReason string

	// InstanceSelected reports whether an instance had been selected.
	InstanceSelected bool
	// Instance is the owned instance snapshot; nil when selection failed
	// or had not happened yet.
	Instance *InstanceInfo

	TimeSegments

	// Status is the current lifecycle state (StatusUnfinished while running).
	Status Status
	// ErrMsg is the error message of a failed attempt. It never contains
	// request or response business values.
	ErrMsg string

	// ResultAdopted reports whether this attempt's outcome is the one
	// returned to the caller (including a final error).
	ResultAdopted bool
	// Abandoned reports that a winner was already adopted when this attempt
	// was still running; the attempt's own result is discarded.
	Abandoned bool
}

// Event is an immutable trace event with a global causal sequence number.
type Event struct {
	// Seq is the monotonic event sequence within the logical call.
	Seq uint64
	// Kind is the event kind.
	Kind EventKind
	// At is when the event happened.
	At time.Time
	// AttemptID is the related attempt, or zero for logical-level events.
	AttemptID AttemptID
	// Detail is an optional human-readable detail.
	Detail string
}

// Snapshot is an immutable, deeply copied view of the whole trace at a
// given point in time.
type Snapshot struct {
	// CallID is the process-unique logical call identifier.
	CallID uint64
	// TakenAt is when the snapshot was produced.
	TakenAt time.Time

	// LogicalReturned reports whether the logical call has returned.
	LogicalReturned bool
	// LogicalErr is the error message returned by the logical call, if any.
	LogicalErr string
	// Winner is the adopted attempt ID, or zero if no result was adopted.
	Winner AttemptID

	// Truncated reports that an event/byte bound was reached.
	Truncated bool
	// DroppedEvents counts verbose events dropped after truncation.
	DroppedEvents uint64
	// RetainedBytes is the estimated number of retained bytes.
	RetainedBytes int

	// Attempts are ordered by start sequence, never by completion order.
	Attempts []Attempt
	// Events are ordered by causal sequence.
	Events []Event
}

// Config configures a Collector.
type Config struct {
	// MaxEvents bounds the number of retained verbose events. Zero or
	// negative values fall back to DefaultMaxEvents.
	MaxEvents int
	// MaxBytes bounds the estimated retained bytes. Zero or negative
	// values fall back to DefaultMaxBytes.
	MaxBytes int
	// Observer, when non-nil, is invoked exactly once on an isolated
	// goroutine after the logical call returns (and after waiting at most
	// ObserverWait for in-flight attempts). A panic inside the observer is
	// recovered and never affects the RPC.
	Observer func(*Snapshot)
	// ObserverWait bounds how long the observer delivery waits for
	// in-flight attempts before taking the snapshot. Zero falls back to
	// DefaultObserverWait.
	ObserverWait time.Duration
}

// Option customizes a Config.
type Option func(*Config)

// WithMaxEvents sets the maximum number of retained verbose events.
func WithMaxEvents(n int) Option {
	return func(c *Config) { c.MaxEvents = n }
}

// WithMaxBytes sets the maximum estimated retained bytes.
func WithMaxBytes(n int) Option {
	return func(c *Config) { c.MaxBytes = n }
}

// WithObserver registers an asynchronously delivered trace observer.
func WithObserver(f func(*Snapshot)) Option {
	return func(c *Config) { c.Observer = f }
}

// WithObserverWait sets the maximum wait before delivering the observer
// snapshot while attempts are still in flight.
func WithObserverWait(d time.Duration) Option {
	return func(c *Config) { c.ObserverWait = d }
}

// Default retention and delivery parameters.
const (
	DefaultMaxEvents    = 256
	DefaultMaxBytes     = 32 * 1024
	DefaultObserverWait = 5 * time.Second

	// rough fixed-size estimates used for the retained-byte bound
	estimatedAttemptBytes = 256
	estimatedEventBytes   = 96
)

// FinishInfo is the plain-value terminal description of an attempt,
// produced by the instrumentation layer from the real RPCInfo/error. It
// must not carry pooled pointers or request/response business values.
type FinishInfo struct {
	Instance      *InstanceInfo
	ConnStart     time.Time
	ConnFinish    time.Time
	WriteStart    time.Time
	ReadStart     time.Time
	Status        Status
	ErrMsg        string
	Abandoned     bool
	ResultAdopted bool
}

var callIDCounter uint64

// Collector owns the trace of one logical call. A Collector is safe for
// concurrent use by the goroutines running concurrent attempts.
type Collector struct {
	id  uint64
	cfg Config

	mu              sync.Mutex
	nextID          AttemptID
	attempts        []*attemptState
	events          []Event
	eventSeq        uint64
	retainedBytes   int
	truncated       bool
	droppedEvents   uint64
	winner          AttemptID
	logicalReturned bool
	logicalErr      string
	doneClosed      bool
	done            chan struct{}
	watchers        map[*watcher]struct{}
	observerFired   bool
}

type attemptState struct {
	data     Attempt
	terminal bool
}

type watcher struct {
	ch      chan Event
	lagging bool
}

// NewCollector creates a Collector for one logical call.
func NewCollector(opts ...Option) *Collector {
	cfg := Config{
		MaxEvents:    DefaultMaxEvents,
		MaxBytes:     DefaultMaxBytes,
		ObserverWait: DefaultObserverWait,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return NewCollectorWithConfig(cfg)
}

// NewCollectorWithConfig creates a Collector from an explicit Config,
// applying defaults to unset fields. It is used by client-level wiring.
func NewCollectorWithConfig(cfg Config) *Collector {
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = DefaultMaxEvents
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	if cfg.ObserverWait <= 0 {
		cfg.ObserverWait = DefaultObserverWait
	}
	return &Collector{
		id:       atomic.AddUint64(&callIDCounter, 1),
		cfg:      cfg,
		done:     make(chan struct{}),
		watchers: make(map[*watcher]struct{}),
	}
}

// CallID returns the process-unique logical call identifier.
func (c *Collector) CallID() uint64 {
	if c == nil {
		return 0
	}
	return c.id
}

// MarkLogicalStart records the beginning of the logical call. It is safe
// to call on a nil Collector.
func (c *Collector) MarkLogicalStart(retryType, method string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.appendEventLocked(EventLogicalStart, 0, retryType+":"+method)
	c.mu.Unlock()
}

// ReserveAttempt reserves a physical attempt, assigning its stable ID and
// start sequence atomically. It must be called at the real launch decision
// point so that Seq reflects launch order rather than goroutine scheduling
// or completion order. It is safe to call on a nil Collector.
func (c *Collector) ReserveAttempt(trigger Trigger, triggeredBy AttemptID, reason string) *Handle {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	seq := int(id) - 1
	st := &attemptState{data: Attempt{
		ID:            id,
		Seq:           seq,
		Trigger:       trigger,
		TriggeredBy:   triggeredBy,
		TriggerReason: c.boundedText(reason),
		Status:        StatusUnfinished,
		TimeSegments:  TimeSegments{ScheduledAt: time.Now()},
	}}
	c.attempts = append(c.attempts, st)
	// attempt records are structurally bounded by the retry policy and are
	// always retained even after truncation; only their verbose strings are
	// accounted against the byte budget.
	c.addBytesLocked(estimatedAttemptBytes + len(reason))
	c.appendEventLocked(EventAttemptReserved, id, trigger.String())
	c.mu.Unlock()
	return &Handle{c: c, id: id}
}

// AppendEvent records a logical-level event (e.g. a suppressed retry). It
// is safe to call on a nil Collector.
func (c *Collector) AppendEvent(kind EventKind, attemptID AttemptID, detail string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.appendEventLocked(kind, attemptID, detail)
	c.mu.Unlock()
}

// MarkWinner records the adopted attempt and flags every other still
// running attempt as abandoned without terminalizing them. It is safe to
// call on a nil Collector.
func (c *Collector) MarkWinner(id AttemptID) {
	if c == nil || id == 0 {
		return
	}
	c.mu.Lock()
	if c.winner == 0 {
		c.winner = id
	}
	for _, st := range c.attempts {
		if st.data.ID == id {
			st.data.ResultAdopted = true
			continue
		}
		if !st.terminal && !st.data.Abandoned {
			st.data.Abandoned = true
			c.appendEventLocked(EventAttemptAbandoned, st.data.ID, "winner already selected")
		}
	}
	c.appendEventLocked(EventWinnerSelected, id, "")
	c.mu.Unlock()
}

// MarkLogicalReturn records the logical call return. The logical return
// does not terminalize in-flight attempts. It is safe to call on a nil
// Collector.
func (c *Collector) MarkLogicalReturn(callErr error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.logicalReturned {
		c.mu.Unlock()
		return
	}
	c.logicalReturned = true
	if callErr != nil {
		c.logicalErr = c.boundedText(callErr.Error())
	}
	c.appendEventLocked(EventLogicalReturn, 0, c.logicalErr)
	allDone := c.allTerminalLocked()
	if allDone {
		c.closeDoneLocked()
	}
	observer := c.cfg.Observer
	shouldDispatch := observer != nil && !c.observerFired
	if shouldDispatch {
		c.observerFired = true
	}
	c.mu.Unlock()

	if shouldDispatch {
		c.dispatchObserver(observer)
	}
}

// Wait blocks until every reserved attempt has reached a terminal state,
// the context is canceled, or the observer wait timeout elapses when ctx
// carries one. It returns nil when all attempts are terminal. It is safe
// to call on a nil Collector.
func (c *Collector) Wait(ctx context.Context) error {
	if c == nil {
		return nil
	}
	for {
		c.mu.Lock()
		ch := c.done
		done := c.allTerminalLocked()
		c.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ch:
			// re-check under lock, a close means all attempts terminal
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Subscribe returns an incremental event channel and a cancel function.
// Sends are non-blocking: a slow consumer receives an EventWatchDropped
// marker and misses intermediate events instead of blocking the retry
// goroutines. The channel is closed by the cancel function.
func (c *Collector) Subscribe(buffer int) (<-chan Event, func()) {
	if c == nil {
		ch := make(chan Event)
		close(ch)
		return ch, func() {}
	}
	if buffer <= 0 {
		buffer = 16
	}
	w := &watcher{ch: make(chan Event, buffer)}
	c.mu.Lock()
	c.watchers[w] = struct{}{}
	c.mu.Unlock()
	cancel := func() {
		c.mu.Lock()
		if _, ok := c.watchers[w]; ok {
			delete(c.watchers, w)
			close(w.ch)
		}
		c.mu.Unlock()
	}
	return w.ch, cancel
}

// Snapshot returns a deeply copied, immutable view of the trace. Running
// attempts are reported with StatusUnfinished; they are never silently
// treated as terminated. It is safe to call on a nil Collector.
func (c *Collector) Snapshot() *Snapshot {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	snap := &Snapshot{
		CallID:          c.id,
		TakenAt:         time.Now(),
		LogicalReturned: c.logicalReturned,
		LogicalErr:      c.logicalErr,
		Winner:          c.winner,
		Truncated:       c.truncated,
		DroppedEvents:   c.droppedEvents,
		RetainedBytes:   c.retainedBytes,
		Attempts:        make([]Attempt, 0, len(c.attempts)),
		Events:          make([]Event, 0, len(c.events)),
	}
	for _, st := range c.attempts {
		a := st.data
		if st.data.Instance != nil {
			ins := *st.data.Instance
			a.Instance = &ins
		}
		snap.Attempts = append(snap.Attempts, a)
	}
	snap.Events = append(snap.Events, c.events...)
	c.mu.Unlock()
	return snap
}

// Truncated reports whether the retention bounds were hit.
func (c *Collector) Truncated() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	v := c.truncated
	c.mu.Unlock()
	return v
}

// Handle is a per-attempt instrumentation handle. Its methods are safe for
// concurrent use and all of them are nil-safe, so instrumentation code can
// skip nil checks on the disabled-trace path.
type Handle struct {
	c  *Collector
	id AttemptID
}

// ID returns the stable attempt identifier.
func (h *Handle) ID() AttemptID {
	if h == nil {
		return 0
	}
	return h.id
}

// MarkStarted records that the physical RPC entered execution.
func (h *Handle) MarkStarted() {
	if h == nil {
		return
	}
	h.c.mu.Lock()
	if st := h.c.lookupLocked(h.id); st != nil && st.data.StartedAt.IsZero() {
		st.data.StartedAt = time.Now()
		h.c.appendEventLocked(EventAttemptStarted, h.id, "")
	}
	h.c.mu.Unlock()
}

// MarkAbandoned flags that a winner was adopted while this attempt is
// still running. The attempt remains non-terminal.
func (h *Handle) MarkAbandoned() {
	if h == nil {
		return
	}
	h.c.mu.Lock()
	if st := h.c.lookupLocked(h.id); st != nil && !st.terminal && !st.data.Abandoned {
		st.data.Abandoned = true
		h.c.appendEventLocked(EventAttemptAbandoned, h.id, "winner already selected")
	}
	h.c.mu.Unlock()
}

// AbortBeforeStart terminalizes an attempt that was reserved but whose
// physical RPC never started because a winner had already been adopted.
func (h *Handle) AbortBeforeStart() {
	if h == nil {
		return
	}
	h.finish(FinishInfo{Status: StatusAborted, ErrMsg: "aborted before start: winner already selected"}, true)
}

// Finish terminalizes the attempt exactly once; duplicate calls are
// ignored so every attempt reaches at most one terminal transition.
func (h *Handle) Finish(info FinishInfo) {
	if h == nil {
		return
	}
	h.finish(info, false)
}

func (h *Handle) finish(info FinishInfo, abortBeforeStart bool) {
	h.c.mu.Lock()
	st := h.c.lookupLocked(h.id)
	if st == nil || st.terminal {
		h.c.mu.Unlock()
		return
	}
	now := time.Now()
	st.terminal = true
	d := &st.data
	d.InstanceSelected = info.Instance != nil
	d.Instance = info.Instance // owned snapshot, no pooled pointer
	if !abortBeforeStart {
		if d.StartedAt.IsZero() {
			d.StartedAt = now
		}
		d.ConnStart = info.ConnStart
		d.ConnFinish = info.ConnFinish
		d.WriteStart = info.WriteStart
		d.ReadStart = info.ReadStart
	}
	d.FinishedAt = now
	d.Status = info.Status
	d.ErrMsg = h.c.boundedText(info.ErrMsg)
	d.Abandoned = info.Abandoned || d.Abandoned
	if info.ResultAdopted {
		d.ResultAdopted = true
	}
	h.c.addBytesLocked(len(info.ErrMsg))
	h.c.appendEventLocked(EventAttemptFinished, h.id, info.Status.String())
	allDone := h.c.allTerminalLocked()
	if allDone {
		h.c.closeDoneLocked()
	}
	h.c.mu.Unlock()
}

func (c *Collector) lookupLocked(id AttemptID) *attemptState {
	idx := int(id) - 1
	if idx < 0 || idx >= len(c.attempts) {
		return nil
	}
	return c.attempts[idx]
}

func (c *Collector) allTerminalLocked() bool {
	for _, st := range c.attempts {
		if !st.terminal {
			return false
		}
	}
	return true
}

func (c *Collector) closeDoneLocked() {
	if !c.doneClosed {
		c.doneClosed = true
		close(c.done)
	}
}

// addBytesLocked accounts retained bytes and flags truncation. Structural
// attempt records are never removed; only verbose events and detail strings
// are gated after the bound is reached.
func (c *Collector) addBytesLocked(n int) {
	c.retainedBytes += n
	if c.retainedBytes > c.cfg.MaxBytes && !c.truncated {
		c.markTruncatedLocked()
	}
}

// boundedText returns the text when the byte budget still allows detail
// strings, otherwise an empty string. Structural fields stay intact.
func (c *Collector) boundedText(s string) string {
	if c.truncated || c.retainedBytes+len(s) > c.cfg.MaxBytes {
		return ""
	}
	return s
}

func (c *Collector) appendEventLocked(kind EventKind, attemptID AttemptID, detail string) {
	if c.truncated && kind != EventTruncated {
		c.droppedEvents++
		return
	}
	if len(c.events) >= c.cfg.MaxEvents {
		c.markTruncatedLocked()
		c.droppedEvents++
		return
	}
	c.eventSeq++
	ev := Event{
		Seq:       c.eventSeq,
		Kind:      kind,
		At:        time.Now(),
		AttemptID: attemptID,
		Detail:    c.boundedText(detail),
	}
	c.retainedBytes += estimatedEventBytes + len(detail)
	c.events = append(c.events, ev)
	if c.retainedBytes > c.cfg.MaxBytes {
		c.markTruncatedLocked()
		return
	}
	c.notifyWatchersLocked(ev)
}

func (c *Collector) markTruncatedLocked() {
	if c.truncated {
		return
	}
	c.truncated = true
	c.eventSeq++
	ev := Event{
		Seq:  c.eventSeq,
		Kind: EventTruncated,
		At:   time.Now(),
	}
	c.events = append(c.events, ev)
	c.notifyWatchersLocked(ev)
}

// notifyWatchersLocked performs non-blocking sends only; watchers can never
// slow down the retry/network goroutines.
func (c *Collector) notifyWatchersLocked(ev Event) {
	for w := range c.watchers {
		select {
		case w.ch <- ev:
		default:
			if !w.lagging {
				w.lagging = true
				select {
				case w.ch <- Event{Kind: EventWatchDropped, At: time.Now(), Detail: "watcher too slow"}:
				default:
				}
			}
		}
	}
}

func (c *Collector) dispatchObserver(observer func(*Snapshot)) {
	wait := c.cfg.ObserverWait
	gofunc.GoFunc(context.Background(), func() {
		defer func() {
			if r := recover(); r != nil {
				klog.Errorf("KITEX: attempttrace observer panicked: %v", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		// best effort: deliver the snapshot even if some attempts are still
		// in flight when the wait elapses.
		_ = c.Wait(ctx)
		snap := c.Snapshot()
		observer(snap)
	})
}

type collectorCtxKey struct{}

// NewCtxWithCollector attaches the per-call Collector to the context.
func NewCtxWithCollector(ctx context.Context, c *Collector) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, collectorCtxKey{}, c)
}

// CollectorFromContext returns the Collector attached to the context, or
// nil when attempt tracing is not enabled for this request.
func CollectorFromContext(ctx context.Context) *Collector {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(collectorCtxKey{}).(*Collector); ok {
		return v
	}
	return nil
}
