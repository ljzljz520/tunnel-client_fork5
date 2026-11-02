// Package shutdown defines the graceful-shutdown drain contract shared by the
// control-plane poller, the dispatcher worker pool, and downstream resources
// such as stdio child processes.
//
// The shutdown phases are strictly ordered:
//
//  1. Polling stops: the poller exits and no new commands are fetched.
//  2. Admission closes: the poller's final in-flight batch may still enter the
//     queue while phase one is ending; admission ends only after it exits.
//  3. Bounded drain: every admitted command keeps executing and may post its
//     response until the drain budget expires.
//  4. Resource reclamation: on a clean drain resources shut down gracefully;
//     when the budget expires unfinished work is reported and reclaimed.
//
// The Coordinator lets components living in independent fx lifecycle hooks
// observe those phases without depending on hook ordering.
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrDrainIncomplete wraps drain outcomes that left admitted work unfinished.
// Callers use errors.Is to detect a bounded drain that ended early and
// errors.As to read the DrainResult for reporting.
var ErrDrainIncomplete = errors.New("shutdown drain incomplete")

// DrainIncompleteError reports a timed-out or partial drain. It is returned by
// the shutdown lifecycle so both the CLI and embedded SDKs observe the same
// contract.
type DrainIncompleteError struct {
	Result DrainResult
}

func (e *DrainIncompleteError) Error() string {
	if e == nil {
		return ErrDrainIncomplete.Error()
	}
	var b strings.Builder
	b.WriteString("shutdown drain incomplete: ")
	fmt.Fprintf(&b, "%d admitted command(s) unfinished", e.Result.UnfinishedTotal())
	if e.Result.InFlightInterrupted > 0 {
		fmt.Fprintf(&b, ", %d interrupted in flight", e.Result.InFlightInterrupted)
	}
	if e.Result.QueuedUnfinished > 0 {
		fmt.Fprintf(&b, ", %d still queued", e.Result.QueuedUnfinished)
	}
	if len(e.Result.UnfinishedRequestIDs) > 0 {
		fmt.Fprintf(&b, " (request ids: %s)", strings.Join(e.Result.UnfinishedRequestIDs, ", "))
	}
	return b.String()
}

func (e *DrainIncompleteError) Unwrap() error { return ErrDrainIncomplete }

// AsDrainResult extracts the DrainResult from a drain-incomplete error.
func AsDrainResult(err error) (DrainResult, bool) {
	var incomplete *DrainIncompleteError
	if errors.As(err, &incomplete) {
		return incomplete.Result, true
	}
	return DrainResult{}, false
}

// DefaultDrainTimeout is the default bounded window for finishing admitted
// work after polling stops.
const DefaultDrainTimeout = 30 * time.Second

// DrainResult reports the outcome of a bounded shutdown drain.
type DrainResult struct {
	// TimedOut is true when the drain budget expired before all admitted work
	// completed and resources had to be reclaimed forcefully.
	TimedOut bool
	// Completed counts admitted commands whose processing returned during the
	// drain window, whether or not a response was posted.
	Completed int64
	// InFlightInterrupted counts commands already executing when the budget
	// expired. Their downstream side effects may have happened, so they are
	// never executed a second time.
	InFlightInterrupted int64
	// QueuedUnfinished counts commands admitted from the control plane but
	// never dispatched to a worker before the budget expired.
	QueuedUnfinished int64
	// UnfinishedRequestIDs identifies the interrupted and abandoned commands.
	// The list is capped to keep reports bounded.
	UnfinishedRequestIDs []string
}

// Clean reports whether every admitted command completed.
func (r DrainResult) Clean() bool {
	return !r.TimedOut && r.InFlightInterrupted == 0 && r.QueuedUnfinished == 0
}

// UnfinishedTotal returns how many admitted commands did not complete.
func (r DrainResult) UnfinishedTotal() int64 {
	return r.InFlightInterrupted + r.QueuedUnfinished
}

// Finalizer releases one terminal resource after the drain ends, for example
// the stdio child process. It must honor the provided reclamation context.
type Finalizer func(ctx context.Context) error

// FinalizerError keeps a failed finalizer attached to its resource name.
type FinalizerError struct {
	Name string
	Err  error
}

func (e FinalizerError) Error() string {
	return e.Name + ": " + e.Err.Error()
}

func (e FinalizerError) Unwrap() error { return e.Err }

type namedFinalizer struct {
	name string
	fn   Finalizer
}

// Coordinator serializes the shutdown phases across components that do not
// share a lifecycle hook. A zero value must not be used; construct one with
// NewCoordinator.
type Coordinator struct {
	pollingStoppedOnce sync.Once
	pollingStopped     chan struct{}

	// pollingActive records that the poller OnStart hook ran. When startup
	// fails before that point, polling stopping is immediate by definition
	// and the drain must not wait for a mark that never comes.
	pollingActive atomic.Bool

	// pipelineStarted records that the dispatcher listener OnStart ran. The
	// drain contract (and the stdio termination wait) applies only after the
	// pipeline is up; a failed-start rollback reclaims resources immediately
	// so the original startup error is not masked.
	pipelineStarted atomic.Bool

	admissionMu     sync.Mutex
	admissionCtx    context.Context
	admissionCancel context.CancelFunc

	doneOnce sync.Once
	done     chan struct{}

	resultMu  sync.RWMutex
	result    DrainResult
	resultSet bool

	finalizersMu sync.Mutex
	finalizers   []namedFinalizer
}

// NewCoordinator returns a coordinator ready for the running runtime: polling
// is active and the admission context is open.
func NewCoordinator() *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		pollingStopped:  make(chan struct{}),
		admissionCtx:    ctx,
		admissionCancel: cancel,
		done:            make(chan struct{}),
	}
}

// MarkPollingStopped signals that the poller goroutine has exited and no more
// commands will be admitted. It is idempotent and nil-safe.
func (c *Coordinator) MarkPollingStopped() {
	if c == nil {
		return
	}
	c.pollingStoppedOnce.Do(func() { close(c.pollingStopped) })
}

// PollingStopped is closed once the poller exited.
func (c *Coordinator) PollingStopped() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.pollingStopped
}

// MarkPollingActive records that the poller lifecycle hook started. It is
// idempotent and nil-safe.
func (c *Coordinator) MarkPollingActive() {
	if c == nil {
		return
	}
	c.pollingActive.Store(true)
}

// PollingActive reports whether the poller was ever started.
func (c *Coordinator) PollingActive() bool {
	return c != nil && c.pollingActive.Load()
}

// MarkPipelineStarted records that the command pipeline started and the
// bounded drain contract applies to subsequent shutdowns. It is idempotent
// and nil-safe.
func (c *Coordinator) MarkPipelineStarted() {
	if c == nil {
		return
	}
	c.pipelineStarted.Store(true)
}

// PipelineStarted reports whether the command pipeline reached its OnStart
// hook. A false value during an OnStop hook identifies a failed-start
// rollback rather than a graceful shutdown.
func (c *Coordinator) PipelineStarted() bool {
	return c != nil && c.pipelineStarted.Load()
}

// AdmissionContext remains open while pulled commands may enter the queue.
// Components finishing the poller's final batch select on it instead of the
// already-canceled polling context. It is canceled when the drain ends.
func (c *Coordinator) AdmissionContext() context.Context {
	if c == nil {
		return context.Background()
	}
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	return c.admissionCtx
}

// Finish records the drain outcome and unblocks every observer of Done. The
// first result wins; later calls are ignored. It is idempotent and nil-safe.
func (c *Coordinator) Finish(result DrainResult) {
	if c == nil {
		return
	}
	c.doneOnce.Do(func() {
		c.resultMu.Lock()
		c.result = result
		c.resultSet = true
		c.resultMu.Unlock()
		c.admissionCancel()
		close(c.done)
	})
}

// Done is closed after the drain ended and its outcome was recorded.
func (c *Coordinator) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

// Result returns the recorded drain outcome, waiting up to ctx if necessary.
// A false return means the drain has not ended before ctx expired.
func (c *Coordinator) Result(ctx context.Context) (DrainResult, bool) {
	if c == nil {
		return DrainResult{}, true
	}
	select {
	case <-c.done:
	case <-ctx.Done():
		return DrainResult{}, false
	}
	c.resultMu.RLock()
	defer c.resultMu.RUnlock()
	return c.result, c.resultSet
}

// RegisterFinalizer adds a resource released only after the drain ends. It
// must be registered during construction, before the runtime starts.
func (c *Coordinator) RegisterFinalizer(name string, fn Finalizer) {
	if c == nil || fn == nil {
		return
	}
	c.finalizersMu.Lock()
	defer c.finalizersMu.Unlock()
	c.finalizers = append(c.finalizers, namedFinalizer{name: name, fn: fn})
}

// RunFinalizers executes every registered finalizer once with ctx and returns
// the failures, if any. Each finalizer receives the remaining reclamation
// budget so a stuck resource cannot delay the others indefinitely.
func (c *Coordinator) RunFinalizers(ctx context.Context) []FinalizerError {
	if c == nil {
		return nil
	}
	c.finalizersMu.Lock()
	finalizers := append([]namedFinalizer(nil), c.finalizers...)
	c.finalizers = nil
	c.finalizersMu.Unlock()

	var failures []FinalizerError
	for _, f := range finalizers {
		if err := f.fn(ctx); err != nil {
			failures = append(failures, FinalizerError{Name: f.name, Err: err})
		}
	}
	return failures
}

// WaitForPollingStopped waits until polling stopped or ctx expires. A nil
// coordinator reports polling as immediately stopped.
func WaitForPollingStopped(ctx context.Context, c *Coordinator) error {
	if c == nil {
		return nil
	}
	select {
	case <-c.PollingStopped():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DeadlineContext bounds the drain by timeout, capped by an earlier deadline
// already carried by parent (for example the fx stop deadline). The drain
// budget therefore always applies even when the caller supplied a later stop
// deadline, while never outliving the parent.
func DeadlineContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}
