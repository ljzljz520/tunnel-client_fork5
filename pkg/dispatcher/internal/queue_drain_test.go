package dispatcherinternal

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/openai/tunnel-client/pkg/controlplane"
	"github.com/openai/tunnel-client/pkg/shutdown"
	"github.com/openai/tunnel-client/pkg/types"
)

// drainProcessor records executions and can delay or block every call.
// Commands are appended to calls only once they truly completed, so the
// slice doubles as an exactly-once/side-effect audit.
type drainProcessor struct {
	mu      sync.Mutex
	calls   []string
	started chan string

	// release, when non-nil, blocks processing until closed or ctx canceled.
	release chan struct{}
	// delay waits briefly when release is nil.
	delay time.Duration
}

func (p *drainProcessor) Process(ctx context.Context, cmd controlplane.PolledCommand) error {
	id := cmd.RequestID().String()
	if p.started != nil {
		select {
		case p.started <- id:
		default:
		}
	}
	if p.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.release:
		}
	} else if p.delay > 0 {
		timer := time.NewTimer(p.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	p.mu.Lock()
	p.calls = append(p.calls, id)
	p.mu.Unlock()
	return nil
}

func (p *drainProcessor) callsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

type drainHarness struct {
	t          *testing.T
	listener   *QueueListener
	queue      controlplane.PolledCommandQueue
	coord      *shutdown.Coordinator
	cancelLoop context.CancelFunc
	baseline   int
}

func newDrainHarness(t *testing.T, proc Processor, queueSize, poolSize int, drainTimeout time.Duration) *drainHarness {
	t.Helper()
	meterProvider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })

	cfg := newTestMCPConfigQueue(t, poolSize)
	queue := make(controlplane.PolledCommandQueue, queueSize)
	listener, err := NewQueueListener(slog.New(slog.NewTextHandler(io.Discard, nil)), proc, queue, cfg, meterProvider)
	if err != nil {
		t.Fatalf("new listener: %v", err)
	}
	coord := shutdown.NewCoordinator()
	listener.ConfigureShutdown(coord, drainTimeout)

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	t.Cleanup(cancelLoop)
	listener.Start(loopCtx)

	return &drainHarness{
		t:          t,
		listener:   listener,
		queue:      queue,
		coord:      coord,
		cancelLoop: cancelLoop,
		baseline:   runtime.NumGoroutine(),
	}
}

// stopDrain drives the coordinated shutdown and returns the recorded result.
// stopBudget bounds the fx-parent context; the drain itself uses the
// configured drain timeout.
func (h *drainHarness) stopDrain(stopBudget time.Duration) shutdown.DrainResult {
	h.t.Helper()
	stopCtx, cancel := context.WithTimeout(context.Background(), stopBudget)
	defer cancel()
	h.listener.BeginShutdown(stopCtx)
	h.coord.MarkPollingStopped()
	h.cancelLoop()
	h.listener.Wait()
	result, ok := h.listener.DrainResult()
	if !ok {
		h.t.Fatal("expected a coordinated drain result")
	}
	return result
}

func (h *drainHarness) requireNoLeftoverWorkers() {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= h.baseline+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("worker goroutine leak: %d now vs %d baseline", runtime.NumGoroutine(), h.baseline)
}

func (h *drainHarness) enqueue(seq int) {
	h.t.Helper()
	h.queue <- newTestCommand(seq)
}

func (h *drainHarness) waitStarted(n int) {
	h.t.Helper()
	proc := h.listener.processor.(*drainProcessor)
	for range n {
		select {
		case <-proc.started:
		case <-time.After(2 * time.Second):
			h.t.Fatal("processor never started")
		}
	}
}

func requireExactlyOnce(t *testing.T, calls []string, want ...int) {
	t.Helper()
	seen := map[string]int{}
	for _, id := range calls {
		seen[id]++
	}
	if len(calls) != len(want) {
		t.Fatalf("executed %d commands %v, want %d", len(calls), calls, len(want))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("command %q executed %d times, want exactly once", id, n)
		}
	}
}

func sortedIDs(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// Slow but responsive commands finish inside the drain budget: clean result.
func TestDrainCompletesInFlightWithinBudget(t *testing.T) {
	t.Parallel()

	proc := &drainProcessor{delay: 40 * time.Millisecond, started: make(chan string, 2)}
	h := newDrainHarness(t, proc, 4, 2, 2*time.Second)

	h.enqueue(0)
	h.enqueue(1)
	h.waitStarted(2)

	result := h.stopDrain(10 * time.Second)
	if !result.Clean() {
		t.Fatalf("expected clean drain, got %+v", result)
	}
	if result.Completed != 2 {
		t.Fatalf("Completed = %d, want 2", result.Completed)
	}
	requireExactlyOnce(t, proc.callsSnapshot(), 0, 1)

	select {
	case <-h.coord.Done():
	default:
		t.Fatal("coordinator not finished after drain")
	}
	reported, ok := h.coord.Result(context.Background())
	if !ok || !reported.Clean() {
		t.Fatalf("coordinator result = %+v ok=%v, want clean", reported, ok)
	}
	h.requireNoLeftoverWorkers()
}

// Commands still queued (or waiting on a saturated pool) when shutdown starts
// are pumped and executed exactly once within the budget.
func TestDrainPumpsQueuedAndPendingCommands(t *testing.T) {
	t.Parallel()

	proc := &drainProcessor{
		delay:   25 * time.Millisecond,
		started: make(chan string, 4),
	}
	h := newDrainHarness(t, proc, 8, 2, 2*time.Second)

	for seq := range 4 {
		h.enqueue(seq)
	}
	// Give the loop time to saturate both workers and block the third
	// Submit; the fourth command must still be sitting in the queue.
	time.Sleep(10 * time.Millisecond)
	if len(h.queue) == 0 {
		t.Fatal("expected at least one command still queued when draining")
	}

	result := h.stopDrain(10 * time.Second)
	if !result.Clean() {
		t.Fatalf("expected clean drain, got %+v", result)
	}
	calls := proc.callsSnapshot()
	requireExactlyOnce(t, calls, 0, 1, 2, 3)
	h.requireNoLeftoverWorkers()
}

// A stuck command exhausts the budget: it is interrupted, reported, and never
// re-executed; all workers are still reclaimed.
func TestDrainBudgetExhaustionInterruptsInFlight(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	proc := &drainProcessor{release: release, started: make(chan string, 1)}
	h := newDrainHarness(t, proc, 8, 1, 80*time.Millisecond)

	h.enqueue(0)
	h.waitStarted(1)

	result := h.stopDrain(10 * time.Second)
	if !result.TimedOut {
		t.Fatalf("expected timed-out drain, got %+v", result)
	}
	if result.InFlightInterrupted != 1 {
		t.Fatalf("InFlightInterrupted = %d, want 1", result.InFlightInterrupted)
	}
	if result.QueuedUnfinished != 0 {
		t.Fatalf("QueuedUnfinished = %d, want 0", result.QueuedUnfinished)
	}
	if got := sortedIDs(result.UnfinishedRequestIDs); len(got) != 1 || got[0] != "req-0" {
		t.Fatalf("UnfinishedRequestIDs = %v, want [req-0]", result.UnfinishedRequestIDs)
	}
	if calls := proc.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("interrupted command must not complete/re-run, got calls %v", calls)
	}

	close(release)
	h.requireNoLeftoverWorkers()
}

// Pool saturated at shutdown plus residual queued work: interrupted in-flight,
// pending Submit, and queued commands are all counted distinctly and never
// executed twice.
func TestDrainReportsQueuedAndPendingUnfinished(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	proc := &drainProcessor{release: release, started: make(chan string, 1)}
	h := newDrainHarness(t, proc, 8, 1, 80*time.Millisecond)

	h.enqueue(0)
	h.enqueue(1)
	h.enqueue(2)
	h.waitStarted(1) // only req-0 is running; req-1 pending Submit, req-2 queued

	result := h.stopDrain(10 * time.Second)
	if !result.TimedOut {
		t.Fatalf("expected timed-out drain, got %+v", result)
	}
	if result.InFlightInterrupted != 1 {
		t.Fatalf("InFlightInterrupted = %d, want 1", result.InFlightInterrupted)
	}
	if result.QueuedUnfinished != 2 {
		t.Fatalf("QueuedUnfinished = %d, want 2 (pending submit + queued)", result.QueuedUnfinished)
	}
	if result.UnfinishedTotal() != 3 {
		t.Fatalf("UnfinishedTotal = %d, want 3", result.UnfinishedTotal())
	}
	// The drain pump promotes the queued command into a pending Submit, so
	// every unfinished command is reported by id.
	ids := sortedIDs(result.UnfinishedRequestIDs)
	if len(ids) != 3 || ids[0] != "req-0" || ids[1] != "req-1" || ids[2] != "req-2" {
		t.Fatalf("UnfinishedRequestIDs = %v, want [req-0 req-1 req-2]", result.UnfinishedRequestIDs)
	}
	if calls := proc.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("unfinished commands must not execute, got %v", calls)
	}

	close(release)
	h.requireNoLeftoverWorkers()
}

// Without a supplied shutdown parent the coordinator cannot drive a drain:
// the listener takes the legacy path and reports no drain result.
func TestDrainWithoutShutdownParentUsesLegacyRelease(t *testing.T) {
	t.Parallel()

	proc := &drainProcessor{delay: 5 * time.Millisecond}
	meterProvider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })
	cfg := newTestMCPConfigQueue(t, 1)
	queue := make(controlplane.PolledCommandQueue, 1)
	listener, err := NewQueueListener(slog.New(slog.NewTextHandler(io.Discard, nil)), proc, queue, cfg, meterProvider)
	if err != nil {
		t.Fatalf("new listener: %v", err)
	}
	listener.ConfigureShutdown(shutdown.NewCoordinator(), time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	cancel()
	listener.Wait()

	if _, ok := listener.DrainResult(); ok {
		t.Fatal("legacy path must not record a drain result")
	}
	if calls := proc.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("legacy cancel path executed commands %v", calls)
	}
}

// Compile-time guard that the harness uses the shared request-id shape.
var _ types.RequestID = types.RequestID("req-0")
