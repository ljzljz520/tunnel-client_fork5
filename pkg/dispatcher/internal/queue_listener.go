package dispatcherinternal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/openai/tunnel-client/pkg/controlplane"
	tclog "github.com/openai/tunnel-client/pkg/log"
	"github.com/openai/tunnel-client/pkg/runtimeconfig"
	"github.com/openai/tunnel-client/pkg/shutdown"
	"github.com/openai/tunnel-client/pkg/tunnelctx"
)

const (
	poolReleaseTimeout = 10 * time.Second
	// interruptUnwindTimeout bounds how long canceled workers may spend
	// unwinding after the drain budget expired before the pool is reclaimed.
	interruptUnwindTimeout = 2 * time.Second
	// maxUnfinishedRequestIDs caps the request-id list attached to a drain
	// report so log lines stay bounded regardless of queue depth.
	maxUnfinishedRequestIDs = 20
)

type workerPool interface {
	Submit(func()) error
	ReleaseTimeout(time.Duration) error
	Cap() int
	Running() int
}

type antsWorkerPool struct {
	base *ants.Pool
}

func (p *antsWorkerPool) Submit(task func()) error {
	return p.base.Submit(task)
}

func (p *antsWorkerPool) ReleaseTimeout(timeout time.Duration) error {
	return p.base.ReleaseTimeout(timeout)
}

func (p *antsWorkerPool) Cap() int { return p.base.Cap() }

func (p *antsWorkerPool) Running() int { return p.base.Running() }

// QueueListener drains the polled command queue and forwards work to the processor
// using a bounded worker pool.
type QueueListener struct {
	logger         *slog.Logger
	processor      Processor
	queue          controlplane.PolledCommandQueue
	pool           workerPool
	metrics        *queueListenerMetrics
	queueHealth    *controlplane.QueueHealth
	activityHealth *ActivityHealth

	listenerWG sync.WaitGroup

	// workCtx is the parent context for every dispatched command. It is
	// deliberately independent of the listener loop context so canceling the
	// loop at shutdown does not interrupt admitted work: commands keep
	// processing (and posting responses) until the bounded drain ends. It is
	// canceled only when workers must be interrupted for reclamation.
	workCtx    context.Context
	workCancel context.CancelFunc

	// Shutdown drain contract. When unset the listener uses the legacy
	// cancel-and-release behavior.
	coordinator  *shutdown.Coordinator
	drainTimeout time.Duration

	shutdownParent chan context.Context

	inflightMu sync.Mutex
	inflight   map[string]int
	inflightWG sync.WaitGroup

	// pending tracks Submit calls whose result is not yet known: a command
	// was dequeued but a saturated pool has not accepted it yet. Shutdown
	// must be able to preempt that wait (the loop's Submit would otherwise
	// block forever once work no longer cancels with the loop context) and
	// must count survivors as unfinished.
	pendingMu      sync.Mutex
	pendingSubmits map[*pendingSubmit]struct{}

	drainActive      atomic.Bool
	completedInDrain atomic.Int64
	drainResultMu    sync.Mutex
	drainResult      *shutdown.DrainResult
}

// pendingSubmit is one in-flight pool.Submit call.
type pendingSubmit struct {
	requestID string
	settled   chan struct{}
	err       error
}

// ObserveHealth attaches observers before Start; it does not change work scheduling.
func (l *QueueListener) ObserveHealth(queue *controlplane.QueueHealth, activity *ActivityHealth) {
	l.queueHealth, l.activityHealth = queue, activity
}

// ConfigureShutdown attaches the shared shutdown coordinator and the bounded
// drain window. It must be called before Start. A nil coordinator preserves
// the legacy immediate-cancel behavior used by isolated tests.
func (l *QueueListener) ConfigureShutdown(coord *shutdown.Coordinator, drainTimeout time.Duration) {
	l.coordinator = coord
	if drainTimeout > 0 {
		l.drainTimeout = drainTimeout
	}
}

// BeginShutdown supplies the fx stop context under which the bounded drain
// runs. It must be called before the listener loop context is canceled.
func (l *QueueListener) BeginShutdown(stopCtx context.Context) {
	if stopCtx == nil {
		stopCtx = context.Background()
	}
	select {
	case l.shutdownParent <- stopCtx:
	default:
	}
}

// DrainResult returns the recorded drain outcome after Wait returned. The
// boolean is false when no coordinated drain ran (legacy shutdown path).
func (l *QueueListener) DrainResult() (shutdown.DrainResult, bool) {
	l.drainResultMu.Lock()
	defer l.drainResultMu.Unlock()
	if l.drainResult == nil {
		return shutdown.DrainResult{}, false
	}
	return *l.drainResult, true
}

type queueListenerMetrics struct {
	workerPoolCapacity  metric.Int64ObservableGauge
	workerPoolOccupancy metric.Int64ObservableGauge
}

func newQueueListenerMetrics(meter metric.Meter, pool workerPool) (*queueListenerMetrics, error) {
	if meter == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil meter")
	}
	if pool == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil worker pool")
	}

	workerPoolCapacity, err := meter.Int64ObservableGauge(
		"dispatcher_worker_pool_capacity",
		metric.WithDescription("Capacity of the dispatcher worker pool."),
		metric.WithUnit("{count}"),
	)
	if err != nil {
		return nil, err
	}

	workerPoolOccupancy, err := meter.Int64ObservableGauge(
		"dispatcher_worker_pool_occupancy",
		metric.WithDescription("Current occupancy (running workers) of the dispatcher worker pool."),
		metric.WithUnit("{count}"),
	)
	if err != nil {
		return nil, err
	}

	if _, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		observer.ObserveInt64(workerPoolCapacity, int64(pool.Cap()))
		observer.ObserveInt64(workerPoolOccupancy, int64(pool.Running()))
		return nil
	}, workerPoolCapacity, workerPoolOccupancy); err != nil {
		return nil, err
	}

	return &queueListenerMetrics{
		workerPoolCapacity:  workerPoolCapacity,
		workerPoolOccupancy: workerPoolOccupancy,
	}, nil
}

type poolFactory func(maxConcurrent int) (workerPool, error)

// NewQueueListener constructs a QueueListener with a worker pool sized according to the MCP configuration.
func NewQueueListener(logger *slog.Logger, processor Processor, queue controlplane.PolledCommandQueue, mcpConfig *runtimeconfig.MCPConfig, meterProvider *sdkmetric.MeterProvider) (*QueueListener, error) {
	return newQueueListener(logger, processor, queue, mcpConfig, meterProvider, func(maxConcurrent int) (workerPool, error) {
		pool, err := ants.NewPool(maxConcurrent)
		if err != nil {
			return nil, fmt.Errorf("dispatcher queue listener: create worker pool: %w", err)
		}
		return &antsWorkerPool{base: pool}, nil
	})
}

func newQueueListener(logger *slog.Logger, processor Processor, queue controlplane.PolledCommandQueue, mcpConfig *runtimeconfig.MCPConfig, meterProvider *sdkmetric.MeterProvider, makePool poolFactory) (*QueueListener, error) {
	if logger == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil logger")
	}
	if processor == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil processor")
	}
	if queue == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil queue")
	}
	if mcpConfig == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil MCP config")
	}
	if mcpConfig.MaxConcurrentRequests <= 0 {
		return nil, fmt.Errorf("dispatcher queue listener: non-positive max concurrent requests")
	}
	if meterProvider == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil meter provider")
	}

	if makePool == nil {
		return nil, fmt.Errorf("dispatcher queue listener: nil pool factory")
	}

	pool, err := makePool(mcpConfig.MaxConcurrentRequests)
	if err != nil {
		return nil, err
	}

	baseLogger := logger.With(tclog.FieldComponent, tclog.ComponentDispatcher)
	metrics, err := newQueueListenerMetrics(meterProvider.Meter("dispatcher"), pool)
	if err != nil {
		return nil, fmt.Errorf("dispatcher queue listener: %w", err)
	}

	workCtx, workCancel := context.WithCancel(context.Background())

	return &QueueListener{
		logger:         baseLogger,
		processor:      processor,
		queue:          queue,
		pool:           pool,
		metrics:        metrics,
		workCtx:        workCtx,
		workCancel:     workCancel,
		shutdownParent: make(chan context.Context, 1),
		inflight:       make(map[string]int),
		pendingSubmits: make(map[*pendingSubmit]struct{}),
	}, nil
}

// Start begins draining the queue until the provided context is canceled or the queue is closed.
func (l *QueueListener) Start(ctx context.Context) {
	l.activityHealth.Accepting(true)
	l.listenerWG.Go(func() {
		l.run(ctx)
	})
}

// Wait blocks until the listener has stopped processing commands.
func (l *QueueListener) Wait() {
	l.listenerWG.Wait()
}

func (l *QueueListener) run(ctx context.Context) {
	defer func() {
		l.activityHealth.Accepting(false)
		l.activityHealth.Stopped()
	}()

	for {
		select {
		case <-ctx.Done():
			l.shutdown(ctx)
			return
		case cmd, ok := <-l.queue:
			if !ok {
				// A closed queue with no coordinator is the legacy test
				// shutdown path: release without a bounded drain.
				l.legacyRelease(ctx)
				return
			}
			l.queueHealth.Dequeued(time.Now())
			l.dispatch(ctx, cmd)
		}
	}
}

// commandContext builds the per-command context from workCtx (not the loop
// context) so admitted commands survive the loop cancellation during a
// bounded drain.
func (l *QueueListener) commandContext(requestID, sessionID string) context.Context {
	cmdCtx := tunnelctx.ContextWithRequestID(l.workCtx, requestID)
	if sessionID != "" {
		cmdCtx = tunnelctx.ContextWithSessionID(cmdCtx, sessionID)
	}
	return cmdCtx
}

func (l *QueueListener) dispatch(loopCtx context.Context, cmd controlplane.PolledCommand) {
	// A saturated pool makes pool.Submit block. The wait is preemptible:
	// when shutdown cancels the loop context the run loop must regain
	// control and enter the drain even while the Submit is still waiting.
	// The drain reconciles that pending submission through inflightWG.
	l.submit(loopCtx, cmd)
}

// submit hands one command to the worker pool. It returns false when waitCtx
// expired before pool.Submit settled; the command stays accounted in inflightWG
// and in the pending set, so the drain either lets it run once the budget
// allows or reports it unfinished. Exactly-once execution is preserved: the
// task runs either via the pool or once inline when Submit itself errors.
func (l *QueueListener) submit(waitCtx context.Context, cmd controlplane.PolledCommand) bool {
	requestID := cmd.RequestID().String()
	sessionID, _ := cmd.SessionID()
	cmdCtx := l.commandContext(requestID, sessionID)
	cmdLogger := tclog.LoggerWithContextIdentifiers(cmdCtx, l.logger)
	task := l.newTask(cmdCtx, cmd, cmdLogger)

	// Account for the task before Submit so a wait for in-flight work cannot
	// observe zero while a task is already accepted by the pool but not yet
	// scheduled.
	l.inflightWG.Add(1)
	handle := l.beginPending(requestID)
	go func() {
		defer l.finishPending(handle)
		if err := l.pool.Submit(task); err != nil {
			handle.err = err
			cmdLogger.ErrorContext(cmdCtx, "failed to submit polled command to worker pool",
				slog.String("error", err.Error()))
			// The command is already off the queue and will not be
			// re-delivered. Run it exactly once inline; the task itself
			// honors the drain-active/context-cancel guard.
			task()
		}
	}()

	select {
	case <-waitCtx.Done():
		return false
	case <-handle.settled:
		return true
	}
}

func (l *QueueListener) beginPending(requestID string) *pendingSubmit {
	h := &pendingSubmit{requestID: requestID, settled: make(chan struct{})}
	l.pendingMu.Lock()
	l.pendingSubmits[h] = struct{}{}
	l.pendingMu.Unlock()
	return h
}

func (l *QueueListener) finishPending(h *pendingSubmit) {
	l.pendingMu.Lock()
	delete(l.pendingSubmits, h)
	l.pendingMu.Unlock()
	close(h.settled)
}

// snapshotPending returns the requests of Submit calls that had not settled
// at the snapshot moment.
func (l *QueueListener) snapshotPending() []*pendingSubmit {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	out := make([]*pendingSubmit, 0, len(l.pendingSubmits))
	for h := range l.pendingSubmits {
		out = append(out, h)
	}
	return out
}

func (l *QueueListener) newTask(cmdCtx context.Context, cmd controlplane.PolledCommand, cmdLogger *slog.Logger) func() {
	requestID := cmd.RequestID().String()
	return func() {
		// A command admitted while the pool was saturated may only obtain a
		// worker after the drain budget expired and workCtx was canceled. It
		// must not execute then: doing so could trigger side effects after
		// the command was reported unfinished. The drain-active guard keeps
		// the legacy (uncoordinated) release path usable: there workCtx may
		// already be canceled while queued tasks are still expected to run
		// and honor the context themselves.
		if l.drainActive.Load() && cmdCtx.Err() != nil {
			l.logger.WarnContext(cmdCtx, "skipping polled command after shutdown reclamation started",
				slog.String("request_id", requestID))
			l.inflightWG.Done()
			return
		}
		l.markInFlight(requestID)
		defer func() {
			l.unmarkInFlight(requestID)
			if l.drainActive.Load() {
				l.completedInDrain.Add(1)
			}
			l.inflightWG.Done()
		}()
		if err := l.processObserved(cmdCtx, cmd); err != nil {
			cmdLogger.WarnContext(cmdCtx, "failed to process polled command",
				slog.String("error", err.Error()))
		}
	}
}

func (l *QueueListener) markInFlight(requestID string) {
	l.inflightMu.Lock()
	l.inflight[requestID]++
	l.inflightMu.Unlock()
}

func (l *QueueListener) unmarkInFlight(requestID string) {
	l.inflightMu.Lock()
	if l.inflight[requestID] <= 1 {
		delete(l.inflight, requestID)
	} else {
		l.inflight[requestID]--
	}
	l.inflightMu.Unlock()
}

func (l *QueueListener) snapshotInFlight() []string {
	l.inflightMu.Lock()
	defer l.inflightMu.Unlock()
	ids := make([]string, 0, len(l.inflight))
	for id := range l.inflight {
		ids = append(ids, id)
	}
	return ids
}

// shutdown runs when the listener loop context is canceled. With a configured
// coordinator it performs the bounded drain advertised by the shared shutdown
// contract; otherwise it falls back to the legacy immediate release.
func (l *QueueListener) shutdown(loopCtx context.Context) {
	parent := l.takeShutdownParent()
	if l.coordinator == nil || parent == nil {
		// Legacy fx stop without a coordinated drain: the old contract
		// canceled in-flight work immediately.
		l.workCancel()
		l.releasePool(loopCtx)
		return
	}

	result := l.drain(parent)

	l.drainResultMu.Lock()
	l.drainResult = &result
	l.drainResultMu.Unlock()

	// Reclaim workers before signaling Done so downstream resources are only
	// stopped after admitted work has truly stopped using them.
	l.workCancel()
	l.releasePool(parent)

	if result.Clean() {
		l.logger.InfoContext(parent, "shutdown drain completed; all admitted commands finished",
			slog.Int64("completed", result.Completed))
	} else {
		attrs := []any{
			slog.Int64("completed", result.Completed),
			slog.Int64("in_flight_interrupted", result.InFlightInterrupted),
			slog.Int64("queued_unfinished", result.QueuedUnfinished),
		}
		if len(result.UnfinishedRequestIDs) > 0 {
			attrs = append(attrs, slog.Any("unfinished_request_ids", result.UnfinishedRequestIDs))
		}
		l.logger.WarnContext(parent, "shutdown drain incomplete; reclaiming resources", attrs...)
	}

	l.coordinator.Finish(result)
}

func (l *QueueListener) takeShutdownParent() context.Context {
	select {
	case parent := <-l.shutdownParent:
		return parent
	default:
		return nil
	}
}

// legacyRelease is the queue-closed shutdown path: let already-queued tasks
// finish under their work context (matching the historical behavior) and only
// then release the pool and cancel work.
func (l *QueueListener) legacyRelease(ctx context.Context) {
	defer l.workCancel()
	if err := l.pool.ReleaseTimeout(poolReleaseTimeout); err != nil {
		l.logger.WarnContext(ctx, "failed to release dispatcher worker pool",
			slog.String("error", err.Error()))
	}
}

func (l *QueueListener) releasePool(parent context.Context) {
	timeout := poolReleaseTimeout
	if parent != nil {
		if deadline, ok := parent.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < timeout {
				timeout = remaining
			}
		}
	}
	if timeout <= 0 {
		// Release must still be invoked to close the pool even when the fx
		// stop deadline has already expired.
		timeout = time.Millisecond
	}
	if err := l.pool.ReleaseTimeout(timeout); err != nil {
		l.logger.WarnContext(parent, "failed to release dispatcher worker pool",
			slog.String("error", err.Error()))
	}
}

// drain executes the bounded shutdown drain:
//
//  1. Wait until polling stopped so the poller's final batch is already in
//     the queue (no work can appear afterwards).
//  2. Pump every queued command into workers while the budget lasts.
//  3. Wait for in-flight commands (processing plus response delivery) until
//     the budget expires. This also reconciles commands whose Submit was
//     waiting on a saturated pool when shutdown started.
//  4. On timeout interrupt the survivors with workCtx cancellation, give them
//     a short unwind window, and report exactly what did not finish.
func (l *QueueListener) drain(parent context.Context) shutdown.DrainResult {
	result := shutdown.DrainResult{}

	// Only wait for the poller when it actually started: on a failed-start
	// rollback its OnStop hook never runs and MarkPollingStopped would never
	// arrive, burning the whole rollback budget and masking the startup error.
	pollingStopped := true
	if l.coordinator.PollingActive() {
		pollingStopped = shutdown.WaitForPollingStopped(parent, l.coordinator) == nil
	}
	if !pollingStopped {
		l.logger.WarnContext(parent, "shutdown drain started before the poller exited; results may be incomplete")
	}

	drainCtx, cancel := shutdown.DeadlineContext(parent, l.drainTimeout)
	defer cancel()
	l.drainActive.Store(true)

	pumpTimedOut := false

	// Polling has stopped by the time this loop pumps, so an empty queue is a
	// stable condition: no producer remains. A command whose Submit cannot
	// settle before the budget expires stays in the pending set and is
	// reconciled (run once or reported) below.
pump:
	for {
		select {
		case <-drainCtx.Done():
			pumpTimedOut = true
			break pump
		default:
		}
		select {
		case cmd, ok := <-l.queue:
			if !ok {
				break pump
			}
			l.queueHealth.Dequeued(time.Now())
			if !l.submit(drainCtx, cmd) {
				pumpTimedOut = true
				break pump
			}
		default:
			break pump
		}
	}

	allDone := l.waitInflight(drainCtx)

	if pollingStopped && !pumpTimedOut && allDone && len(l.queue) == 0 && drainCtx.Err() == nil {
		result.Completed = l.completedInDrain.Load()
		return result
	}

	// Budget expired while work remained (or polling never provably
	// stopped): record exactly what was executing and what was still waiting
	// for a worker at the expiry moment, before interrupting anything.
	// Commands in these sets may have produced side effects, so they are
	// never executed again.
	interrupted := l.snapshotInFlight()
	pending := l.snapshotPending()
	l.workCancel()

	unwindCtx, unwindCancel := boundedContext(parent, interruptUnwindTimeout)
	defer unwindCancel()
	// Blocked Submits unblock once the interrupted workers release their
	// slots; waitInflight then covers both the worker unwind and the pending
	// tasks (which skip under the canceled drain context).
	if !l.waitInflight(unwindCtx) {
		l.logger.WarnContext(parent, "dispatcher workers did not unwind after interruption")
	}

	result.InFlightInterrupted = int64(len(interrupted))
	result.UnfinishedRequestIDs = appendRequestIDs(result.UnfinishedRequestIDs, interrupted, maxUnfinishedRequestIDs)
	pendingIDs := make([]string, 0, len(pending))
	for _, h := range pending {
		pendingIDs = append(pendingIDs, h.requestID)
	}
	result.UnfinishedRequestIDs = appendRequestIDs(result.UnfinishedRequestIDs, pendingIDs, maxUnfinishedRequestIDs)

	result.QueuedUnfinished = int64(len(l.queue)) + int64(len(pending))
	result.Completed = l.completedInDrain.Load()
	// A budget expiry with no unfinished work is only an error when polling
	// never provably stopped (an orchestration failure); otherwise the drain
	// effectively completed.
	result.TimedOut = !pollingStopped || result.UnfinishedTotal() > 0
	return result
}

func (l *QueueListener) waitInflight(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		l.inflightWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func appendRequestIDs(dst, ids []string, capN int) []string {
	for _, id := range ids {
		if len(dst) >= capN {
			return dst
		}
		dst = append(dst, id)
	}
	return dst
}

// boundedContext returns a context bounded by timeout but never outliving the
// parent deadline.
func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if parent == nil {
		return ctx, cancel
	}
	go func() {
		select {
		case <-parent.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (l *QueueListener) processObserved(ctx context.Context, cmd controlplane.PolledCommand) (err error) {
	if l.activityHealth == nil {
		return l.processor.Process(ctx, cmd)
	}
	slot := l.activityHealth.begin(time.Now())
	outcome := &workOutcome{}
	ctx = context.WithValue(ctx, workOutcomeKey{}, outcome)
	finished := false
	defer func() {
		if err != nil {
			recordWorkFailure(ctx, err, 0)
		}
		l.activityHealth.finish(slot, time.Now(), !finished || outcome.failed, outcome.timedOut)
	}()
	err = l.processor.Process(ctx, cmd)
	finished = true
	return err
}
