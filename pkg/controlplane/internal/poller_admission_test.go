package internal

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/openai/tunnel-client/pkg/controlplane"
	"github.com/openai/tunnel-client/pkg/types"
)

// finalBatchFetcher returns one batch and cancels the polling context at the
// same instant, then blocks until canceled. It reproduces the shutdown/poll
// race where the last polled batch returns exactly when polling stops.
type finalBatchFetcher struct {
	batch       []controlplane.PolledCommand
	cancelRun   context.CancelFunc
	batchPolled chan struct{}
	once        sync.Once
}

func (f *finalBatchFetcher) Poll(ctx context.Context, limit int) ([]controlplane.PolledCommand, types.TunnelServiceRequestID, error) {
	if len(f.batch) > 0 {
		out := append([]controlplane.PolledCommand(nil), f.batch...)
		f.batch = nil
		if f.cancelRun != nil {
			f.cancelRun()
		}
		f.once.Do(func() { close(f.batchPolled) })
		return out, "", nil
	}
	<-ctx.Done()
	return nil, "", ctx.Err()
}

// TestPollerAdmitsFinalBatchAfterPollingCanceled verifies the shutdown/poll
// race: when the last poll returns in the same moment the run context is
// canceled, every command of the already-pulled batch is still admitted
// exactly once through the admission context.
func TestPollerAdmitsFinalBatchAfterPollingCanceled(t *testing.T) {
	t.Parallel()

	const batchSize = 3
	// Capacity below batch size so the second and third enqueues experience
	// backpressure while the poller is shutting down: a run-context-based
	// enqueue would drop them immediately.
	queue := &chanQueue{ch: make(chan controlplane.PolledCommand, 1)}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	admission, cancelAdmission := context.WithCancel(context.Background())
	defer cancelAdmission()

	fetcher := &finalBatchFetcher{
		batch: []controlplane.PolledCommand{
			stubCommand{id: "1"},
			stubCommand{id: "2"},
			stubCommand{id: "3"},
		},
		cancelRun:   cancelRun,
		batchPolled: make(chan struct{}),
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	meterProvider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })

	p, err := NewPoller(queue, fetcher, logger, meterProvider.Meter("test"), time.Second, 0, 0, 0)
	if err != nil {
		t.Fatalf("new poller: %v", err)
	}
	setter, ok := p.(AdmissionContextSetter)
	if !ok {
		t.Fatalf("poller does not implement AdmissionContextSetter")
	}
	setter.SetAdmissionContext(func() context.Context { return admission })

	runDone := make(chan struct{})
	go func() {
		p.Run(runCtx)
		close(runDone)
	}()

	// Drain on the consumer side with a delay larger than the backpressure
	// retry tick so at least one enqueue must wait under shutdown.
	got := make(map[string]int)
	for range batchSize {
		select {
		case cmd := <-queue.ch:
			got[cmd.RequestID().String()]++
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for a final-batch command")
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not exit after admitting the final batch")
	}

	if len(got) != batchSize {
		t.Fatalf("expected %d distinct admitted commands, got %d", batchSize, len(got))
	}
	for id, count := range got {
		if count != 1 {
			t.Fatalf("command %q admitted %d times, want exactly once", id, count)
		}
	}
}

// TestPollerWithoutAdmissionContextUsesRunContext locks in the legacy
// behavior for callers (and existing graphs) that never configure shutdown:
// canceling run context drops work that cannot be admitted immediately.
func TestPollerWithoutAdmissionContextUsesRunContext(t *testing.T) {
	t.Parallel()

	queue := &chanQueue{ch: make(chan controlplane.PolledCommand, 1)}
	runCtx, cancelRun := context.WithCancel(context.Background())
	fetcher := &finalBatchFetcher{
		batch: []controlplane.PolledCommand{
			stubCommand{id: "1"},
			stubCommand{id: "2"},
		},
		cancelRun:   cancelRun,
		batchPolled: make(chan struct{}),
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	meterProvider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })

	p, err := NewPoller(queue, fetcher, logger, meterProvider.Meter("test"), time.Second, 0, 0, 0)
	if err != nil {
		t.Fatalf("new poller: %v", err)
	}
	p.Run(runCtx)
	// No consumer: with run context canceled, at most the single buffered
	// slot can have accepted a command; the second command must not block.
	if len(queue.ch) > 1 {
		t.Fatalf("legacy poller admitted %d commands after cancellation", len(queue.ch))
	}
}
