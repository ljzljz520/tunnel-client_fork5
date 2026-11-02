package shutdown

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPollingStoppedPhases(t *testing.T) {
	t.Parallel()

	c := NewCoordinator()

	select {
	case <-c.PollingStopped():
		t.Fatal("polling must be active on a new coordinator")
	default:
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- WaitForPollingStopped(context.Background(), c)
	}()

	select {
	case err := <-waitErr:
		t.Fatalf("WaitForPollingStopped returned before MarkPollingStopped: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	c.MarkPollingStopped()
	c.MarkPollingStopped() // idempotent

	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("WaitForPollingStopped: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForPollingStopped did not unblock")
	}

	select {
	case <-c.PollingStopped():
	case <-time.After(time.Second):
		t.Fatal("PollingStopped channel not closed")
	}
}

func TestWaitForPollingStoppedNilCoordinator(t *testing.T) {
	t.Parallel()

	if err := WaitForPollingStopped(context.Background(), nil); err != nil {
		t.Fatalf("nil coordinator must report stopped immediately: %v", err)
	}
	if ch := (*Coordinator)(nil).PollingStopped(); ch != nil {
		t.Fatalf("nil coordinator channel must be nil")
	}
	if ch := (*Coordinator)(nil).Done(); ch != nil {
		t.Fatalf("nil coordinator done channel must be nil")
	}
	(*Coordinator)(nil).MarkPollingStopped()
	(*Coordinator)(nil).Finish(DrainResult{})
	(*Coordinator)(nil).RegisterFinalizer("x", nil)
	if errs := (*Coordinator)(nil).RunFinalizers(context.Background()); errs != nil {
		t.Fatalf("nil coordinator finalizers must be empty: %v", errs)
	}
}

func TestStartupPhaseMarkers(t *testing.T) {
	t.Parallel()

	var nilCoord *Coordinator
	if nilCoord.PollingActive() || nilCoord.PipelineStarted() {
		t.Fatal("nil coordinator must report both phases inactive")
	}
	nilCoord.MarkPollingActive()
	nilCoord.MarkPipelineStarted()

	c := NewCoordinator()
	if c.PollingActive() || c.PipelineStarted() {
		t.Fatal("a new coordinator must report the runtime as not started")
	}

	c.MarkPollingActive()
	c.MarkPollingActive()
	if !c.PollingActive() {
		t.Fatal("PollingActive must be true after MarkPollingActive")
	}
	if c.PipelineStarted() {
		t.Fatal("polling must not imply the pipeline started")
	}

	c.MarkPipelineStarted()
	c.MarkPipelineStarted()
	if !c.PipelineStarted() {
		t.Fatal("PipelineStarted must be true after MarkPipelineStarted")
	}

	// Phase markers are independent of drain completion.
	select {
	case <-c.Done():
		t.Fatal("startup markers must not close Done")
	default:
	}
}

func TestAdmissionContextOpenUntilFinish(t *testing.T) {
	t.Parallel()

	c := NewCoordinator()
	admission := c.AdmissionContext()
	if err := admission.Err(); err != nil {
		t.Fatalf("admission context must start open: %v", err)
	}

	c.MarkPollingStopped()
	if err := admission.Err(); err != nil {
		t.Fatalf("stopping polling must not close admission: %v", err)
	}

	c.Finish(DrainResult{})
	if err := admission.Err(); err == nil {
		t.Fatal("admission context must be canceled once the drain ends")
	}
}

func TestFinishFirstResultWins(t *testing.T) {
	t.Parallel()

	c := NewCoordinator()
	first := DrainResult{TimedOut: true, InFlightInterrupted: 2}
	second := DrainResult{Completed: 99}

	c.Finish(first)
	c.Finish(second)

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done not closed after Finish")
	}

	result, ok := c.Result(context.Background())
	if !ok {
		t.Fatal("Result must be available after Finish")
	}
	if !result.TimedOut || result.InFlightInterrupted != 2 {
		t.Fatalf("first result must win, got %+v", result)
	}
	if result.Completed != 0 {
		t.Fatalf("later Finish calls must be ignored, got %+v", result)
	}
}

func TestResultWaitsWithContext(t *testing.T) {
	t.Parallel()

	c := NewCoordinator()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if _, ok := c.Result(ctx); ok {
		t.Fatal("Result must report false while the drain is running")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		c.Finish(DrainResult{Completed: 3})
	}()

	result, ok := c.Result(context.Background())
	if !ok {
		t.Fatal("Result must settle after Finish")
	}
	if result.Completed != 3 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestFinalizersRunOnceWithAggregatedErrors(t *testing.T) {
	t.Parallel()

	c := NewCoordinator()
	var mu sync.Mutex
	var ran []string

	c.RegisterFinalizer("a", func(context.Context) error {
		mu.Lock()
		ran = append(ran, "a")
		mu.Unlock()
		return nil
	})
	c.RegisterFinalizer("b", func(context.Context) error {
		mu.Lock()
		ran = append(ran, "b")
		mu.Unlock()
		return fmt.Errorf("boom")
	})
	c.RegisterFinalizer("", nil) // nil finalizers are ignored

	errs := c.RunFinalizers(context.Background())
	if len(errs) != 1 {
		t.Fatalf("expected one failure, got %d: %v", len(errs), errs)
	}
	if errs[0].Name != "b" || errs[0].Err.Error() != "boom" {
		t.Fatalf("unexpected failure: %+v", errs[0])
	}
	// FinalizerError.Unwrap exposes the underlying error.
	var asTarget FinalizerError
	if !errors.As(error(errs[0]), &asTarget) {
		t.Fatalf("failure must be a FinalizerError, got %T", errs[0])
	}
	if asTarget.Unwrap() == nil || asTarget.Unwrap().Error() != "boom" {
		t.Fatalf("FinalizerError must unwrap to the finalizer's error, got %v", asTarget.Unwrap())
	}
	mu.Lock()
	got := append([]string(nil), ran...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("finalizers must run in registration order, got %v", got)
	}

	// Finalizers execute at most once.
	if errs := c.RunFinalizers(context.Background()); errs != nil {
		t.Fatalf("second RunFinalizers must be empty, got %v", errs)
	}
}

func TestFinalizerReceivesCanceledContext(t *testing.T) {
	t.Parallel()

	c := NewCoordinator()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	observed := make(chan error, 1)
	c.RegisterFinalizer("x", func(ctx context.Context) error {
		observed <- ctx.Err()
		return nil
	})
	c.RunFinalizers(ctx)

	select {
	case err := <-observed:
		if err == nil {
			t.Fatal("finalizer must observe the caller context")
		}
	case <-time.After(time.Second):
		t.Fatal("finalizer did not run")
	}
}

func TestDrainResultCleanAndUnfinishedTotal(t *testing.T) {
	t.Parallel()

	clean := DrainResult{Completed: 4}
	if !clean.Clean() || clean.UnfinishedTotal() != 0 {
		t.Fatalf("completed-only drain must be clean, got %+v", clean)
	}

	interrupted := DrainResult{TimedOut: true, InFlightInterrupted: 1, QueuedUnfinished: 2,
		UnfinishedRequestIDs: []string{"req-1", "req-2"}}
	if interrupted.Clean() {
		t.Fatal("timed-out drain with unfinished work must not be clean")
	}
	if interrupted.UnfinishedTotal() != 3 {
		t.Fatalf("unfinished total = %d, want 3", interrupted.UnfinishedTotal())
	}

	queued := DrainResult{QueuedUnfinished: 1}
	if queued.Clean() {
		t.Fatal("drain with queued leftovers must not be clean")
	}
}

func TestDeadlineContext(t *testing.T) {
	t.Parallel()

	t.Run("parent without deadline gets bounded", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := DeadlineContext(context.Background(), 25*time.Millisecond)
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > 25*time.Millisecond {
			t.Fatalf("unexpected remaining deadline: %v", remaining)
		}
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected deadline exceeded, got %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("bounded context never expired")
		}
	})

	t.Run("drain timeout applies under a later parent deadline", func(t *testing.T) {
		t.Parallel()
		parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
		defer parentCancel()

		ctx, cancel := DeadlineContext(parent, 25*time.Millisecond)
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline")
		}
		if remaining := time.Until(deadline); remaining <= 0 || remaining > 25*time.Millisecond {
			t.Fatalf("drain budget must bound the context, remaining %v", remaining)
		}
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected deadline exceeded, got %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("drain-bounded context never expired")
		}
		if err := parent.Err(); err != nil {
			t.Fatalf("drain expiry must not cancel the parent, got %v", err)
		}
	})

	t.Run("earlier parent deadline caps the drain timeout", func(t *testing.T) {
		t.Parallel()
		parent, parentCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer parentCancel()
		parentDeadline, _ := parent.Deadline()

		ctx, cancel := DeadlineContext(parent, time.Second)
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(parentDeadline) {
			t.Fatalf("parent deadline must cap the drain budget, got %v want %v", deadline, parentDeadline)
		}

		cancel()
		if err := ctx.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel must cancel the derived context, got %v", err)
		}
		if err := parent.Err(); err != nil {
			t.Fatalf("cancel must not cancel the parent, got %v", err)
		}
	})
}
