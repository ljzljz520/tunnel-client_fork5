package mcpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/fx"

	tclog "github.com/openai/tunnel-client/pkg/log"
	"github.com/openai/tunnel-client/pkg/runtimeconfig"
	"github.com/openai/tunnel-client/pkg/shutdown"
)

type stdioCommandTransport struct {
	logger                *slog.Logger
	lifecycle             fx.Lifecycle
	shutdowner            fx.Shutdowner
	observation           *ProtocolObservation
	observationGeneration string

	// coordinator, when set, delays child termination until the bounded
	// shutdown drain ended so admitted commands finish (or are reported)
	// before the stdio process they depend on is reclaimed.
	coordinator *shutdown.Coordinator

	mu              sync.Mutex
	commandArgs     []string
	policy          runtimeconfig.StdioFailurePolicy
	generation      uint64
	childStartedAt  time.Time
	restartDelay    time.Duration
	active          bool
	recovering      bool
	recoveryCancel  context.CancelFunc
	recoveryDone    chan struct{}
	onChildLost     func()
	onChildStarted  func(uint64, int)
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	stdout          io.ReadCloser
	transport       *mcp.IOTransport
	waitDone        chan error
	signals         chan os.Signal
	started         bool
	stopping        bool
	signalForwarded bool

	commandLabel string
	shutdownOnce sync.Once
}

// A failed child is retired before a replacement starts. The delay also
// bounds a crash loop without holding a dispatcher request open.
const (
	stdioRestartInitialDelay = 100 * time.Millisecond
	stdioRestartMaxDelay     = 5 * time.Second
	stdioRestartStableTime   = 30 * time.Second
	stdioChildStopGrace      = time.Second
)

var errStdioChildRestarting = errors.New("mcpclient: stdio MCP command is restarting")

// The shared transport must identify the generation of the connection it got,
// even if the child exits between Connect returning and its caller resuming.
type stdioGenerationConnection struct {
	mcp.Connection
	generation uint64
}

func (c *stdioGenerationConnection) physicalGeneration() (uint64, bool) {
	return c.generation, true
}

// StdioRuntimeInfo describes the active stdio MCP process details.
type StdioRuntimeInfo struct {
	PID     int    `json:"pid,omitempty"`
	Command string `json:"command,omitempty"`
}

// StdioRuntimeInfoProvider exposes runtime details for stdio transport.
type StdioRuntimeInfoProvider interface {
	StdioRuntimeInfo() StdioRuntimeInfo
}

func newStdioCommandTransport(logger *slog.Logger, lifecycle fx.Lifecycle, shutdowner fx.Shutdowner) *stdioCommandTransport {
	if logger != nil {
		logger = logger.With(
			slog.String(tclog.FieldComponent, tclog.ComponentMcpClient),
			slog.String("transport", "stdio"),
		)
	}
	return &stdioCommandTransport{
		logger:     logger,
		lifecycle:  lifecycle,
		shutdowner: shutdowner,
	}
}

func (t *stdioCommandTransport) Transport(cfg *runtimeconfig.MCPConfig) (mcp.Transport, error) {
	if cfg == nil {
		return nil, errors.New("mcpclient: mcp config is required for stdio transport")
	}
	if len(cfg.CommandArgs) == 0 {
		return nil, errors.New("mcpclient: mcp.command is required for stdio transport")
	}
	if t.lifecycle == nil {
		return nil, errors.New("mcpclient: lifecycle is required for stdio transport")
	}
	if cfg.StdioFailurePolicy != "" && cfg.StdioFailurePolicy != runtimeconfig.StdioFailurePolicyRestart && cfg.StdioFailurePolicy != runtimeconfig.StdioFailurePolicyExit {
		return nil, errors.New("mcpclient: invalid stdio failure policy")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.transport != nil {
		return t, nil
	}

	t.commandArgs = append([]string(nil), cfg.CommandArgs...)
	t.policy = cfg.StdioFailurePolicy
	t.commandLabel = strings.TrimSpace(cfg.Command)
	if t.commandLabel == "" {
		t.commandLabel = strings.Join(cfg.CommandArgs, " ")
	}
	t.generation = 1
	cmd, stdin, stdout, transport, err := t.prepareChild(t.generation)
	if err != nil {
		return nil, err
	}
	t.cmd, t.stdin, t.stdout, t.transport = cmd, stdin, stdout, transport
	t.waitDone = make(chan error, 1)

	t.lifecycle.Append(fx.Hook{
		OnStart: t.start,
		OnStop:  t.stop,
	})

	return t, nil
}

// Connect never waits for a replacement child. This lets each failed logical
// request complete and allows the next request to use a fresh MCP session.
func (t *stdioCommandTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	t.mu.Lock()
	if !t.active || t.stopping || t.transport == nil {
		t.mu.Unlock()
		return nil, errStdioChildRestarting
	}
	transport := t.transport
	generation := t.generation
	t.mu.Unlock()

	conn, err := transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	stillCurrent := t.active && !t.stopping && t.generation == generation
	t.mu.Unlock()
	if !stillCurrent {
		_ = conn.Close()
		return nil, errStdioChildRestarting
	}
	return &stdioGenerationConnection{Connection: conn, generation: generation}, nil
}

func (t *stdioCommandTransport) Generation() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.generation
}

func (t *stdioCommandTransport) SetOnChildLost(callback func()) {
	t.mu.Lock()
	t.onChildLost = callback
	t.mu.Unlock()
}

func (t *stdioCommandTransport) prepareChild(generation uint64) (*exec.Cmd, io.WriteCloser, io.ReadCloser, *mcp.IOTransport, error) {
	cmd := exec.Command(t.commandArgs[0], t.commandArgs[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("mcpclient: stdio command stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, nil, nil, nil, fmt.Errorf("mcpclient: stdio command stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	transport := &mcp.IOTransport{
		Reader: &stdioEOFReader{reader: stdout, onEOF: func() {
			t.childLost(generation, "stdio MCP command stdout closed", io.EOF, "child_transport_closed")
		}},
		Writer: &stdioErrWriter{writer: stdin, onError: func(err error) {
			t.childLost(generation, "stdio MCP command stdin write failed", err, "child_transport_closed")
		}},
	}
	return cmd, stdin, stdout, transport, nil
}

func (t *stdioCommandTransport) start(ctx context.Context) error {
	t.mu.Lock()
	cmd := t.cmd
	if cmd == nil || t.started {
		t.mu.Unlock()
		return nil
	}
	if err := cmd.Start(); err != nil {
		_ = t.stdin.Close()
		_ = t.stdout.Close()
		t.mu.Unlock()
		return fmt.Errorf("mcpclient: start stdio command: %w", err)
	}
	t.started = true
	t.active = true
	t.childStartedAt = time.Now()
	if t.observation != nil {
		t.observationGeneration = t.observation.beginChild()
		t.observation.childStarted(t.observationGeneration)
	}
	waitDone := t.waitDone
	generation := t.generation
	onChildStarted := t.onChildStarted
	t.mu.Unlock()

	t.logInfo(ctx, "stdio MCP command started")
	t.startSignalForwarding()
	if onChildStarted != nil {
		onChildStarted(generation, cmd.Process.Pid)
	}
	go t.waitForExit(cmd, waitDone, generation)
	return nil
}

func (t *stdioCommandTransport) stop(ctx context.Context) error {
	t.mu.Lock()
	t.stopping = true
	t.active = false
	started := t.started
	cancel := t.recoveryCancel
	recoveryDone := t.recoveryDone
	observationGeneration := t.observationGeneration
	t.mu.Unlock()
	t.stopSignalForwarding()

	if cancel != nil {
		cancel()
	}

	// Outlive polling cancellation: admitted commands may still be executing
	// against this child during the bounded shutdown drain. Hook registration
	// order runs the dispatcher drain hook before this one, so Done is
	// normally already closed here; the ctx branch is only a backstop so a
	// misconfigured graph cannot hang shutdown. Termination proceeds no
	// matter which branch wins so the child is never orphaned.
	//
	// When the pipeline never reached its OnStart hook this is a failed-start
	// rollback: the drain hook will not run and Done never closes, so reclaim
	// the child immediately instead of burning the rollback budget and
	// masking the original startup error.
	if t.coordinator != nil && t.coordinator.PipelineStarted() {
		select {
		case <-t.coordinator.Done():
		case <-ctx.Done():
		}
	}

	// Let a concurrent recovery settle (it refuses to spawn replacements
	// once stopping is set) before deciding which process to terminate.
	if recoveryDone != nil {
		select {
		case <-recoveryDone:
		case <-ctx.Done():
		}
	}

	t.mu.Lock()
	cmd := t.cmd
	waitDone := t.waitDone
	stdin := t.stdin
	stdout := t.stdout
	t.mu.Unlock()

	if cmd == nil || !started {
		return nil
	}
	if t.observation != nil {
		t.observation.childClosed(observationGeneration, "child_stopped")
	}

	return t.terminateChild(ctx, cmd, waitDone, stdin, stdout)
}

// terminateChild closes the child pipes, asks the process to exit gracefully,
// and force-kills it if it ignores SIGTERM or the reclamation budget expires.
// It always reaps the process so shutdown never leaves an orphan.
func (t *stdioCommandTransport) terminateChild(ctx context.Context, cmd *exec.Cmd, waitDone chan error, stdin io.WriteCloser, stdout io.ReadCloser) error {
	if stdin != nil {
		_ = stdin.Close()
	}
	if stdout != nil {
		_ = stdout.Close()
	}
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	_ = cmd.Process.Signal(syscall.SIGTERM)
	if waitDone == nil {
		return nil
	}

	grace := time.NewTimer(stdioChildStopGrace)
	defer grace.Stop()
	select {
	case <-waitDone:
		t.logInfo(ctx, "stdio MCP command terminated after shutdown")
		return nil
	case <-ctx.Done():
	case <-grace.C:
	}

	t.logWarn(ctx, "stdio MCP command did not exit after SIGTERM; force killing")
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.logWarn(ctx, "failed to kill stdio MCP command", slog.String("error", err.Error()))
	}
	// Reap the child so no zombie/worker survives the runtime.
	<-waitDone
	return nil
}

func (t *stdioCommandTransport) waitForExit(cmd *exec.Cmd, waitDone chan error, generation uint64) {
	err := cmd.Wait()
	if err != nil {
		t.logWarn(context.Background(), "stdio MCP command exited")
	} else {
		t.logInfo(context.Background(), "stdio MCP command exited")
	}
	waitDone <- err
	close(waitDone)
	t.childLost(generation, "stdio MCP command exited", err, "child_exited")
}

// childLost arbitrates the three ways a child can disappear. Only the first
// failure for a generation invalidates the logical connection or starts a
// recovery. The callback runs outside the mutex, before a new child is visible.
func (t *stdioCommandTransport) childLost(generation uint64, reason string, cause error, observationReason string) {
	t.mu.Lock()
	if t.stopping || !t.active || t.generation != generation {
		t.mu.Unlock()
		return
	}
	t.active = false
	t.recovering = true
	t.generation++
	if t.restartDelay == 0 || time.Since(t.childStartedAt) >= stdioRestartStableTime {
		t.restartDelay = stdioRestartInitialDelay
	} else {
		t.restartDelay *= 2
		if t.restartDelay > stdioRestartMaxDelay {
			t.restartDelay = stdioRestartMaxDelay
		}
	}
	delay := t.restartDelay
	callback := t.onChildLost
	policy := t.policy
	signalForwarded := t.signalForwarded
	cmd, stdin, stdout, waitDone := t.cmd, t.stdin, t.stdout, t.waitDone
	observationGeneration := t.observationGeneration
	var recoveryCtx context.Context
	var recoveryDone chan struct{}
	if !signalForwarded && policy != runtimeconfig.StdioFailurePolicyExit {
		var cancel context.CancelFunc
		recoveryCtx, cancel = context.WithCancel(context.Background())
		t.recoveryCancel = cancel
		recoveryDone = make(chan struct{})
		t.recoveryDone = recoveryDone
	}
	t.mu.Unlock()

	// Close old pipes before invalidating the shared SDK connection so a
	// request that already obtained it cannot keep writing to the lost child.
	if stdin != nil {
		_ = stdin.Close()
	}
	if stdout != nil {
		_ = stdout.Close()
	}
	if t.observation != nil {
		t.observation.childClosed(observationGeneration, observationReason)
	}
	if callback != nil {
		callback()
	}
	if signalForwarded || policy == runtimeconfig.StdioFailurePolicyExit {
		t.requestShutdown(reason, cause)
		return
	}
	t.logWarn(context.Background(), "stdio MCP command failed; restarting child", slog.String("reason", reason))
	go t.restartChild(recoveryCtx, recoveryDone, cmd, stdin, stdout, waitDone, delay)
}

func (t *stdioCommandTransport) restartChild(ctx context.Context, done chan struct{}, oldCmd *exec.Cmd, oldStdin io.WriteCloser, oldStdout io.ReadCloser, oldWaitDone chan error, delay time.Duration) {
	defer close(done)
	t.retireChild(ctx, oldCmd, oldStdin, oldStdout, oldWaitDone)
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		t.mu.Lock()
		generation := t.generation + 1
		t.mu.Unlock()
		cmd, stdin, stdout, transport, err := t.prepareChild(generation)
		if err == nil {
			t.mu.Lock()
			if t.stopping || ctx.Err() != nil {
				t.mu.Unlock()
				_ = stdin.Close()
				_ = stdout.Close()
				return
			}
			err = cmd.Start()
			if err == nil {
				t.cmd, t.stdin, t.stdout, t.transport = cmd, stdin, stdout, transport
				t.waitDone = make(chan error, 1)
				t.generation = generation
				t.childStartedAt = time.Now()
				t.restartDelay = delay
				if t.observation != nil {
					t.observationGeneration = t.observation.beginChild()
					t.observation.childStarted(t.observationGeneration)
				}
				t.active = true
				t.recovering = false
				waitDone := t.waitDone
				onChildStarted := t.onChildStarted
				t.mu.Unlock()
				t.logInfo(context.Background(), "stdio MCP command restarted")
				if onChildStarted != nil {
					onChildStarted(generation, cmd.Process.Pid)
				}
				go t.waitForExit(cmd, waitDone, generation)
				return
			}
			t.mu.Unlock()
			_ = stdin.Close()
			_ = stdout.Close()
		}
		if ctx.Err() != nil {
			return
		}
		t.logWarn(context.Background(), "stdio MCP command restart failed")
		if delay < stdioRestartMaxDelay {
			delay *= 2
			if delay > stdioRestartMaxDelay {
				delay = stdioRestartMaxDelay
			}
		}
	}
}

func (t *stdioCommandTransport) retireChild(ctx context.Context, cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, waitDone chan error) {
	if stdin != nil {
		_ = stdin.Close()
	}
	if stdout != nil {
		_ = stdout.Close()
	}
	if cmd == nil || cmd.Process == nil || waitDone == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(stdioChildStopGrace)
	defer timer.Stop()
	select {
	case <-waitDone:
		return
	case <-ctx.Done():
	case <-timer.C:
	}
	_ = cmd.Process.Kill()
	<-waitDone
}

func (t *stdioCommandTransport) requestShutdown(reason string, cause error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	stopping := t.stopping
	t.mu.Unlock()
	if stopping {
		return
	}
	t.shutdownOnce.Do(func() {
		attrs := []any{slog.String("reason", reason), slog.String("command", t.commandLabel)}
		if cause != nil {
			attrs = append(attrs, slog.String("error", cause.Error()))
		}
		if t.shutdowner == nil {
			t.logWarn(context.Background(), "stdio MCP command failed; shutdowner unavailable", attrs...)
			return
		}
		t.logWarn(context.Background(), "stdio MCP command failed; requesting tunnel-client shutdown", attrs...)
		// An unexpected loss of stdio must fail the parent. A parent signal
		// is intentional even if the child's exit wins the stop race.
		options := []fx.ShutdownOption{fx.ExitCode(1)}
		t.mu.Lock()
		if t.signalForwarded {
			options = nil
		}
		t.mu.Unlock()
		if err := t.shutdowner.Shutdown(options...); err != nil {
			t.logWarn(context.Background(), "stdio MCP shutdown request failed", slog.String("error", err.Error()))
		}
	})
}

func (t *stdioCommandTransport) startSignalForwarding() {
	t.mu.Lock()
	cmd := t.cmd
	if cmd == nil || cmd.Process == nil || t.signals != nil {
		t.mu.Unlock()
		return
	}
	signals := make(chan os.Signal, 2)
	t.signals = signals
	t.mu.Unlock()

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sig := range signals {
			t.forwardSignal(nil, sig)
		}
	}()
}

func (t *stdioCommandTransport) forwardSignal(cmd *exec.Cmd, sig os.Signal) {
	t.mu.Lock()
	if cmd == nil {
		cmd = t.cmd
	}
	if t.stopping {
		t.mu.Unlock()
		return
	}
	if t.recovering || !t.active {
		t.signalForwarded = true
		cancel := t.recoveryCancel
		t.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		t.requestShutdown("parent signal during stdio MCP command restart", nil)
		return
	}
	if cmd != nil && cmd == t.cmd {
		// Keep signal intent and child loss ordered even if Process.Signal
		// races the child exit and fails.
		t.signalForwarded = true
		failed := cmd.Process == nil || cmd.Process.Signal(sig) != nil
		t.mu.Unlock()
		if failed {
			t.requestShutdown("parent signal after stdio MCP command exit", nil)
		}
		return
	}
	t.mu.Unlock()
}

func (t *stdioCommandTransport) stopSignalForwarding() {
	t.mu.Lock()
	signals := t.signals
	t.signals = nil
	t.mu.Unlock()
	if signals == nil {
		return
	}
	signal.Stop(signals)
	close(signals)
}

func (t *stdioCommandTransport) StdioRuntimeInfo() StdioRuntimeInfo {
	t.mu.Lock()
	defer t.mu.Unlock()

	info := StdioRuntimeInfo{
		Command: t.commandLabel,
	}
	if info.Command == "" && t.cmd != nil {
		info.Command = strings.Join(t.cmd.Args, " ")
	}
	if t.active && t.cmd != nil && t.cmd.Process != nil {
		info.PID = t.cmd.Process.Pid
	}
	return info
}

func (t *stdioCommandTransport) logInfo(ctx context.Context, msg string, attrs ...any) {
	if t.logger == nil {
		return
	}
	t.logger.InfoContext(ctx, msg, attrs...)
}

func (t *stdioCommandTransport) logWarn(ctx context.Context, msg string, attrs ...any) {
	if t.logger == nil {
		return
	}
	t.logger.WarnContext(ctx, msg, attrs...)
}

type stdioEOFReader struct {
	reader io.Reader
	onEOF  func()
}

func (r *stdioEOFReader) Read(p []byte) (int, error) {
	if r.reader == nil {
		return 0, io.EOF
	}
	n, err := r.reader.Read(p)
	if err != nil && errors.Is(err, io.EOF) && r.onEOF != nil {
		r.onEOF()
	}
	return n, err
}

func (r *stdioEOFReader) Close() error {
	if r.reader == nil {
		return nil
	}
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type stdioErrWriter struct {
	writer  io.Writer
	onError func(error)
}

func (w *stdioErrWriter) Write(p []byte) (int, error) {
	if w.writer == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := w.writer.Write(p)
	if err != nil && w.onError != nil {
		w.onError(err)
	}
	return n, err
}

func (w *stdioErrWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
