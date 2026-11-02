package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	tunnelclient "github.com/openai/tunnel-client"
	"github.com/openai/tunnel-client/pkg/config"
	"github.com/openai/tunnel-client/pkg/controlplane"
	harnesspkg "github.com/openai/tunnel-client/testsupport/e2e"
	"github.com/openai/tunnel-client/testsupport/mockmcpserver"
	"github.com/openai/tunnel-client/testsupport/mocktunnelservice"
)

// drainToolCommand scripts one tools/call against the stdio mock server. The
// mock records requestID into MOCK_MCP_INVOCATION_LOG, which doubles as an
// exactly-once execution audit across shutdown.
func drainToolCommand(t *testing.T, requestID, toolName string, expectResponse bool) mocktunnelservice.CommandResponse {
	t.Helper()
	callID := "call-" + requestID
	payload := json.RawMessage(fmt.Sprintf(`{
		"jsonrpc":"2.0","id":%q,"method":"tools/call",
		"params":{"name":%q,"arguments":{"name":%q,"request_id":%q}}
	}`, callID, toolName, toolName, requestID))
	command := mocktunnelservice.CommandResponse{
		Command: mocktunnelservice.NewCommand(requestID, payload, nil),
	}
	if expectResponse {
		command.ExpectedResponses = []mocktunnelservice.ExpectedResponse{{
			RequestID: requestID,
			Assert: func(tb testing.TB, resp mocktunnelservice.ReceivedResponse) {
				tb.Helper()
				if resp.ResponseCode != http.StatusOK {
					tb.Fatalf("%s status = %d, want 200: %s", requestID, resp.ResponseCode, resp.JSONResponse)
				}
				if !bytes.Contains(resp.JSONResponse, []byte("hello "+toolName)) {
					tb.Fatalf("%s unexpected payload: %s", requestID, resp.JSONResponse)
				}
			},
		}}
	} else {
		// The drain budget expires before the stuck tool can answer.
		command.NoResponseExpected = true
	}
	return command
}

func waitForFileContains(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %s never contained %q", path, want)
}

func readInvocationCounts(t *testing.T, path string) map[string]int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read invocation log: %v", err)
	}
	counts := map[string]int{}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			counts[line]++
		}
	}
	return counts
}

func readChildPID(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid log: %v", err)
	}
	fields := strings.Fields(string(data))
	require.NotEmpty(t, fields, "pid log empty")
	pid, err := strconv.Atoi(fields[len(fields)-1])
	require.NoError(t, err)
	return pid
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func waitProcessGone(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stdio child pid %d still alive after %v", pid, timeout)
}

func matchedResponse(h *harnesspkg.Harness, requestID string) (mocktunnelservice.ReceivedResponse, bool) {
	for _, resp := range h.ControlPlane.ReceivedResponses(mocktunnelservice.ResponseMatchMatched) {
		if resp.RequestID == requestID {
			return resp, true
		}
	}
	return mocktunnelservice.ReceivedResponse{}, false
}

// A slow tool call already executing against the stdio child when shutdown
// starts finishes within the drain budget: its response is posted before the
// child is terminated, the drain reports no incomplete work, and the child
// exits and is reaped. The tool executes exactly once.
func TestShutdownDrainSlowResponseCompletesBeforeStdioTermination(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pidLog := dir + "/pids.log"
	invocationLog := dir + "/invocations.log"
	commandArgs := append([]string{"env",
		"MOCK_MCP_PID_LOG=" + pidLog,
		"MOCK_MCP_INVOCATION_LOG=" + invocationLog,
		"MOCK_MCP_DELAY_RESPONSE_NAME=slow",
		"MOCK_MCP_DELAY_MS=1000",
	}, mockmcpserver.StdioServerCommand(t)...)

	h := harnesspkg.NewHarness(t,
		harnesspkg.WithMCPCommand(commandArgs),
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.Logging.Level = slog.LevelInfo
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithInitializationPhaseCommandsWithoutSessionHeaders(),
			mocktunnelservice.WithCommandResponses(
				drainToolCommand(t, "drain-slow", "slow", true),
			),
		),
	)
	h.Start(t)

	waitForFileContains(t, invocationLog, "drain-slow", 10*time.Second)
	pid := readChildPID(t, pidLog)
	require.True(t, processAlive(pid), "stdio child must be running before shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, h.StopClient(ctx))
	elapsed := time.Since(start)

	// The drain waited for the in-flight slow response (~1s) but remained
	// bounded by the total shutdown deadline.
	require.Greater(t, elapsed, 800*time.Millisecond, "shutdown returned before the slow response could finish")
	require.Less(t, elapsed, 20*time.Second, "drain was not bounded")

	resp, ok := matchedResponse(h, "drain-slow")
	require.True(t, ok, "slow-tool response was not posted during drain")
	require.Equal(t, http.StatusOK, resp.ResponseCode)
	require.Contains(t, string(resp.JSONResponse), "hello slow")

	waitProcessGone(t, pid, 5*time.Second)
	require.Equal(t, map[string]int{"drain-slow": 1}, readInvocationCounts(t, invocationLog),
		"tool must execute exactly once across shutdown")
}

// A tool that never answers exhausts the (short) drain budget: shutdown
// reports the interrupted command via the shared ErrDrainIncomplete contract,
// and the stdio child that ignores SIGTERM is SIGKILLed and reaped. The tool
// is never executed a second time.
func TestShutdownDrainBudgetExhaustionReportsAndReclaims(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pidLog := dir + "/pids.log"
	invocationLog := dir + "/invocations.log"
	blockFIFO := dir + "/block.fifo"
	require.NoError(t, syscall.Mkfifo(blockFIFO, 0o600))

	commandArgs := append([]string{"env",
		"MOCK_MCP_PID_LOG=" + pidLog,
		"MOCK_MCP_INVOCATION_LOG=" + invocationLog,
		"MOCK_MCP_HARD_BLOCK_NAME=stuck",
		"MOCK_MCP_BLOCK_FIFO=" + blockFIFO,
	}, mockmcpserver.StdioServerCommand(t)...)

	var logs bytes.Buffer
	h := harnesspkg.NewHarness(t,
		harnesspkg.WithMCPCommand(commandArgs),
		harnesspkg.WithLogWriter(&logs),
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.Logging.Level = slog.LevelInfo
			// Far shorter than the tool can wait; the total fx deadline still
			// leaves the 15s reclamation margin.
			cfg.ControlPlane.ShutdownDrainTimeout = 300 * time.Millisecond
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithInitializationPhaseCommandsWithoutSessionHeaders(),
			mocktunnelservice.WithCommandResponses(
				drainToolCommand(t, "drain-stuck", "stuck", false),
			),
		),
	)
	h.Start(t)

	waitForFileContains(t, invocationLog, "drain-stuck", 10*time.Second)
	pid := readChildPID(t, pidLog)
	require.True(t, processAlive(pid), "stdio child must be running before shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	stopErr := h.StopClient(ctx)
	elapsed := time.Since(start)

	require.Error(t, stopErr)
	require.True(t, errors.Is(stopErr, tunnelclient.ErrDrainIncomplete),
		"stop error must wrap ErrDrainIncomplete, got %v", stopErr)
	result, ok := tunnelclient.AsDrainResult(stopErr)
	require.True(t, ok)
	require.True(t, result.TimedOut)
	require.EqualValues(t, 1, result.InFlightInterrupted)
	require.EqualValues(t, 0, result.QueuedUnfinished)
	require.Contains(t, result.UnfinishedRequestIDs, "drain-stuck")

	// Drain (~300ms) plus the 1s SIGTERM grace before SIGKILL, nowhere near
	// the fx reclamation margin.
	require.Greater(t, elapsed, 900*time.Millisecond, "SIGTERM grace was not awaited")
	require.Less(t, elapsed, 10*time.Second, "budget exhaustion did not bound shutdown")

	logText := logs.String()
	require.Contains(t, logText, "shutdown drain incomplete")
	require.Contains(t, logText, "in_flight_interrupted")
	require.Contains(t, logText, "force killing")

	waitProcessGone(t, pid, 5*time.Second)
	require.Equal(t, map[string]int{"drain-stuck": 1}, readInvocationCounts(t, invocationLog),
		"interrupted tool must never run twice")
}

// The poller's final batch can return at the exact instant polling is
// canceled. Admission stays open until the poller exits, so those commands
// enter the queue, drain exactly once, and post their responses before the
// stdio child is terminated.
func TestShutdownPollingStopRaceAdmitsFinalBatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pidLog := dir + "/pids.log"
	invocationLog := dir + "/invocations.log"
	commandArgs := append([]string{"env",
		"MOCK_MCP_PID_LOG=" + pidLog,
		"MOCK_MCP_INVOCATION_LOG=" + invocationLog,
		"MOCK_MCP_DELAY_RESPONSE_NAME=race",
		"MOCK_MCP_DELAY_MS=500",
	}, mockmcpserver.StdioServerCommand(t)...)

	raceIDs := map[string]bool{"drain-race-1": true, "drain-race-2": true}

	// Hold the fetched batch INSIDE the poller (after the control plane
	// returned it, before it is admitted) until fx shutdown has already
	// canceled the polling context. This deterministically drives the
	// stop/admission race.
	fetched := make(chan struct{}, 8)
	release := make(chan struct{})
	var blocked atomic.Bool
	var observed atomic.Int64
	observer := func(cmd controlplane.PolledCommand) {
		id := cmd.RequestID().String()
		if !raceIDs[id] {
			return
		}
		observed.Add(1)
		select {
		case fetched <- struct{}{}:
		default:
		}
		if !blocked.Swap(true) {
			<-release
		}
	}

	h := harnesspkg.NewHarness(t,
		harnesspkg.WithMCPCommand(commandArgs),
		harnesspkg.WithPolledCommandObserver(observer),
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.Logging.Level = slog.LevelWarn
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithInitializationPhaseCommandsWithoutSessionHeaders(),
			mocktunnelservice.WithCommandResponses(
				drainToolCommand(t, "drain-race-1", "race", true),
				drainToolCommand(t, "drain-race-2", "race", true),
			),
		),
	)
	h.Start(t)

	select {
	case <-fetched:
	case <-time.After(10 * time.Second):
		t.Fatal("final batch never returned from the control plane")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Depending on poll batching, a command not yet fetched when polling is
	// canceled correctly stays undelivered and unexecuted; tolerate that in
	// the mock and assert the exact executed set below.
	h.ControlPlane.AllowPending()
	stopErr := make(chan error, 1)
	go func() {
		stopErr <- h.StopClient(ctx)
	}()

	// Let the poller OnStop cancel the polling context while the batch is
	// still held, then release it for admission. Admission must still accept
	// this final fetched batch.
	time.Sleep(100 * time.Millisecond)
	close(release)

	require.NoError(t, <-stopErr, "admitted final batch must drain cleanly")

	invocations := readInvocationCounts(t, invocationLog)
	require.GreaterOrEqual(t, observed.Load(), int64(1), "race setup never observed a fetched batch")
	executed := 0
	for id := range raceIDs {
		switch n := invocations[id]; n {
		case 0:
			// Never fetched before polling stopped: never admitted, never
			// executed, no response posted.
			_, ok := matchedResponse(h, id)
			require.False(t, ok, "%s got a response despite never being executed", id)
		case 1:
			executed++
			resp, ok := matchedResponse(h, id)
			require.True(t, ok, "%s response missing after raced shutdown", id)
			require.Equal(t, http.StatusOK, resp.ResponseCode, "%s: %s", id, resp.JSONResponse)
		default:
			t.Fatalf("%s executed %d times, want exactly once", id, n)
		}
	}
	require.Equal(t, int(observed.Load()), executed,
		"every fetched command must be admitted and executed exactly once")

	pid := readChildPID(t, pidLog)
	waitProcessGone(t, pid, 5*time.Second)
}
