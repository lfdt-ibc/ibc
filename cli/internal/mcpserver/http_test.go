// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestListenHTTP(t *testing.T) {
	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	h, err := srv.ListenHTTP("127.0.0.1:0", testToken)
	require.NoError(t, err)
	require.NotNil(t, h.server.ErrorLog, "panics recovered by net/http go to the relayer logger")

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL()}, nil)
	require.NoError(t, err)

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_get_context"})
	require.NoError(t, err)
	require.False(t, res.IsError)
	require.NoError(t, cs.Close())

	require.NoError(t, h.Stop())
	_, err = mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL(), MaxRetries: -1}, nil)
	require.Error(t, err, "stopped server refuses connections")
}

func TestStopCancelsInFlightWait(t *testing.T) {
	rel := &fakeRelayer{}
	started := make(chan struct{})
	var once sync.Once
	rel.onPackets = func(ctx context.Context) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	}
	srv, err := New(&fakeChain{}, rel, Options{PollInterval: time.Hour})
	require.NoError(t, err)
	h, err := srv.ListenHTTP("127.0.0.1:0", testToken)
	require.NoError(t, err)

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL()}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = cs.CallTool(ctx, &mcp.CallToolParams{
			Name: "ibc_wait_for_packets",
			Arguments: map[string]any{
				"chain_id":        "1",
				"tx_hash":         txHash,
				"timeout_seconds": 60,
			},
		})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not reach the relayer")
	}

	// Stop ends the client's session and its event stream, so Shutdown does
	// not wait for its deadline. The tool call must already have been canceled.
	stopStart := time.Now()
	require.NoError(t, h.Stop())
	require.Less(t, time.Since(stopStart), httpShutdownTimeout)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight wait still running after stop")
	}
	_, err = mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(context.Background(), &mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL(), MaxRetries: -1}, nil)
	require.Error(t, err, "stopped server refuses connections")
}

func TestStopClosesConnectionsLeftAfterShutdownTimeout(t *testing.T) {
	previous := httpShutdownTimeout
	httpShutdownTimeout = 100 * time.Millisecond
	t.Cleanup(func() { httpShutdownTimeout = previous })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, stop := context.WithCancel(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	// This handler ignores cancellation, as a stuck stream would.
	h := startHTTP(ln, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(entered)
		<-release
	}), stop, slog.Default())

	resp, err := http.Get("http://" + ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	<-entered

	require.ErrorIs(t, h.Stop(), context.DeadlineExceeded)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		readDone <- err
	}()
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stream still open after Stop")
	}
}

func TestStartRelayerMCPHTTPPanicOnProcessLog(t *testing.T) {
	var logs syncBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, stop := context.WithCancel(context.Background())
	const marker = "mcp-http-panic-for-container-log"
	h := startHTTP(ln, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(marker)
	}), stop, slog.Default())
	t.Cleanup(func() { _ = h.Stop() })

	// The recovered panic closes the connection. The client error is not the signal.
	resp, _ := http.Get("http://" + ln.Addr().String())
	if resp != nil {
		resp.Body.Close()
	}
	// net/http logs the panic on its serve goroutine, possibly after Get returns.
	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "http: panic serving") && strings.Contains(logs.String(), marker)
	}, 2*time.Second, 10*time.Millisecond, "process log:\n%s", logs.String())
}

func TestStartRelayerMCPSessionLogOnProcessLog(t *testing.T) {
	var logs syncBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	h, err := srv.ListenHTTP("127.0.0.1:0", testToken)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Stop() })

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL()}, nil)
	require.NoError(t, err)
	require.NoError(t, cs.Close())

	require.Contains(t, logs.String(), "server session connected")
}

func TestRequireLoopback(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:3001", "localhost:3001", "[::1]:3001"} {
		require.NoError(t, RequireLoopback(ok), ok)
	}
	for _, bad := range []string{"0.0.0.0:3001", ":3001", "10.0.0.5:3001", "relayer.internal:3001", "127.0.0.1"} {
		require.Error(t, RequireLoopback(bad), bad)
	}

	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	_, err = srv.ListenHTTP("0.0.0.0:0", testToken)
	require.ErrorContains(t, err, "not loopback")
}

// syncBuffer is a log target written by server goroutines and read by the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestStopEndsUnansweredApprovalPrompt(t *testing.T) {
	previous := httpShutdownTimeout
	httpShutdownTimeout = 300 * time.Millisecond
	t.Cleanup(func() { httpShutdownTimeout = previous })

	chain := &fakeChain{}
	srv, err := New(chain, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	h, err := srv.ListenHTTP("127.0.0.1:0", testToken)
	require.NoError(t, err)

	prompted := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	// An older client: the SDK sends the prompt itself. The user never answers.
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, &mcp.ClientOptions{
		ElicitationHandler: func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			once.Do(func() { close(prompted) })
			<-release
			return &mcp.ElicitResult{Action: "decline"}, nil
		},
	})
	ctx := context.Background()
	cs, err := client.Connect(ctx,
		&mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL(), MaxRetries: -1},
		&mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	// Cleanups run last-in first-out: release the handler before the client
	// close, which waits for it.
	t.Cleanup(func() { close(release) })

	go func() { _, _ = cs.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_ift_send", Arguments: sendArgs("10")}) }()
	select {
	case <-prompted:
	case <-time.After(5 * time.Second):
		t.Fatal("approval prompt not sent")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- h.Stop() }()
	select {
	case <-stopped:
	case <-time.After(4 * httpShutdownTimeout):
		t.Fatal("Stop blocked on an unanswered approval prompt")
	}
	require.Zero(t, chain.sendCount(), "nothing is sent without an answer")
}

// Every record the MCP server writes to the process log carries module=mcp, so
// an operator can tell MCP lines from the relayer's own.
func TestProcessLogLinesCarryModule(t *testing.T) {
	var logs syncBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	audit, err := OpenAuditLog(t.TempDir() + "/audit.jsonl")
	require.NoError(t, err)
	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{Audit: audit, PollInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	srv.skipConfirmation = true
	h, err := srv.ListenHTTP("127.0.0.1:0", testToken)
	require.NoError(t, err)

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL()}, nil)
	require.NoError(t, err)
	_, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_ift_send", Arguments: map[string]any{
		"chain_id": chainA, "ift": tokenA, "client_id": "base-0", "to": bobHex, "amount": "100",
	}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return listSends(t, srv)[0].Status == sendMined },
		time.Second, 10*time.Millisecond)
	require.NoError(t, cs.Close())
	require.NoError(t, h.Stop())
	require.NoError(t, srv.Close())

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	require.NotEmpty(t, lines)
	audits := 0
	for _, line := range lines {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec), line)
		require.Equal(t, LogModule, rec["module"], line)
		if rec["msg"] == "MCP audit" {
			audits++
		}
	}
	require.GreaterOrEqual(t, audits, 2, "the send's intent and outcome lines")
}
