// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestSendAuditRecordsOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	chain := &fakeChain{}
	rel := &fakeRelayer{}
	srv := newAuditServer(t, chain, rel, log)

	_, out, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	require.NoError(t, err)
	require.Equal(t, sendMined, out.Status)
	require.Equal(t, 1, chain.sendCount())

	recs := readAudit(t, path)
	// The relay the send requested belongs to the send and has no line of its own.
	require.Equal(t, 1, rel.relayCount())
	require.Len(t, recs, 2)
	require.Equal(t, recs[0].ID, recs[1].ID)
	require.Equal(t, outcomeIntent, recs[0].Outcome)
	require.Equal(t, bobHex, recs[0].To)
	require.Equal(t, "100", recs[0].Amount)
	require.Equal(t, outcomeMined, recs[1].Outcome)
	require.Equal(t, txHash, recs[1].TxHash)

	chain.sendErr = NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "not mined")
	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	require.Error(t, err)
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodeTxNotConfirmed, te.Code)
	require.False(t, te.Retryable)

	recs = readAudit(t, path)
	require.Equal(t, outcomeNotConfirmed, recs[len(recs)-1].Outcome)
	require.Equal(t, txHash, recs[len(recs)-1].TxHash)
	require.Equal(t, recs[len(recs)-2].ID, recs[len(recs)-1].ID)
}

func TestSendAuditDenialDoesNotSubmit(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	chain := &fakeChain{}
	srv := newAuditServer(t, chain, &fakeRelayer{}, log)

	in := sendInput("10")
	in.From = "relayer-key"
	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	require.Error(t, err)
	require.Zero(t, chain.sendCount())

	recs := readAudit(t, path)
	require.Len(t, recs, 1)
	require.Equal(t, outcomeDenied, recs[0].Outcome)
	require.Equal(t, string(CodePolicyDenied), recs[0].Code)

	// An invalid argument is refused with one failed line and no intent line.
	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("1.5"))
	require.Error(t, err)
	require.Zero(t, chain.sendCount())
	recs = readAudit(t, path)
	require.Len(t, recs, 2)
	require.Equal(t, outcomeFailed, recs[1].Outcome)
	require.Equal(t, string(CodeInvalidArgument), recs[1].Code)
}

func TestSendAuditClosedBlocksBroadcast(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	require.NoError(t, log.Close())
	chain := &fakeChain{}
	srv := newAuditServer(t, chain, &fakeRelayer{}, log)

	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("10"))
	require.Error(t, err)
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodeConfigError, te.Code)
	require.Zero(t, chain.sendCount())
	require.Empty(t, readAudit(t, path))
}

func TestSendAuditFinalFailureKeepsMinedTx(t *testing.T) {
	chain := &fakeChain{}
	srv, err := New(chain, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	srv.skipConfirmation = true
	script := &scriptedAudit{failAt: 2}
	srv.audit = script

	_, out, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("10"))
	require.NoError(t, err)
	require.Equal(t, sendMined, out.Status)
	require.Equal(t, txHash, out.Tx.TxHash)
	require.Equal(t, 1, chain.sendCount())
	require.Contains(t, out.Warnings[0], "audit log update failed")
	require.Equal(t, toolWait, out.Next.Tool)
	require.Equal(t, nextWait, out.Next.Reason)
	require.Len(t, script.records(), 1)
	require.Equal(t, outcomeIntent, script.records()[0].Outcome)
}

func TestRelayAuditRecordsOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	rel := &fakeRelayer{}
	srv := newAuditServer(t, &fakeChain{}, rel, log)

	_, out, err := srv.relayTx(context.Background(), nil, RelayInput{ChainID: chainA, TxHash: txHash})
	require.NoError(t, err)
	require.Len(t, out.Packets, 1)
	require.Equal(t, 1, rel.relayCount())

	recs := readAudit(t, path)
	require.Len(t, recs, 2)
	require.Equal(t, recs[0].ID, recs[1].ID)
	require.Equal(t, outcomeIntent, recs[0].Outcome)
	require.Equal(t, outcomeRelayed, recs[1].Outcome)
	require.Equal(t, txHash, recs[1].TxHash)
}

func TestSendAuditEmitsProcessLog(t *testing.T) {
	// The relayer logger is slog.Default, a text handler on stderr. A container
	// collects that stream. This captures the same record, not only the file.
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	chain := &fakeChain{}
	srv := newAuditServer(t, chain, &fakeRelayer{}, log)

	_, out, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	require.NoError(t, err)
	require.Equal(t, txHash, out.Tx.TxHash)

	mined := auditProcessLine(t, logs.String(), "outcome=mined")
	require.Contains(t, mined, "tool=ibc_ift_send")
	require.Contains(t, mined, "chain="+chainA)
	require.Contains(t, mined, `code=""`)
	require.Contains(t, mined, "amount=100")
	require.Contains(t, mined, "client=base-0")
	require.Contains(t, mined, "tx="+txHash)
	require.NotContains(t, mined, agentKey)
	require.NotContains(t, mined, bobHex)
	require.NotContains(t, mined, tokenA)

	// The message stays in the file. It must not reach the process log: it can
	// carry an RPC URL or an approval token.
	const leaked = "http://user:secret-token@rpc.example/v1 approval=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.2026-01-01T00:00:00Z.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	chain.sendErr = NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "broadcast %s", leaked)
	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	require.Error(t, err)

	failed := auditProcessLine(t, logs.String(), "outcome=not_confirmed")
	require.Contains(t, failed, "tool=ibc_ift_send")
	require.Contains(t, failed, "chain="+chainA)
	require.Contains(t, failed, "code="+string(CodeTxNotConfirmed))
	require.Contains(t, failed, "tx="+txHash)
	require.NotContains(t, logs.String(), "secret-token")
	require.NotContains(t, logs.String(), "http://")
	require.NotContains(t, logs.String(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	recs := readAudit(t, path)
	require.Contains(t, recs[len(recs)-1].Message, "secret-token")
	require.Equal(t, outcomeNotConfirmed, recs[len(recs)-1].Outcome)
}

func auditProcessLine(t *testing.T, logs, marker string) string {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `msg="MCP audit"`) && strings.Contains(line, marker) {
			return line
		}
	}
	t.Fatalf("no MCP audit process log containing %s\n%s", marker, logs)
	return ""
}

func newAuditServer(t *testing.T, chain ChainBackend, rel RelayerBackend, log *AuditLog) *Server {
	t.Helper()
	srv, err := New(chain, rel, Options{Audit: log})
	require.NoError(t, err)
	srv.skipConfirmation = true
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func sendInput(amount string) SendInput {
	return SendInput{
		ChainID: chainA, IFT: tokenA, ClientID: "base-0",
		To: bobHex, Amount: amount, From: agentKey,
	}
}

// scriptedAudit fails the failAt-th write, counting from 1.
type scriptedAudit struct {
	mu     sync.Mutex
	recs   []auditRecord
	failAt int
	n      int
}

func (s *scriptedAudit) write(rec auditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	if s.failAt > 0 && s.n == s.failAt {
		return errDiskFull
	}
	s.recs = append(s.recs, rec)
	return nil
}

func (s *scriptedAudit) Close() error { return nil }

func (s *scriptedAudit) records() []auditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]auditRecord, len(s.recs))
	copy(out, s.recs)
	return out
}

var errDiskFull = errString("disk full")

type errString string

func (e errString) Error() string { return string(e) }

// slowChain broadcasts, then returns only after its context is cancelled and
// a further delay, as a send waiting on the signer lock at shutdown would.
type slowChain struct {
	*fakeChain
	entered chan struct{}
}

func (c slowChain) IFTSend(ctx context.Context, req SendRequest) (TxRef, error) {
	close(c.entered)
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
	return TxRef{}, NewTxError(CodeTxNotConfirmed, TxRef{ChainID: req.ChainID, TxHash: txHash}, "not mined")
}

func TestCloseWaitsForSendOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	chain := slowChain{fakeChain: &fakeChain{}, entered: make(chan struct{})}
	srv, err := New(chain, &fakeRelayer{}, Options{Audit: log})
	require.NoError(t, err)
	srv.skipConfirmation = true

	done := make(chan error, 1)
	go func() {
		_, _, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
		done <- err
	}()
	<-chain.entered
	require.NoError(t, srv.Close())
	requireCode(t, <-done, CodeTxNotConfirmed)

	recs := readAudit(t, path)
	require.Len(t, recs, 2)
	require.Equal(t, outcomeNotConfirmed, recs[1].Outcome)
	require.Equal(t, txHash, recs[1].TxHash, "the broadcast hash survives shutdown")

	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	requireCode(t, err, CodeRelayerUnavailable)
}

// hangChain broadcasts and does not return until released, as a send stuck
// past every shutdown deadline would.
type hangChain struct {
	*fakeChain
	entered chan struct{}
	release chan struct{}
}

func (c hangChain) IFTSend(_ context.Context, req SendRequest) (TxRef, error) {
	close(c.entered)
	<-c.release
	return TxRef{}, NewTxError(CodeTxNotConfirmed, TxRef{ChainID: req.ChainID, TxHash: txHash}, "not mined")
}

func newHangServer(t *testing.T) (*Server, hangChain, string) {
	t.Helper()
	previous := writeDrainTimeout
	writeDrainTimeout = 300 * time.Millisecond
	t.Cleanup(func() { writeDrainTimeout = previous })
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	chain := hangChain{fakeChain: &fakeChain{}, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(chain.release) })
	srv, err := New(chain, &fakeRelayer{}, Options{Audit: log})
	require.NoError(t, err)
	srv.skipConfirmation = true
	go func() { _, _, _ = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100")) }()
	<-chain.entered
	return srv, chain, path
}

func TestCloseDrainIsBoundedAndRefusalsAreAudited(t *testing.T) {
	srv, _, path := newHangServer(t)

	// HTTPServer.Stop calls stop first; that starts the drain's clock.
	start := time.Now()
	srv.stop()
	closed := make(chan error, 1)
	go func() { closed <- srv.Close() }()

	// While Close drains, a new write is refused, and the refusal is audited.
	require.Eventually(t, func() bool {
		srv.writesMu.Lock()
		defer srv.writesMu.Unlock()
		return srv.stopping
	}, time.Second, time.Millisecond)
	_, _, err := srv.relayTx(context.Background(), nil, RelayInput{ChainID: chainA, TxHash: txHash})
	requireCode(t, err, CodeRelayerUnavailable)

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * writeDrainTimeout):
		t.Fatal("Close did not give up on a stuck send")
	}
	require.Less(t, time.Since(start), writeDrainTimeout+200*time.Millisecond)

	recs := readAudit(t, path)
	require.Len(t, recs, 2, "the stuck send's intent line and the refused relay")
	require.Equal(t, outcomeIntent, recs[0].Outcome)
	require.Equal(t, "ibc_relay_tx", recs[1].Tool)
	require.Equal(t, string(CodeRelayerUnavailable), recs[1].Code)
}

func TestCloseDoesNotWaitAgainAfterHTTPStopDeadline(t *testing.T) {
	srv, _, _ := newHangServer(t)

	srv.stop()
	// The HTTP stop used its whole deadline, longer than the drain.
	time.Sleep(writeDrainTimeout + 50*time.Millisecond)
	start := time.Now()
	require.NoError(t, srv.Close())
	require.Less(t, time.Since(start), 100*time.Millisecond, "MCP stays within one deadline")
}

func TestCloseAfterHTTPStopDeadlineDoesNotWarnWithoutWrites(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	// Repeated: with the timer already expired, select used to pick it over a
	// drained wait at random.
	for range 50 {
		log, err := OpenAuditLog(filepath.Join(t.TempDir(), AuditFileName))
		require.NoError(t, err)
		srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{Audit: log})
		require.NoError(t, err)
		srv.stop()
		srv.writesMu.Lock()
		srv.stopStarted = time.Now().Add(-2 * writeDrainTimeout)
		srv.writesMu.Unlock()
		require.NoError(t, srv.Close())
	}
	require.NotContains(t, logs.String(), "MCP write still running")
}
