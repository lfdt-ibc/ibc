// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// broadcastChain reports the broadcast, then mines only when mine is closed.
type broadcastChain struct {
	*fakeChain
	mine chan struct{}
}

func (c broadcastChain) IFTSend(ctx context.Context, req SendRequest) (TxRef, error) {
	tx := TxRef{ChainID: req.ChainID, TxHash: txHash}
	req.OnBroadcast(tx)
	select {
	case <-c.mine:
		return tx, nil
	case <-ctx.Done():
		return TxRef{}, NewTxError(CodeTxNotConfirmed, tx, "not mined")
	}
}

func newSendServer(t *testing.T, chain ChainBackend, rel RelayerBackend) *Server {
	t.Helper()
	srv, err := New(chain, rel, Options{PollInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	srv.skipConfirmation = true
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func listSends(t *testing.T, srv *Server) []SendRecord {
	t.Helper()
	_, out, err := srv.listSends(context.Background(), nil, ListSendsInput{})
	require.NoError(t, err)
	return out.Sends
}

func TestSendReturnsAtBroadcast(t *testing.T) {
	chain := broadcastChain{fakeChain: &fakeChain{}, mine: make(chan struct{})}
	rel := &fakeRelayer{}
	srv := newSendServer(t, chain, rel)

	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100"}
	_, out, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	require.NoError(t, err)
	require.Equal(t, sendBroadcast, out.Status)
	require.Equal(t, txHash, out.Tx.TxHash)
	require.Equal(t, agentKey, out.From, "from defaults to the client's signer")
	require.Equal(t, "0.0001 USDX", out.AmountDisplay)
	require.Equal(t, NextStep{
		Tool: toolWait, Arguments: map[string]string{"chain_id": chainA, "tx_hash": txHash},
		Reason: "Waiting for the source tx to be mined.",
	}, out.Next)
	require.Zero(t, rel.relayCount(), "the relay waits for mining")

	_, waited, err := srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 1})
	require.NoError(t, err)
	require.True(t, waited.TimedOut)
	require.Equal(t, toolWait, waited.Next.Tool)
	require.Equal(t, waitMiningHint, waited.Next.Reason)

	close(chain.mine)
	require.Eventually(t, func() bool { return listSends(t, srv)[0].Status == sendMined },
		time.Second, 10*time.Millisecond)
	require.Equal(t, 1, rel.relayCount())
	require.Equal(t, toolWait, listSends(t, srv)[0].Next.Tool)
}

func TestWaitFollowsSendWithoutPackets(t *testing.T) {
	srv := newSendServer(t, &fakeChain{}, &fakeRelayer{})
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", Relay: relayNever}
	_, out, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	require.NoError(t, err)
	require.Equal(t, sendMined, out.Status)

	start := time.Now()
	_, waited, err := srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 600})
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second, "waiting cannot help before ibc_relay_tx")
	require.False(t, waited.TimedOut)
	require.Equal(t, toolRelay, waited.Next.Tool)
	require.Equal(t, nextNever, waited.Next.Reason)
}

func TestFailedSendIsListedAndEndsWaits(t *testing.T) {
	chain := &fakeChain{sendErr: NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "not mined")}
	srv := newSendServer(t, chain, &fakeRelayer{})
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100"}
	_, _, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	requireCode(t, err, CodeTxNotConfirmed)

	sends := listSends(t, srv)
	require.Len(t, sends, 1)
	require.Equal(t, sendFailed, sends[0].Status)
	require.Equal(t, CodeTxNotConfirmed, sends[0].Error.Code)
	require.Equal(t, toolRelay, sends[0].Next.Tool, "it may mine later, and nothing relayed it")
	require.Equal(t, txHash, sends[0].Next.Arguments["tx_hash"])

	_, waited, err := srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 600})
	require.NoError(t, err)
	require.Equal(t, toolRelay, waited.Next.Tool)

	chain.sendErr = NewTxError(CodeTxReverted, TxRef{ChainID: chainA, TxHash: txHash}, "reverted")
	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	requireCode(t, err, CodeTxReverted)
	_, _, err = srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 600})
	requireCode(t, err, CodeTxReverted)
}

func TestRelayUpdatesTheSendsNextStep(t *testing.T) {
	srv := newSendServer(t, &fakeChain{}, &fakeRelayer{})
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", Relay: relayNever}
	_, out, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	require.NoError(t, err)
	require.Equal(t, toolRelay, out.Next.Tool)

	_, _, err = srv.relayTx(context.Background(), nil, RelayInput{ChainID: chainA, TxHash: txHash})
	require.NoError(t, err)
	sent := listSends(t, srv)[0]
	require.True(t, sent.RelayRequested)
	require.Equal(t, toolWait, sent.Next.Tool, "after the relay, waiting is the next step")

	_, waited, err := srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 1})
	require.NoError(t, err)
	require.True(t, waited.TimedOut, "the wait waits instead of asking for another relay")
}

// lateReplaceChain mines txHash, then reports a replacement that was not mined.
type lateReplaceChain struct{ *fakeChain }

func (c lateReplaceChain) IFTSend(_ context.Context, req SendRequest) (TxRef, error) {
	go func() {
		time.Sleep(20 * time.Millisecond)
		req.OnReplaced(txHash, replacementHash)
	}()
	return TxRef{ChainID: req.ChainID, TxHash: txHash}, nil
}

func TestLateReplacementKeepsTheMinedHash(t *testing.T) {
	srv := newSendServer(t, lateReplaceChain{&fakeChain{}}, &fakeRelayer{})
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100"}
	_, _, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(srv.replaced.family(txHash)) == 2 },
		time.Second, 5*time.Millisecond)
	sent := listSends(t, srv)[0]
	require.Equal(t, sendMined, sent.Status)
	require.Equal(t, txHash, sent.Tx.TxHash)
}

// slowTokenChain never answers token reads on chainB.
type slowTokenChain struct{ *fakeChain }

func (c slowTokenChain) TokenInfo(ctx context.Context, chainID, ift string) (TokenInfo, error) {
	if chainID == chainB {
		<-ctx.Done()
		return TokenInfo{}, ctx.Err()
	}
	return c.fakeChain.TokenInfo(ctx, chainID, ift)
}

func TestContextBoundsTokenReads(t *testing.T) {
	previous := tokenReadTimeout
	tokenReadTimeout = 50 * time.Millisecond
	t.Cleanup(func() { tokenReadTimeout = previous })
	srv, err := New(slowTokenChain{&fakeChain{}}, &fakeRelayer{}, Options{
		Tokens: []TokenRef{{ChainID: chainB, IFT: tokenA}, {ChainID: chainA, IFT: tokenA}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	_, out, err := srv.getContext(context.Background(), nil, ContextInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Tokens[0].Error, "the slow chain reports an error")
	require.Equal(t, "USDX", out.Tokens[1].Symbol, "the other chain still answers")
}

func TestListSendsNewestFirst(t *testing.T) {
	srv := newSendServer(t, &fakeChain{}, &fakeRelayer{})
	for _, amount := range []string{"1", "2", "3"} {
		in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: amount}
		_, _, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
		require.NoError(t, err)
	}
	_, out, err := srv.listSends(context.Background(), nil, ListSendsInput{Limit: 2})
	require.NoError(t, err)
	require.Len(t, out.Sends, 2)
	require.Equal(t, "3", out.Sends[0].Amount)
	require.Equal(t, "2", out.Sends[1].Amount)

	_, _, err = srv.listSends(context.Background(), nil, ListSendsInput{Limit: maxSends + 1})
	requireCode(t, err, CodeInvalidArgument)
}

func TestConfiguredTokens(t *testing.T) {
	h := newHarness(t, clientOpts{tokens: []TokenRef{{ChainID: chainA, IFT: tokenA}}})

	out, terr := h.call("ibc_get_context", nil, nil)
	require.Nil(t, terr)
	require.Equal(t, []any{map[string]any{
		"chain_id": chainA, "ift": tokenA, "symbol": "USDX", "decimals": float64(6),
	}}, out["tokens"])

	out, terr = h.call("ibc_ift_balance", map[string]any{"chain_id": chainA, "address": agentKey}, nil)
	require.Nil(t, terr, "ift defaults to the chain's only token")
	require.Equal(t, tokenA, out["ift"])
	require.Equal(t, 1, h.chain.tokenReads, "token metadata is read once")

	out, terr = h.call("ibc_ift_send", map[string]any{
		"chain_id": chainA, "client_id": "base-0", "to": bobHex, "amount": "5",
	}, nil)
	require.Nil(t, terr)
	require.Equal(t, tokenA, h.chain.sends[0].IFT)
	require.Equal(t, agentKey, h.chain.sends[0].From)

	_, terr = h.call("ibc_ift_balance", map[string]any{"chain_id": chainB, "address": agentKey}, nil)
	require.Equal(t, CodeInvalidArgument, terr.Code)
	require.Contains(t, terr.Message, "has 0 configured tokens")
}

// A TX_NOT_CONFIRMED send that mines later is listed as mined once its packets
// are seen, by ibc_relay_tx or by ibc_wait_for_packets, and its audit id gets a
// mined line.
func TestNotConfirmedSendIsMinedOnceItsPacketsAreSeen(t *testing.T) {
	for _, seenBy := range []string{toolRelay, toolWait} {
		t.Run(seenBy, func(t *testing.T) {
			auditPath := t.TempDir() + "/audit.jsonl"
			audit, err := OpenAuditLog(auditPath)
			require.NoError(t, err)
			chain := &fakeChain{sendErr: NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "not mined")}
			rel := &fakeRelayer{
				relayPackets: []ObservedPacket{{SourceClientID: "base-0", Sequence: "42", Selection: SelectionSelected}},
				script:       [][]PacketStatus{{packet(StatePending, false)}},
			}
			srv, err := New(chain, rel, Options{Audit: audit, PollInterval: 10 * time.Millisecond})
			require.NoError(t, err)
			srv.skipConfirmation = true
			t.Cleanup(func() { _ = srv.Close() })

			in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100"}
			_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, in)
			requireCode(t, err, CodeTxNotConfirmed)
			require.Equal(t, sendFailed, listSends(t, srv)[0].Status)

			if seenBy == toolRelay {
				_, _, err = srv.relayTx(context.Background(), &mcp.CallToolRequest{}, RelayInput{ChainID: chainA, TxHash: txHash})
			} else {
				_, _, err = srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
					WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 1})
			}
			require.NoError(t, err)

			got := listSends(t, srv)[0]
			require.Equal(t, sendMined, got.Status)
			require.Nil(t, got.Error)
			require.Equal(t, txHash, got.Tx.TxHash)
			require.Contains(t, got.Warnings[len(got.Warnings)-1], "mined later")
			require.Equal(t, seenBy == toolRelay, got.RelayRequested)

			bz, err := os.ReadFile(auditPath)
			require.NoError(t, err)
			require.Contains(t, string(bz), `"id":"`+got.ID+`","tool":"ibc_ift_send","outcome":"mined"`)
		})
	}
}

// A follow-up wait keeps until=received rather than falling back to final.
func TestWaitNextKeepsUntil(t *testing.T) {
	rel := &fakeRelayer{script: [][]PacketStatus{{packet(StatePending, false)}}}
	srv := newSendServer(t, &fakeChain{}, rel)
	_, out, err := srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, Until: untilReceived, TimeoutSeconds: 1})
	require.NoError(t, err)
	require.True(t, out.TimedOut)
	require.Equal(t, toolWait, out.Next.Tool)
	require.Equal(t, untilReceived, out.Next.Arguments["until"])

	_, out, err = srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 1})
	require.NoError(t, err)
	require.NotContains(t, out.Next.Arguments, "until", "final is the default")
}
