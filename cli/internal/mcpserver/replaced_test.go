// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

const (
	replacementHash = "0x2222222222222222222222222222222222222222222222222222222222222222"
	laterHash       = "0x3333333333333333333333333333333333333333333333333333333333333333"
)

// minedRelayer knows packets only for the one hash that was mined.
type minedRelayer struct {
	mu     sync.Mutex
	mined  string
	relays []string
}

func (m *minedRelayer) Relay(_ context.Context, _, txHash string, _ []PacketSelector) ([]ObservedPacket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.relays = append(m.relays, txHash)
	if !strings.EqualFold(txHash, m.mined) {
		return nil, NewError(CodePacketsNotFound, "no packets found in tx %s", txHash)
	}
	return []ObservedPacket{{SourceClientID: "base-0", Sequence: "42", Selection: SelectionSelected}}, nil
}

func (m *minedRelayer) Packets(_ context.Context, f PacketFilter, _ uint32, _ string) (PacketsPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !strings.EqualFold(f.SourceTxHash, m.mined) {
		return PacketsPage{Packets: []PacketStatus{}}, nil
	}
	return PacketsPage{Packets: []PacketStatus{packet(StateSucceeded, true)}}, nil
}

func TestReplacementsFamily(t *testing.T) {
	var r replacements
	require.Equal(t, []string{txHash}, r.family(txHash))
	r.add(txHash, replacementHash)
	r.add(replacementHash, laterHash)
	want := []string{laterHash, replacementHash, txHash}
	require.Equal(t, want, r.family(txHash))
	require.Equal(t, want, r.family(replacementHash), "every hash of a transfer finds the whole group")

	// Reports that arrive out of order still join one group, in signing order.
	const latestHash = "0x4444444444444444444444444444444444444444444444444444444444444444"
	var unordered replacements
	unordered.add(laterHash, latestHash)
	unordered.add(replacementHash, laterHash)
	unordered.add(txHash, replacementHash)
	want = []string{latestHash, laterHash, replacementHash, txHash}
	for _, h := range want {
		require.Equal(t, want, unordered.family(h), "family of %s", h)
	}
	var split replacements
	split.add(txHash, replacementHash)
	split.add(laterHash, latestHash)
	split.add(replacementHash, laterHash)
	for _, h := range want {
		require.Equal(t, want, split.family(h), "family of %s", h)
	}

	var cased replacements
	upper := "0xABCD" + txHash[6:]
	cased.add(upper, replacementHash)
	require.Equal(t, []string{replacementHash, strings.ToLower(upper)}, cased.family(strings.ToLower(upper)),
		"hashes are matched case-insensitively")
}

func TestReplacedSendIsFollowedByTools(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	chain := &fakeChain{}
	chain.sendErr = NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "not mined")
	rel := &minedRelayer{}
	srv := newAuditServer(t, chain, rel, log)

	_, _, err = srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	requireCode(t, err, CodeTxNotConfirmed)

	// After the call returned, the relayer re-signs the transfer and the
	// replacement is mined.
	require.NotNil(t, chain.sends[0].OnReplaced)
	chain.sends[0].OnReplaced(txHash, replacementHash)
	rel.mined = replacementHash

	recs := readAudit(t, path)
	last := recs[len(recs)-1]
	require.Equal(t, outcomeReplaced, last.Outcome)
	require.Equal(t, replacementHash, last.TxHash)
	require.Equal(t, recs[0].ID, last.ID, "the replacement is filed under the send")
	require.Contains(t, last.Message, txHash)

	// The agent still holds the first hash.
	_, page, err := srv.getPackets(context.Background(), nil, PacketsInput{SourceChainID: chainA, SourceTxHash: txHash})
	require.NoError(t, err)
	require.Len(t, page.Packets, 1)

	_, relayed, err := srv.relayTx(context.Background(), nil, RelayInput{ChainID: chainA, TxHash: txHash})
	require.NoError(t, err)
	require.Len(t, relayed.Packets, 1)
	require.Len(t, relayed.Warnings, 1)
	require.Contains(t, relayed.Warnings[0], replacementHash)
	require.Equal(t, []string{replacementHash}, rel.relays, "the newest hash is tried first")
	recs = readAudit(t, path)
	require.Equal(t, replacementHash, recs[len(recs)-1].TxHash, "the audit names the hash that was relayed")

	_, waited, err := srv.waitForPackets(context.Background(), &mcp.CallToolRequest{},
		WaitInput{ChainID: chainA, TxHash: txHash, TimeoutSeconds: 1})
	require.NoError(t, err)
	require.True(t, waited.Done)
	require.Contains(t, waited.Next.Reason, replacementHash)
}

func TestReplacedSendWhereOriginalWasMined(t *testing.T) {
	chain := &fakeChain{}
	rel := &minedRelayer{mined: txHash}
	srv := newAuditServer(t, chain, rel, nil)

	_, _, err := srv.iftSend(context.Background(), &mcp.CallToolRequest{}, sendInput("100"))
	require.NoError(t, err)
	// A replacement was signed, but the original was mined first.
	chain.sends[0].OnReplaced(txHash, replacementHash)

	for _, asked := range []string{txHash, replacementHash} {
		_, page, err := srv.getPackets(context.Background(), nil, PacketsInput{SourceChainID: chainA, SourceTxHash: asked})
		require.NoError(t, err)
		require.Len(t, page.Packets, 1, asked)

		_, relayed, err := srv.relayTx(context.Background(), nil, RelayInput{ChainID: chainA, TxHash: asked})
		require.NoError(t, err, asked)
		require.Len(t, relayed.Packets, 1, asked)
	}
}

func TestReplacedApprovalReplayShowsNewHash(t *testing.T) {
	h := newConfirmingHarness(t, clientOpts{})
	fields := []string{
		"Send", chainA, strings.ToLower(tokenA), agentKey, strings.ToLower(bobHex),
		"100", "base-0", defaultSendTimeout.String(), relayAuto,
	}
	state := h.srv.confirm.issue(fields...)
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", From: agentKey}
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		RequestState: state,
		InputResponses: mcp.InputResponseMap{
			confirmInputKey: &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}},
		},
	}}
	_, _, err := h.srv.iftSend(context.Background(), req, in)
	require.NoError(t, err)
	h.chain.sends[0].OnReplaced(txHash, replacementHash)

	_, _, err = h.srv.iftSend(context.Background(), req, in)
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Contains(t, te.Message, "already used")
	require.NotNil(t, te.Tx)
	require.Equal(t, replacementHash, te.Tx.TxHash)
}

func requireCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, code, te.Code)
}
