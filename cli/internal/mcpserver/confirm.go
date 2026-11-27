// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	confirmInputKey = "confirm"
	confirmStateTTL = 10 * time.Minute
	// spentRetention keeps a spent approval past expiry so a replay reports used,
	// not expired, and the agent is not told to send again.
	spentRetention = 24 * time.Hour
)

const (
	confirmAccepted confirmStatus = iota
	confirmMismatch
	// confirmExpired is a token that expired unspent.
	confirmExpired
	confirmUsed
	// confirmStale expired over spentRetention ago, so whether it was spent is unknown.
	confirmStale
	// confirmOtherRun was issued before a restart, so whether it was spent is unknown.
	confirmOtherRun
)

// confirmations ties a user's approval to one exact write: the state token is an
// HMAC over a single-use nonce and the write's resolved fields.
type confirmations struct {
	secret []byte
	// run tags this process's tokens; state is in memory, so redeem reports an
	// earlier run's token as such rather than as a mismatch.
	run string
	now func() time.Time

	mu sync.Mutex
	// used holds accepted nonces; tx is set once broadcast returns a hash.
	used map[string]spentConfirm
}

// spentConfirm is one redeemed approval. expiry is the token's own expiry.
// The record is kept until spentRetention after it.
type spentConfirm struct {
	expiry time.Time
	tx     *TxRef
}

// confirmStatus is the result of redeem. Only acceptance spends the nonce,
// and that happens before broadcast.
type confirmStatus int

func newConfirmations(now func() time.Time) *confirmations {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	run := make([]byte, 8)
	if _, err := rand.Read(run); err != nil {
		panic(err)
	}
	return &confirmations{
		secret: secret, run: hex.EncodeToString(run), now: now, used: map[string]spentConfirm{},
	}
}

// issue returns a state token for the write described by fields.
func (c *confirmations) issue(fields ...string) string {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	n := hex.EncodeToString(nonce)
	exp := c.now().Add(confirmStateTTL).UTC().Format(time.RFC3339)
	return c.run + "." + n + "." + exp + "." + c.mac(n, exp, fields)
}

// redeem checks a state token against fields and spends it. Mismatch or expiry
// leaves it unspent; a spent nonce reports used even after expiry.
func (c *confirmations) redeem(state string, fields ...string) confirmStatus {
	run, nonce, exp, mac, ok := splitConfirmState(state)
	if !ok {
		return confirmMismatch
	}
	if run != c.run {
		return confirmOtherRun
	}
	if !hmac.Equal([]byte(mac), []byte(c.mac(nonce, exp, fields))) {
		return confirmMismatch
	}
	expiry, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		return confirmMismatch
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for n, rec := range c.used {
		if now.After(rec.expiry.Add(spentRetention)) {
			delete(c.used, n)
		}
	}
	// Spent is checked before expiry: an expired token that was spent may
	// already have been broadcast.
	if _, spent := c.used[nonce]; spent {
		return confirmUsed
	}
	if now.After(expiry.Add(spentRetention)) {
		return confirmStale
	}
	if now.After(expiry) {
		return confirmExpired
	}
	c.used[nonce] = spentConfirm{expiry: expiry}
	return confirmAccepted
}

// noteTx records the broadcast hash for an already spent approval.
func (c *confirmations) noteTx(state string, tx TxRef) {
	nonce, ok := c.ownNonce(state)
	if tx.TxHash == "" || !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, spent := c.used[nonce]
	if !spent {
		return
	}
	copied := tx
	rec.tx = &copied
	c.used[nonce] = rec
}

// spentTx returns the hash stored for a spent approval, or nil.
func (c *confirmations) spentTx(state string) *TxRef {
	nonce, ok := c.ownNonce(state)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, spent := c.used[nonce]
	if !spent || rec.tx == nil || rec.tx.TxHash == "" {
		return nil
	}
	copied := *rec.tx
	return &copied
}

// ownNonce is the nonce of a state token this run issued.
func (c *confirmations) ownNonce(state string) (string, bool) {
	run, nonce, _, _, ok := splitConfirmState(state)
	return nonce, ok && run == c.run
}

func splitConfirmState(state string) (run, nonce, exp, mac string, ok bool) {
	parts := strings.Split(state, ".")
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

func (c *confirmations) mac(nonce, exp string, fields []string) string {
	h := hmac.New(sha256.New, c.secret)
	h.Write([]byte(nonce + "\x00" + exp))
	for _, f := range fields {
		h.Write([]byte{0})
		h.Write([]byte(f))
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// noElicitationMessage explains why a client without elicitation cannot send.
const noElicitationMessage = "user confirmation is required but this MCP client does not support elicitation. " +
	"Tell the user that sends need a client with elicitation; do not retry."

// writeAction is the first approval field and the verb in the prompt.
const writeAction = "Send"

// askConfirmation builds the approval request; a client without elicitation is refused, not sent unconfirmed.
func (s *Server) askConfirmation(
	ctx context.Context, req *mcp.CallToolRequest, w writeRequest, to string, amount *big.Int, fields []string,
) (*mcp.CallToolResult, error) {
	if !clientElicits(req) {
		return nil, NewError(CodePolicyDenied, "%s", noElicitationMessage)
	}

	shown := amount.String() + " base units"
	if d := s.displayAmount(ctx, w.chainID, w.ift, amount); d != "" {
		shown = fmt.Sprintf("%s (%s base units)", d, amount)
	}
	msg := fmt.Sprintf(
		"%s %s of token %s on chain %s from signer %q to %s, over client %s with a %s packet timeout, "+
			"relay %s. Approve this transaction?",
		writeAction, shown, w.ift, w.chainID, w.from, to, w.clientID, w.timeout, w.relay)

	return &mcp.CallToolResult{
		InputRequests: mcp.InputRequestMap{
			confirmInputKey: &mcp.ElicitParams{
				Message: msg,
				RequestedSchema: &jsonschema.Schema{
					Type: "object",
					Properties: map[string]*jsonschema.Schema{
						"approve": {
							Type:        "boolean",
							Title:       "Approve",
							Description: "Sign and broadcast this transaction",
						},
					},
					Required: []string{"approve"},
				},
			},
		},
		RequestState: s.confirm.issue(fields...),
	}, nil
}

// clientElicits reports whether the client can answer the approval request.
func clientElicits(req *mcp.CallToolRequest) bool {
	if req == nil {
		return false
	}
	caps := req.ClientCapabilities()
	return caps != nil && caps.Elicitation != nil
}

// checkConfirmation verifies the retry carries the user's approval for exactly this write.
func (s *Server) checkConfirmation(req *mcp.CallToolRequest, fields []string) error {
	switch s.confirm.redeem(req.Params.RequestState, fields...) {
	case confirmAccepted:
	case confirmUsed:
		return spentApprovalError(s.confirm.spentTx(req.Params.RequestState))
	case confirmOtherRun:
		return NewError(CodePolicyDenied,
			"confirmation was issued before this relayer restarted, so whether it was used is not known; "+
				"the transfer may already be broadcast. Ask the user before calling ibc_ift_send again.")
	case confirmStale:
		return NewError(CodePolicyDenied,
			"confirmation expired more than %s ago and this server no longer knows whether it was used; "+
				"the transfer may already be broadcast. Ask the user before calling ibc_ift_send again.",
			spentRetention)
	default:
		// Mismatch and expiry did not spend the token.
		return NewError(CodePolicyDenied,
			"confirmation does not match this transaction or has expired; call the tool again")
	}
	res, ok := req.Params.InputResponses[confirmInputKey].(*mcp.ElicitResult)
	if !ok || res == nil {
		return NewError(CodeUserDeclined, "no confirmation answer was returned; nothing was sent. Retry only if the user asks.")
	}
	if res.Action != "accept" {
		return NewError(CodeUserDeclined, "user did not approve the transaction (%s); nothing was sent. Retry only if the user asks.", res.Action)
	}
	if approve, _ := res.Content["approve"].(bool); !approve {
		return NewError(CodeUserDeclined, "user did not approve the transaction; nothing was sent. Retry only if the user asks.")
	}
	return nil
}

// spentApprovalError reports a replayed approval. Approvals are spent before broadcast,
// so a hash is present only if that broadcast has since returned one.
func spentApprovalError(tx *TxRef) *ToolError {
	if tx == nil || tx.TxHash == "" {
		return NewError(CodePolicyDenied,
			"confirmation was already used; the transfer may already be broadcast. No transaction hash was returned. Ask the user. Do not call ibc_ift_send.")
	}
	e := NewError(CodePolicyDenied,
		"confirmation was already used; the transfer may already be broadcast. Call ibc_get_packets with source_tx_hash. Do not call ibc_ift_send.")
	copied := *tx
	e.Tx = &copied
	return e
}

// noteBroadcast stores a hash on the approval that authorized this send.
func (s *Server) noteBroadcast(req *mcp.CallToolRequest, tx TxRef) {
	if req == nil || req.Params == nil {
		return
	}
	s.confirm.noteTx(req.Params.RequestState, tx)
}
