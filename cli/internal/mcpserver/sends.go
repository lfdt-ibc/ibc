// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Send statuses, in order.
const (
	sendSubmitting = "submitting"
	sendBroadcast  = "broadcast"
	sendMined      = "mined"
	sendFailed     = "failed"
)

const (
	// maxSends caps the sends kept in memory for ibc_list_sends.
	maxSends          = 100
	defaultSendsLimit = 20
)

// SendRecord is one ibc_ift_send of this run and how far it got.
type SendRecord struct {
	ID             string           `json:"id"`
	Time           time.Time        `json:"time"`
	ChainID        string           `json:"chain_id"`
	ClientID       string           `json:"client_id"`
	IFT            string           `json:"ift"`
	From           string           `json:"from"`
	To             string           `json:"to"`
	Amount         string           `json:"amount"                    jsonschema:"base units"`
	AmountDisplay  string           `json:"amount_display,omitempty"  jsonschema:"amount in whole tokens with symbol; empty when token metadata could not be read"`
	Relay          string           `json:"relay"`
	Tx             *TxRef           `json:"tx,omitempty"              jsonschema:"latest broadcast tx; the mined one once mined"`
	Status         string           `json:"status"                    jsonschema:"submitting, broadcast, mined or failed"`
	RelayRequested bool             `json:"relay_requested"`
	Packets        []ObservedPacket `json:"packets,omitempty"         jsonschema:"packets the relay request found"`
	Warnings       []string         `json:"warnings,omitempty"`
	Error          *ToolError       `json:"error,omitempty"`
	Next           NextStep         `json:"next"`
}

// ListSendsInput is the input of ibc_list_sends.
type ListSendsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"most recent sends to return; default 20"`
}

// ListSendsOutput is the result of ibc_list_sends.
type ListSendsOutput struct {
	Sends []SendRecord `json:"sends" jsonschema:"newest first"`
}

// sendLog keeps the latest sends of this run.
type sendLog struct {
	mu    sync.Mutex
	sends []*SendRecord // oldest first
}

func (l *sendLog) add(r SendRecord) *SendRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.sends) == maxSends {
		l.sends = l.sends[1:]
	}
	p := &r
	l.sends = append(l.sends, p)
	return p
}

func (l *sendLog) update(r *SendRecord, f func(*SendRecord)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f(r)
}

func (l *sendLog) get(r *SendRecord) SendRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return copySend(r)
}

// recent returns up to limit sends, newest first.
func (l *sendLog) recent(limit int) []SendRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]SendRecord, 0, min(limit, len(l.sends)))
	for i := len(l.sends) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, copySend(l.sends[i]))
	}
	return out
}

// byHash finds the newest send that broadcast any of hashes.
func (l *sendLog) byHash(hashes []string) (SendRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.sends) - 1; i >= 0; i-- {
		r := l.sends[i]
		for _, h := range hashes {
			if r.Tx != nil && strings.EqualFold(r.Tx.TxHash, h) {
				return copySend(r), true
			}
		}
	}
	return SendRecord{}, false
}

// relayed records a successful ibc_relay_tx for any of hashes on the send that broadcast it.
func (l *sendLog) relayed(hashes []string, next NextStep) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.sends) - 1; i >= 0; i-- {
		r := l.sends[i]
		for _, h := range hashes {
			if r.Status == sendMined && r.Tx != nil && strings.EqualFold(r.Tx.TxHash, h) {
				r.RelayRequested, r.Next = true, next
				return
			}
		}
	}
}

// minedLate marks a send that failed with TX_NOT_CONFIRMED as mined, as tx,
// once its packets are seen: the tx mined after the confirm wait. It returns
// the send when it changed.
func (l *sendLog) minedLate(hashes []string, tx TxRef) (SendRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.sends) - 1; i >= 0; i-- {
		r := l.sends[i]
		if r.Status != sendFailed || r.Error == nil || r.Error.Code != CodeTxNotConfirmed || r.Tx == nil {
			continue
		}
		for _, h := range hashes {
			if strings.EqualFold(r.Tx.TxHash, h) {
				r.Status, r.Error, r.Tx = sendMined, nil, &tx
				r.Warnings = append(r.Warnings, "not mined within the confirm wait; mined later as "+tx.TxHash)
				r.Next = txNext(toolWait, &tx, "Mined after the confirm wait; wait for its packets.")
				return copySend(r), true
			}
		}
	}
	return SendRecord{}, false
}

// noteMinedLate records a TX_NOT_CONFIRMED send that has since mined, in the
// send log and with a mined line under its audit id.
func (s *Server) noteMinedLate(hashes []string, tx TxRef) {
	r, ok := s.sends.minedLate(hashes, tx)
	if !ok {
		return
	}
	_ = s.record(auditRecord{
		ID: r.ID, Tool: toolSend, ChainID: tx.ChainID, Outcome: outcomeMined, TxHash: tx.TxHash,
		Message: "mined after the confirm wait",
	})
}

func copySend(r *SendRecord) SendRecord {
	c := *r
	if r.Tx != nil {
		tx := *r.Tx
		c.Tx = &tx
	}
	c.Packets = append([]ObservedPacket(nil), r.Packets...)
	c.Warnings = append([]string(nil), r.Warnings...)
	return c
}

// sendNextStep is the next step for a send in status st.
func sendNextStep(r SendRecord) NextStep {
	switch r.Status {
	case sendSubmitting:
		return NextStep{Tool: toolListSends, Reason: "Not broadcast yet; check this send again."}
	case sendBroadcast:
		return txNext(toolWait, r.Tx, "Waiting for the source tx to be mined.")
	case sendFailed:
		if r.Error != nil && r.Error.Code == CodeTxNotConfirmed && r.Tx != nil {
			return txNext(toolRelay, r.Tx,
				"Not mined within the confirm wait; it may mine later. Relay it (safe to repeat), then wait; do not send again.")
		}
		return NextStep{Reason: "The send failed; see error."}
	}
	return r.Next
}

// txNext is a call to tool for tx's chain_id and tx_hash.
func txNext(tool string, tx *TxRef, reason string) NextStep {
	if tx == nil {
		return NextStep{Reason: reason}
	}
	return NextStep{Tool: tool, Arguments: map[string]string{"chain_id": tx.ChainID, "tx_hash": tx.TxHash}, Reason: reason}
}

func (s *Server) listSends(
	_ context.Context,
	_ *mcp.CallToolRequest,
	in ListSendsInput,
) (*mcp.CallToolResult, ListSendsOutput, error) {
	limit := in.Limit
	if limit == 0 {
		limit = defaultSendsLimit
	}
	if limit < 1 || limit > maxSends {
		return nil, ListSendsOutput{}, NewError(CodeInvalidArgument, "limit must be between 1 and %d", maxSends)
	}
	sends := s.sends.recent(limit)
	for i := range sends {
		sends[i].Next = sendNextStep(sends[i])
	}
	return nil, ListSendsOutput{Sends: sends}, nil
}
