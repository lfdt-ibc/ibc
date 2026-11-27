// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// SendInput is the input of ibc_ift_send.
type SendInput struct {
	ChainID              string `json:"chain_id"                  jsonschema:"source chain"`
	IFT                  string `json:"ift,omitempty"                    jsonschema:"IFT token address; optional when the chain has one configured token"`
	ClientID             string `json:"client_id"                        jsonschema:"source client, e.g. base-0"`
	To                   string `json:"to"                               jsonschema:"receiver: hex address or signer alias"`
	Amount               string `json:"amount"                           jsonschema:"base units, digits only"`
	From                 string `json:"from,omitempty"                   jsonschema:"signer alias; defaults to the client's signer"`
	PacketTimeoutSeconds int    `json:"packet_timeout_seconds,omitempty" jsonschema:"packet lifetime from now; default 900"`
	Relay                string `json:"relay,omitempty"                  jsonschema:"auto (default): relay unless the client auto-relays; always; never"`
}

// SendOutput is the result of ibc_ift_send.
type SendOutput = SendRecord

func (s *Server) iftSend(
	ctx context.Context,
	req *mcp.CallToolRequest,
	in SendInput,
) (*mcp.CallToolResult, SendOutput, error) {
	reqCtx := ctx
	ctx, cancel := s.bind(ctx)
	defer cancel()

	timeout := defaultSendTimeout
	if in.PacketTimeoutSeconds != 0 {
		timeout = time.Duration(in.PacketTimeoutSeconds) * time.Second
	}
	relayMode := in.Relay
	if relayMode == "" {
		relayMode = relayAuto
	}

	switch {
	case in.ClientID == "":
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "client_id is required"))
	case timeout < minSendTimeout || timeout > maxSendTimeout:
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "packet_timeout_seconds must be between %d and %d",
			int(minSendTimeout.Seconds()), int(maxSendTimeout.Seconds())))
	case relayMode != relayAuto && relayMode != relayAlways && relayMode != relayNever:
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "relay must be auto, always or never"))
	}
	ift, err := s.resolveIFT(in.ChainID, in.IFT)
	if err != nil {
		return s.rejectSend(in, relayMode, err)
	}
	in.IFT = ift

	w, ask, err := s.authorizeWrite(ctx, req, writeRequest{
		chainID: in.ChainID, ift: in.IFT, from: in.From, to: in.To, amount: in.Amount,
		clientID: in.ClientID, timeout: timeout, relay: relayMode,
	})
	if err != nil {
		return s.rejectSend(in, relayMode, err)
	}
	if ask != nil {
		return ask, SendOutput{}, nil
	}
	in.From = w.from
	display := s.displayAmount(ctx, in.ChainID, in.IFT, w.amount)

	if !s.beginWrite() {
		return s.rejectSend(in, relayMode, NewError(CodeRelayerUnavailable, "the MCP server is stopping"))
	}
	rec, err := s.beginAudit(auditRecord{
		Tool: toolSend, ChainID: in.ChainID, IFT: in.IFT, From: in.From, To: w.to,
		Amount: w.amount.String(), ClientID: in.ClientID, Relay: relayMode,
	})
	if err != nil {
		s.endWrite()
		return nil, SendOutput{}, err
	}
	st := s.sends.add(SendRecord{
		ID: rec.ID, Time: s.now().UTC(), ChainID: in.ChainID, ClientID: in.ClientID, IFT: in.IFT,
		From: in.From, To: w.to, Amount: w.amount.String(), AmountDisplay: display, Relay: relayMode, Status: sendSubmitting,
	})

	// The send outlives this call: it returns at broadcast, and mining and the
	// relay request finish in the background, bound only to the server's life.
	broadcast := make(chan struct{})
	var once sync.Once
	finished := make(chan struct{})
	go func() {
		defer s.endWrite()
		defer close(finished)
		s.runSend(rec, req, st, SendRequest{
			ChainID: in.ChainID, IFT: in.IFT, ClientID: in.ClientID, To: w.to, Amount: w.amount,
			From: in.From, Timeout: timeout, ConfirmTimeout: s.confirmTimeout,
			OnBroadcast: func(tx TxRef) {
				s.noteBroadcast(req, tx)
				s.sends.update(st, func(r *SendRecord) { r.Tx, r.Status = &tx, sendBroadcast })
				once.Do(func() { close(broadcast) })
			},
			OnReplaced: func(previous, replacement string) {
				s.noteReplaced(rec, req, in.ChainID, previous, replacement)
				s.sends.update(st, func(r *SendRecord) {
					if r.Status != sendMined {
						r.Tx = &TxRef{ChainID: in.ChainID, TxHash: replacement}
					}
				})
			},
		}, relayMode)
	}()
	select {
	case <-broadcast:
	case <-finished:
	case <-reqCtx.Done():
	}
	out := s.sends.get(st)
	if out.Status == sendFailed {
		return nil, SendOutput{}, out.Error
	}
	out.Next = sendNextStep(out)
	return nil, out, nil
}

// runSend submits the transfer, waits for mining and requests the relay, recording each step on st.
func (s *Server) runSend(rec auditRecord, req *mcp.CallToolRequest, st *SendRecord, sr SendRequest, relayMode string) {
	ctx := s.shutdown
	tx, err := s.chain.IFTSend(ctx, sr)
	// The approval is already spent. Keep a returned hash so a retry of the
	// same approval can show it; never invent one.
	if ref, ok := observedBroadcast(tx, err); ok {
		s.noteBroadcast(req, ref)
	}
	if err != nil {
		_ = s.finish(rec, err)
		te := asToolError(err, CodeChainRPCError)
		s.sends.update(st, func(r *SendRecord) {
			r.Status, r.Error = sendFailed, te
			if te.Tx != nil {
				tx := *te.Tx
				r.Tx = &tx
			}
		})
		return
	}

	var warnings []string
	var packets []ObservedPacket
	requestRelay, skippedForAutoRelay := s.planRelay(ctx, relayMode, sr.ChainID, sr.ClientID)
	relayed, relayFailed := false, false
	if requestRelay {
		got, err := s.relayer.Relay(ctx, tx.ChainID, tx.TxHash, nil)
		if err != nil {
			relayFailed = true
			warnings = append(warnings, "tx is mined but the relay request failed: "+
				asToolError(err, CodeRelayerError).Message)
		} else {
			relayed, packets = true, got
			for _, p := range packets {
				if p.Selection == SelectionUnconfigured {
					warnings = append(warnings, fmt.Sprintf(
						"packet %s/%s has no route on this relayer and will not be delivered by it",
						p.SourceClientID, p.Sequence))
				}
			}
		}
	}
	rec.Outcome = outcomeMined
	rec.TxHash = tx.TxHash
	if err := s.record(rec); err != nil {
		warnings = append(warnings, "audit log update failed: "+err.Error())
	}
	next := sendNext(relayMode, relayed, relayFailed, skippedForAutoRelay, packets)
	s.sends.update(st, func(r *SendRecord) {
		r.Tx, r.Status, r.RelayRequested, r.Packets = &tx, sendMined, relayed, packets
		r.Warnings = append(r.Warnings, warnings...)
		r.Next = next.at(&tx)
	})
}

// rejectSend audits a send refused before submission.
func (s *Server) rejectSend(in SendInput, relay string, err error) (*mcp.CallToolResult, SendOutput, error) {
	s.note(auditRecord{
		Tool: "ibc_ift_send", ChainID: in.ChainID, IFT: in.IFT, From: in.From, To: in.To,
		Amount: in.Amount, ClientID: in.ClientID, Relay: relay,
	}, err)
	return nil, SendOutput{}, err
}

// planRelay applies the relay mode; auto skips the request when the source client end has autoRelay.
func (s *Server) planRelay(ctx context.Context, mode, chainID, clientID string) (request, skippedForAutoRelay bool) {
	switch mode {
	case relayNever:
		return false, false
	case relayAlways:
		return true, false
	}
	end, found, err := s.clientEnd(ctx, chainID, clientID)
	if err != nil || !found {
		return true, false
	}
	return !end.AutoRelay, end.AutoRelay
}

// sendNext is the next step after a mined transfer. It waits only when a packet was selected,
// and names no tool when no packet is left for this relayer. Arguments are set by at.
func sendNext(mode string, requested, failed, skippedForAutoRelay bool, packets []ObservedPacket) NextStep {
	var unconfigured []string
	selected, notSelected := false, 0
	for _, p := range packets {
		switch p.Selection {
		case SelectionUnconfigured:
			unconfigured = append(unconfigured, p.SourceClientID+"/"+p.Sequence)
		case SelectionSelected:
			selected = true
		case SelectionNotSelected:
			notSelected++
		}
	}
	var reasons []string
	if len(unconfigured) > 0 {
		reasons = append(reasons, unconfiguredNext(unconfigured))
	}
	if len(packets) > 0 && len(unconfigured) == len(packets) {
		return NextStep{Reason: "The transfer is mined. " + reasons[0]}
	}
	var tool string
	switch {
	case failed:
		tool, reasons = toolRelay, append(reasons, nextRelayFailed)
	case mode == relayNever:
		tool, reasons = toolRelay, append(reasons, nextNever)
	case skippedForAutoRelay:
		tool, reasons = toolWait, append(reasons, nextAutoRelay)
	case !requested:
	case selected:
		tool, reasons = toolWait, append(reasons, nextWait)
	case len(unconfigured) > 0:
	case len(packets) == 0:
		tool, reasons = toolRelay, append(reasons, nextNoPackets)
	case notSelected == len(packets):
		tool, reasons = toolRelay, append(reasons, nextNotSelected)
	default:
		tool, reasons = toolRelay, append(reasons, nextNotQueued)
	}
	return NextStep{Tool: tool, Reason: strings.Join(reasons, " ")}
}

// observedBroadcast returns the broadcast tx hash, taken from the tool error when IFTSend failed.
func observedBroadcast(tx TxRef, err error) (TxRef, bool) {
	if tx.TxHash != "" {
		return tx, true
	}
	var te *ToolError
	if errors.As(err, &te) && te.Tx != nil && te.Tx.TxHash != "" {
		return *te.Tx, true
	}
	return TxRef{}, false
}
