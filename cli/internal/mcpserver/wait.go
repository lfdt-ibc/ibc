// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Reasons of the next step after a wait.
const (
	waitDoneHint        = "Every packet finished. Only succeeded means delivered; timed_out, rejected and relay_failed failed."
	waitNoPacketsHint   = "No packets seen yet; ask the relayer to deliver them, then wait again."
	waitNotSelectedHint = "Some packets are not_selected; relay them, then wait again."
	waitMovingHint      = "Packets are still moving; wait again."
	waitMiningHint      = "The source tx is not mined yet; wait again."
	waitBusyHint        = "Too many waits are in progress on this server; wait again."
	// waitCanceledMessage names the next call, and that it is not ibc_ift_send.
	waitCanceledMessage = "The wait was cancelled. Call ibc_wait_for_packets again, not ibc_ift_send."
)

// WaitInput is the input of ibc_wait_for_packets.
type WaitInput struct {
	ChainID        string `json:"chain_id"                  jsonschema:"source chain"`
	TxHash         string `json:"tx_hash"                   jsonschema:"source tx"`
	Until          string `json:"until,omitempty"           jsonschema:"final (default) or received (recv tx seen)"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty" jsonschema:"seconds; default 45, maximum 1800"`
}

// WaitSummary counts packets by state.
type WaitSummary struct {
	Succeeded   int `json:"succeeded"`
	Pending     int `json:"pending"`
	TimedOut    int `json:"timed_out"`
	Rejected    int `json:"rejected"`
	RelayFailed int `json:"relay_failed"`
	NotSelected int `json:"not_selected"`
}

// WaitOutput is the result of ibc_wait_for_packets.
type WaitOutput struct {
	Done           bool           `json:"done"            jsonschema:"every packet reached until; check each state"`
	TimedOut       bool           `json:"timed_out"       jsonschema:"timeout_seconds passed first; not an error"`
	ElapsedSeconds int            `json:"elapsed_seconds"`
	Summary        WaitSummary    `json:"summary"`
	Packets        []PacketStatus `json:"packets"`
	Next           NextStep       `json:"next"`
}

// progressNotifier reports packet state changes to clients that sent a
// progress token. Progress counts packets that reached the until condition.
type progressNotifier struct {
	req   *mcp.CallToolRequest
	token any
	step  float64
}

func (s *Server) waitForPackets(
	ctx context.Context,
	req *mcp.CallToolRequest,
	in WaitInput,
) (*mcp.CallToolResult, WaitOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	if err := requireTxHash(in.TxHash); err != nil {
		return nil, WaitOutput{}, err
	}
	until := in.Until
	if until == "" {
		until = untilFinal
	}
	if until != untilFinal && until != untilReceived {
		return nil, WaitOutput{}, NewError(CodeInvalidArgument, "until must be final or received")
	}
	timeout := s.waitDefault
	if in.TimeoutSeconds > 0 {
		timeout = time.Duration(in.TimeoutSeconds) * time.Second
	}
	if timeout > s.waitMax {
		timeout = s.waitMax
	}

	start := time.Now()
	// waitCtx bounds each relayer query as well as the sleep between them, so
	// a slow or paginated query cannot hold the call past timeout_seconds.
	waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
	defer waitCancel()
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()

	notifier := newProgressNotifier(req)
	seen := map[string]PacketState{}
	var packets []PacketStatus
	var lastErr *ToolError
	used := in.TxHash
	mining := false
	result := func(done, timedOut bool, next NextStep) (*mcp.CallToolResult, WaitOutput, error) {
		out := waitResult(packets, done, timedOut, start)
		out.Next = next.at(&TxRef{ChainID: in.ChainID, TxHash: used})
		// A follow-up wait keeps this wait's goal; final is the default.
		if out.Next.Tool == toolWait && until != untilFinal {
			out.Next.Arguments["until"] = until
		}
		if note := replacedNote(in.TxHash, used); note != "" {
			out.Next.Reason = note + " " + out.Next.Reason
		}
		return nil, out, nil
	}

	// Over the cap, queue for a slot within the same timeout, without polling.
	select {
	case s.waitSlots <- struct{}{}:
		defer func() { <-s.waitSlots }()
		// Ticks while queued are dropped, so polling starts on its own cadence.
		ticker.Reset(s.poll)
	case <-ctx.Done():
		return nil, WaitOutput{}, waitCancelError(ctx.Err())
	case <-waitCtx.Done():
		if ctx.Err() != nil {
			return nil, WaitOutput{}, waitCancelError(ctx.Err())
		}
		return result(false, true, NextStep{Tool: toolWait, Reason: waitBusyHint})
	}

	for {
		got, gotHash, err := s.txPackets(waitCtx, in.ChainID, in.TxHash)
		if err == nil {
			packets, used, lastErr = got, gotHash, nil
			if len(packets) > 0 {
				// Packets exist only once the source tx mined.
				s.noteMinedLate(s.replaced.family(in.TxHash), TxRef{ChainID: in.ChainID, TxHash: used})
			}
			notifier.changes(ctx, seen, packets, until)
			if len(packets) > 0 && allReached(packets, until) {
				return result(true, false, NextStep{Reason: waitDoneHint})
			}
			// not_selected is not in progress. Return the relay hint now
			// instead of holding the call until timeout_seconds.
			if countState(packets, StateNotSelected) > 0 {
				return result(false, false, NextStep{Tool: toolRelay, Reason: waitNotSelectedHint})
			}
			// Without packets, a send of this server says whether waiting can help.
			if len(packets) == 0 {
				if rec, ok := s.sends.byHash(s.replaced.family(in.TxHash)); ok {
					switch {
					case rec.Status == sendFailed:
						next := sendNextStep(rec)
						if next.Tool == "" {
							return nil, WaitOutput{}, rec.Error
						}
						return result(false, false, next)
					case rec.Status == sendMined && rec.Next.Tool != toolWait:
						return result(false, false, rec.Next)
					}
					mining = rec.Status != sendMined
				}
			}
		}

		if ctx.Err() != nil {
			return nil, WaitOutput{}, waitCancelError(ctx.Err())
		}
		if waitCtx.Err() != nil {
			// The wait's own deadline, not a cancellation: a normal timed-out result.
			return result(false, true, waitTimeoutNext(packets, lastErr, mining))
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, WaitOutput{}, waitCancelError(err)
			}
			lastErr = asToolError(err, CodeRelayerError)
			if !lastErr.Retryable {
				return nil, WaitOutput{}, lastErr
			}
		}

		select {
		case <-ctx.Done():
			return nil, WaitOutput{}, waitCancelError(ctx.Err())
		case <-waitCtx.Done():
			return result(false, true, waitTimeoutNext(packets, lastErr, mining))
		case <-ticker.C:
		}
	}
}

// waitCancelError reports a cancelled wait as RELAYER_UNAVAILABLE, which is retryable:
// waiting again is a read. A CHAIN_RPC_ERROR or RELAYER_UNAVAILABLE that is not itself
// a context error is kept.
func waitCancelError(err error) *ToolError {
	var te *ToolError
	if errors.As(err, &te) && (te.Code == CodeChainRPCError || te.Code == CodeRelayerUnavailable) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return te
	}
	return NewError(CodeRelayerUnavailable, "%s", waitCanceledMessage)
}

// txPackets lists a source tx's packets, trying each re-signed hash newest
// first, and returns the hash that had them.
func (s *Server) txPackets(ctx context.Context, chainID, txHash string) ([]PacketStatus, string, error) {
	for _, h := range s.replaced.family(txHash) {
		packets, err := s.txHashPackets(ctx, chainID, h)
		if err != nil {
			return nil, txHash, err
		}
		if len(packets) > 0 {
			return packets, h, nil
		}
	}
	return []PacketStatus{}, txHash, nil
}

// txHashPackets lists every packet of one source tx hash across all pages.
func (s *Server) txHashPackets(ctx context.Context, chainID, txHash string) ([]PacketStatus, error) {
	filter := PacketFilter{SourceChainID: chainID, SourceTxHash: txHash}
	all := []PacketStatus{}
	seen := map[string]struct{}{}
	cursor := ""
	for {
		if _, dup := seen[cursor]; dup {
			return nil, NewError(CodeRelayerError, "packet page cursor %q repeated", cursor)
		}
		seen[cursor] = struct{}{}
		page, err := s.relayer.Packets(ctx, filter, maxPacketsLimit, cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Packets...)
		if !page.HasMore || page.NextCursor == "" {
			return all, nil
		}
		cursor = page.NextCursor
	}
}

func reached(p PacketStatus, until string) bool {
	if p.Final {
		return true
	}
	return until == untilReceived && p.RecvTx != nil
}

func allReached(packets []PacketStatus, until string) bool {
	for _, p := range packets {
		if !reached(p, until) {
			return false
		}
	}
	return true
}

// waitTimeoutNext is the next step when the wait ended before done.
func waitTimeoutNext(packets []PacketStatus, lastErr *ToolError, mining bool) NextStep {
	switch {
	case lastErr != nil:
		return NextStep{Tool: toolWait, Reason: "The relayer was unreachable at the last poll: " +
			strings.TrimSpace(lastErr.Message)}
	case len(packets) == 0 && mining:
		return NextStep{Tool: toolWait, Reason: waitMiningHint}
	case len(packets) == 0:
		return NextStep{Tool: toolRelay, Reason: waitNoPacketsHint}
	case countState(packets, StateNotSelected) > 0:
		return NextStep{Tool: toolRelay, Reason: waitNotSelectedHint}
	default:
		return NextStep{Tool: toolWait, Reason: waitMovingHint}
	}
}

func countState(packets []PacketStatus, st PacketState) int {
	n := 0
	for _, p := range packets {
		if p.State == st {
			n++
		}
	}
	return n
}

func waitResult(packets []PacketStatus, done, timedOut bool, start time.Time) WaitOutput {
	if packets == nil {
		packets = []PacketStatus{}
	}
	out := WaitOutput{
		Done: done, TimedOut: timedOut, Packets: packets,
		ElapsedSeconds: int(time.Since(start).Seconds()),
	}
	for _, p := range packets {
		switch p.State {
		case StateSucceeded:
			out.Summary.Succeeded++
		case StatePending:
			out.Summary.Pending++
		case StateTimedOut:
			out.Summary.TimedOut++
		case StateRejected:
			out.Summary.Rejected++
		case StateRelayFailed:
			out.Summary.RelayFailed++
		case StateNotSelected:
			out.Summary.NotSelected++
		}
	}
	return out
}

func newProgressNotifier(req *mcp.CallToolRequest) *progressNotifier {
	n := &progressNotifier{req: req}
	if req != nil && req.Params != nil {
		n.token = req.Params.GetProgressToken()
	}
	return n
}

// changes notifies one progress step per packet whose state differs from seen.
func (n *progressNotifier) changes(
	ctx context.Context,
	seen map[string]PacketState,
	packets []PacketStatus,
	until string,
) {
	if n.token == nil || n.req.Session == nil {
		return
	}
	done := 0
	for _, p := range packets {
		if reached(p, until) {
			done++
		}
	}
	for _, p := range packets {
		key := p.SourceClientID + "/" + p.Sequence
		prev, known := seen[key]
		if known && prev == p.State {
			continue
		}
		seen[key] = p.State

		msg := fmt.Sprintf("packet %s: %s", key, p.State)
		if known {
			msg = fmt.Sprintf("packet %s: %s -> %s", key, prev, p.State)
		}
		// Progress must strictly increase and packets can appear mid-wait, so it
		// counts state changes; the message carries the packet tally.
		n.step++
		_ = n.req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
			ProgressToken: n.token,
			Message:       fmt.Sprintf("%s (%d/%d packets done)", msg, done, len(packets)),
			Progress:      n.step,
		})
	}
}
