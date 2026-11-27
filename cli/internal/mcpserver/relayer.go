// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"connectrpc.com/connect"

	"github.com/lfdt-ibc/ibc/cli/api/v2/relayer"
)

var _ RelayerBackend = (*RelayerAPI)(nil)

var stateProto = map[PacketState]relayer.PacketState{
	StateNotSelected: relayer.PacketState_PACKET_STATE_NOT_SELECTED,
	StatePending:     relayer.PacketState_PACKET_STATE_PENDING,
	StateSucceeded:   relayer.PacketState_PACKET_STATE_SUCCEEDED,
	StateTimedOut:    relayer.PacketState_PACKET_STATE_TIMED_OUT,
	StateRejected:    relayer.PacketState_PACKET_STATE_REJECTED,
	StateRelayFailed: relayer.PacketState_PACKET_STATE_RELAY_FAILED,
}

// RelayerAPI adapts RelayerApiService to RelayerBackend, in-process via the
// relayer's own handler (same validation, no network hop) or via a Connect client.
type RelayerAPI struct {
	api relayer.RelayerApiServiceClient
}

// NewRelayerAPI wraps api: a RelayerApiServiceHandler or a client for one.
func NewRelayerAPI(api relayer.RelayerApiServiceClient) *RelayerAPI {
	return &RelayerAPI{api: api}
}

func (r *RelayerAPI) Relay(
	ctx context.Context, chainID, txHash string, packets []PacketSelector,
) ([]ObservedPacket, error) {
	req := &relayer.RelayRequest{TxHash: txHash, SourceChainId: chainID}
	if len(packets) == 0 {
		req.Selection = &relayer.RelayRequest_AllPackets{AllPackets: &relayer.AllPackets{}}
	} else {
		selected := make([]*relayer.PacketSelector, 0, len(packets))
		for _, p := range packets {
			seq, err := strconv.ParseUint(p.Sequence, 10, 64)
			if err != nil {
				return nil, NewError(CodeInvalidArgument, "invalid sequence %q", p.Sequence)
			}
			selected = append(selected, &relayer.PacketSelector{SourceClientId: p.SourceClientID, SequenceNumber: seq})
		}
		req.Selection = &relayer.RelayRequest_SelectedPackets{
			SelectedPackets: &relayer.SelectedPackets{Packets: selected},
		}
	}

	res, err := r.api.Relay(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, relayTxError(err)
	}

	out := make([]ObservedPacket, 0, len(res.Msg.GetPackets()))
	for _, p := range res.Msg.GetPackets() {
		out = append(out, ObservedPacket{
			SourceClientID: p.GetSourceClientId(),
			Sequence:       strconv.FormatUint(p.GetSequenceNumber(), 10),
			Selection:      selectionFromProto(p.GetSelection()),
		})
	}
	return out, nil
}

func (r *RelayerAPI) Packets(
	ctx context.Context, filter PacketFilter, limit uint32, cursor string,
) (PacketsPage, error) {
	pf := &relayer.PacketFilter{
		SourceChainId:       nonEmpty(filter.SourceChainID),
		DestinationChainId:  nonEmpty(filter.DestinationChainID),
		SourceClientId:      nonEmpty(filter.SourceClientID),
		DestinationClientId: nonEmpty(filter.DestinationClientID),
		SourceTxHash:        nonEmpty(filter.SourceTxHash),
		SequenceNumber:      filter.Sequence,
	}
	if filter.State != "" {
		state, ok := stateToProto(filter.State)
		if !ok {
			return PacketsPage{}, NewError(CodeInvalidArgument, "unknown state %q", filter.State)
		}
		pf.State = &state
	}

	res, err := r.api.Packets(ctx, connect.NewRequest(&relayer.PacketsRequest{
		Filter: pf, Limit: limit, Cursor: cursor,
	}))
	if err != nil {
		return PacketsPage{}, relayerError(err)
	}

	page := PacketsPage{
		Packets:    make([]PacketStatus, 0, len(res.Msg.GetPackets())),
		HasMore:    res.Msg.GetHasMore(),
		NextCursor: res.Msg.GetNextCursor(),
	}
	for _, p := range res.Msg.GetPackets() {
		page.Packets = append(page.Packets, packetFromProto(p))
	}
	return page, nil
}

func packetFromProto(p *relayer.PacketStatus) PacketStatus {
	state := stateFromProto(p.GetState())
	return PacketStatus{
		SourceClientID: p.GetSourceClientId(),
		Sequence:       strconv.FormatUint(p.GetSequenceNumber(), 10),
		State:          state,
		Final:          state.Final(),
		SendTx:         txFromProto(p.GetSendTx()),
		RecvTx:         txFromProto(p.GetRecvTx()),
		AckTx:          txFromProto(p.GetAckTx()),
		TimeoutTx:      txFromProto(p.GetTimeoutTx()),
	}
}

func txFromProto(tx *relayer.TransactionInfo) *TxRef {
	if tx == nil || tx.GetTxHash() == "" {
		return nil
	}
	return &TxRef{ChainID: tx.GetChainId(), TxHash: tx.GetTxHash()}
}

func stateToProto(s PacketState) (relayer.PacketState, bool) {
	v, ok := stateProto[s]
	return v, ok
}

func stateFromProto(s relayer.PacketState) PacketState {
	for k, v := range stateProto {
		if v == s {
			return k
		}
	}
	return StateUnspecified
}

func selectionFromProto(s relayer.PacketSelection) PacketSelection {
	switch s {
	case relayer.PacketSelection_PACKET_SELECTION_SELECTED:
		return SelectionSelected
	case relayer.PacketSelection_PACKET_SELECTION_NOT_SELECTED:
		return SelectionNotSelected
	case relayer.PacketSelection_PACKET_SELECTION_UNCONFIGURED:
		return SelectionUnconfigured
	default:
		return SelectionUnspecified
	}
}

// relayTxError reports NotFound "no packets found" as PACKETS_NOT_FOUND, which is
// retryable: a just-mined tx returns it until the node serves the receipt.
func relayTxError(err error) *ToolError {
	var ce *connect.Error
	if errors.As(err, &ce) && ce.Code() == connect.CodeNotFound &&
		strings.Contains(ce.Message(), "no packets found") {
		return NewError(CodePacketsNotFound, "relayer: %s. Retry ibc_relay_tx", ce.Message())
	}
	return relayerError(err)
}

// relayerError classifies a Connect error: transport failures are retryable.
func relayerError(err error) *ToolError {
	var ce *connect.Error
	if errors.As(err, &ce) {
		switch ce.Code() {
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeCanceled:
			return NewError(CodeRelayerUnavailable, "relayer: %s", ce.Message())
		case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeFailedPrecondition:
			return NewError(CodeInvalidArgument, "relayer: %s", ce.Message())
		default:
			return NewError(CodeRelayerError, "relayer: %s", ce.Message())
		}
	}
	return NewError(CodeRelayerUnavailable, "relayer: %s", err.Error())
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
