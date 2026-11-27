// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/lfdt-ibc/ibc/cli/api/v2/relayer"
)

type stubRelayerService struct {
	lastRelay   *relayer.RelayRequest
	lastPackets *relayer.PacketsRequest
	relayErr    error
}

func (s *stubRelayerService) Relay(
	_ context.Context, req *connect.Request[relayer.RelayRequest],
) (*connect.Response[relayer.RelayResponse], error) {
	if s.relayErr != nil {
		return nil, s.relayErr
	}
	s.lastRelay = req.Msg
	return connect.NewResponse(&relayer.RelayResponse{Packets: []*relayer.ObservedPacket{
		{SourceClientId: "base-0", SequenceNumber: 7, Selection: relayer.PacketSelection_PACKET_SELECTION_SELECTED},
		{SourceClientId: "op-0", SequenceNumber: 8, Selection: relayer.PacketSelection_PACKET_SELECTION_UNCONFIGURED},
	}}), nil
}

func (s *stubRelayerService) Packets(
	_ context.Context, req *connect.Request[relayer.PacketsRequest],
) (*connect.Response[relayer.PacketsResponse], error) {
	s.lastPackets = req.Msg
	if req.Msg.GetCursor() == "boom" {
		return nil, connect.NewError(connect.CodeInvalidArgument, nil)
	}
	return connect.NewResponse(&relayer.PacketsResponse{
		Packets: []*relayer.PacketStatus{{
			State:          relayer.PacketState_PACKET_STATE_SUCCEEDED,
			SequenceNumber: 7,
			SourceClientId: "base-0",
			SendTx:         &relayer.TransactionInfo{TxHash: "0xsend", ChainId: "1"},
			RecvTx:         &relayer.TransactionInfo{TxHash: "0xrecv", ChainId: "8453"},
			AckTx:          &relayer.TransactionInfo{TxHash: "0xack", ChainId: "1"},
		}},
		HasMore:    true,
		NextCursor: "c2",
	}), nil
}

func TestRelayerAPIOverConnect(t *testing.T) {
	stub := &stubRelayerService{}
	mux := http.NewServeMux()
	mux.Handle(relayer.NewRelayerApiServiceHandler(stub))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	api := NewRelayerAPI(relayer.NewRelayerApiServiceClient(ts.Client(), ts.URL))
	ctx := context.Background()

	observed, err := api.Relay(ctx, "1", txHash, nil)
	require.NoError(t, err)
	require.NotNil(t, stub.lastRelay.GetAllPackets())
	require.Equal(t, []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "7", Selection: SelectionSelected},
		{SourceClientID: "op-0", Sequence: "8", Selection: SelectionUnconfigured},
	}, observed)

	_, err = api.Relay(ctx, "1", txHash, []PacketSelector{{SourceClientID: "base-0", Sequence: "7"}})
	require.NoError(t, err)
	require.Equal(t, uint64(7), stub.lastRelay.GetSelectedPackets().GetPackets()[0].GetSequenceNumber())

	seq := uint64(7)
	page, err := api.Packets(
		ctx,
		PacketFilter{SourceChainID: "1", SourceTxHash: txHash, Sequence: &seq, State: StatePending},
		50,
		"",
	)
	require.NoError(t, err)
	f := stub.lastPackets.GetFilter()
	require.Equal(t, "1", f.GetSourceChainId())
	require.Equal(t, txHash, f.GetSourceTxHash())
	require.Equal(t, relayer.PacketState_PACKET_STATE_PENDING, f.GetState())
	require.Nil(t, f.DestinationChainId, "empty filters are left unset")
	require.Equal(t, uint32(50), stub.lastPackets.GetLimit())

	require.True(t, page.HasMore)
	require.Equal(t, "c2", page.NextCursor)
	require.Equal(t, PacketStatus{
		SourceClientID: "base-0", Sequence: "7", State: StateSucceeded, Final: true,
		SendTx: &TxRef{ChainID: "1", TxHash: "0xsend"},
		RecvTx: &TxRef{ChainID: "8453", TxHash: "0xrecv"},
		AckTx:  &TxRef{ChainID: "1", TxHash: "0xack"},
	}, page.Packets[0])

	_, err = api.Packets(ctx, PacketFilter{}, 1, "boom")
	invalid := asToolError(err, "")
	require.Equal(t, CodeInvalidArgument, invalid.Code)
	require.False(t, invalid.Retryable)

	ts.Close()
	_, err = api.Packets(ctx, PacketFilter{}, 1, "")
	te := asToolError(err, "")
	require.Equal(t, CodeRelayerUnavailable, te.Code)
	require.True(t, te.Retryable)
}

func TestStateMappingCoversProto(t *testing.T) {
	for name, v := range relayer.PacketState_value {
		st := stateFromProto(relayer.PacketState(v))
		if name == "PACKET_STATE_UNSPECIFIED" {
			require.Equal(t, StateUnspecified, st)
			continue
		}
		back, ok := stateToProto(st)
		require.True(t, ok, name)
		require.Equal(t, relayer.PacketState(v), back, name)
	}
	require.Len(t, PacketStates, len(relayer.PacketState_value)-1)
}

// Inside `ibc relayer run` the adapter wraps the API's handler directly.
func TestRelayerAPIInProcess(t *testing.T) {
	api := NewRelayerAPI(&stubRelayerService{})
	page, err := api.Packets(context.Background(), PacketFilter{SourceTxHash: txHash}, 1, "")
	require.NoError(t, err)
	require.Equal(t, StateSucceeded, page.Packets[0].State)

	_, err = api.Packets(context.Background(), PacketFilter{}, 1, "boom")
	invalid := asToolError(err, "")
	require.Equal(t, CodeInvalidArgument, invalid.Code)
	require.False(t, invalid.Retryable)
}

func TestRelayNotFoundIsRetryable(t *testing.T) {
	stub := &stubRelayerService{
		relayErr: connect.NewError(connect.CodeNotFound, errors.New("no packets found: transaction 0xabc on chain 1")),
	}
	api := NewRelayerAPI(stub)

	_, err := api.Relay(context.Background(), "1", txHash, nil)
	te := asToolError(err, "")
	require.Equal(t, CodePacketsNotFound, te.Code)
	require.True(t, te.Retryable)
	require.Contains(t, te.Message, "no packets found")
	require.Contains(t, te.Message, "Retry ibc_relay_tx")
	require.NotContains(t, te.Message, "ibc_ift_send")
	require.NotContains(t, te.Message, "send again")

	stub.relayErr = connect.NewError(connect.CodeInvalidArgument, errors.New("bad sequence"))
	_, err = api.Relay(context.Background(), "1", txHash, nil)
	te = asToolError(err, "")
	require.Equal(t, CodeInvalidArgument, te.Code)
	require.False(t, te.Retryable)
	require.NotContains(t, te.Message, "ibc_ift_send")

	stub.relayErr = connect.NewError(connect.CodeNotFound, errors.New("client for chain \"1\": not found"))
	_, err = api.Relay(context.Background(), "1", txHash, nil)
	te = asToolError(err, "")
	require.Equal(t, CodeInvalidArgument, te.Code)
	require.False(t, te.Retryable, "only the no-packets relay result is retryable")
}
