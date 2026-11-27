// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/lfdt-ibc/ibc/cli/internal/otel"
)

func TestToolCallMetrics(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{
		PollInterval: time.Millisecond,
		Meter:        provider.Meter("ibc.relayer"),
	})
	require.NoError(t, err)
	srv.skipConfirmation = true

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := srv.MCP().Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	clientSession, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Wait()
		_ = srv.Close()
	})

	// ACT
	okRes, err := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_get_context"})
	require.NoError(t, err)
	require.False(t, okRes.IsError)

	badRes, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "ibc_ift_balance",
		Arguments: map[string]any{
			"chain_id": chainA,
			"ift":      "not-an-address",
			"address":  agentKey,
		},
	})
	require.NoError(t, err)
	require.True(t, badRes.IsError)

	const supplied = "0x1111111111111111111111111111111111111111"
	_, err = clientSession.CallTool(ctx, &mcp.CallToolParams{Name: supplied})
	require.Error(t, err)

	// ASSERT
	points := collectToolCalls(t, reader)
	require.Len(t, points, 3)
	require.Equal(t, int64(1), seriesValue(t, points, "ibc_get_context", "ok", ""))
	require.Equal(t, int64(1), seriesValue(t, points, "ibc_ift_balance", "error", string(CodeInvalidArgument)))
	require.Equal(t, int64(1), seriesValue(t, points, "unknown", "error", "other"))
	for _, point := range points {
		for _, kv := range point.Attributes.ToSlice() {
			require.NotContains(t, kv.Value.AsString(), "not-an-address")
			require.NotContains(t, kv.Value.AsString(), supplied)
		}
	}
}

func collectToolCalls(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.DataPoint[int64] {
	t.Helper()

	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resources))
	for _, scope := range resources.ScopeMetrics {
		if scope.Scope.Name != "ibc.relayer" {
			continue
		}
		for _, m := range scope.Metrics {
			if m.Name != "mcp_tool_calls_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			return sum.DataPoints
		}
	}
	require.FailNow(t, "mcp_tool_calls_total not found")
	return nil
}

func seriesValue(
	t *testing.T,
	points []metricdata.DataPoint[int64],
	tool, result, code string,
) int64 {
	t.Helper()

	want := attribute.NewSet(
		otel.AttrTool.String(tool),
		otel.AttrResult.String(result),
		otel.AttrCode.String(code),
	)
	for _, point := range points {
		if point.Attributes.Equals(&want) {
			return point.Value
		}
	}
	require.FailNowf(t, "missing series", "tool=%s result=%s code=%s", tool, result, code)
	return 0
}

func TestToolCallMetricsCountApprovalByProtocol(t *testing.T) {
	// Metrics wrap the SDK's multi round-trip middleware: an older client is
	// prompted within one call, a 2026-07-28 client calls again.
	for version, calls := range map[string]int64{"2025-11-25": 1, "2026-07-28": 2} {
		t.Run(version, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

			h := newConfirmingHarness(t, clientOpts{
				protocolVersion: version,
				meter:           provider.Meter("ibc.relayer"),
				elicit: func(*mcp.ElicitParams) (*mcp.ElicitResult, error) {
					return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}}, nil
				},
			})
			_, terr := h.call("ibc_ift_send", sendArgs("10"), nil)
			require.Nil(t, terr)
			require.Equal(t, 1, h.chain.sendCount())

			points := collectToolCalls(t, reader)
			require.Len(t, points, 1)
			require.Equal(t, calls, seriesValue(t, points, "ibc_ift_send", "ok", ""))
		})
	}
}
