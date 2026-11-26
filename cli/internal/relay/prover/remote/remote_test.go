// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	channeltypesv2 "github.com/cosmos/ibc-go/v11/modules/core/04-channel/v2/types"
	"github.com/lfdt-ibc/ibc/cli/internal/network"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

func TestHTTPClientRequestTimeout(t *testing.T) {
	for _, tt := range []struct {
		name    string
		timeout *time.Duration
		want    time.Duration
	}{
		{name: "unset", want: 30 * time.Second},
		{name: "zero", timeout: new(time.Duration(0)), want: 30 * time.Second},
		{name: "negative", timeout: new(-time.Second), want: 30 * time.Second},
		{name: "configured", timeout: new(250 * time.Millisecond), want: 250 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, newHTTPClient(nil, tt.timeout).Timeout)
		})
	}
}

func TestProverRequestCancellation(t *testing.T) {
	for _, method := range []string{"LatestProvableHeight", "ClientUpdatePayloads", "PacketProofs"} {
		for _, reason := range []string{"request timeout", "parent deadline", "parent cancellation"} {
			t.Run(method+"/"+reason, func(t *testing.T) {
				safetyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ctx := safetyCtx
				const shortTimeout = 50 * time.Millisecond
				timeout := time.Minute
				wantCode := connect.CodeDeadlineExceeded
				switch reason {
				case "request timeout":
					timeout = shortTimeout
				case "parent deadline":
					var deadlineCancel context.CancelFunc
					ctx, deadlineCancel = context.WithTimeout(ctx, shortTimeout)
					defer deadlineCancel()
				case "parent cancellation":
					wantCode = connect.CodeCanceled
				}

				server := httptest.NewUnstartedServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					if reason == "parent cancellation" {
						cancel()
					}
					<-r.Context().Done()
				}))
				server.EnableHTTP2 = true
				server.StartTLS()
				t.Cleanup(server.Close)
				endpoint := network.Endpoint{
					URL: server.URL,
					TLS: server.Client().Transport.(*http.Transport).TLSClientConfig.Clone(),
				}
				prover := NewFromEndpoint(endpoint, "chain-a", "client-0", &timeout, slog.Default())
				var err error
				switch method {
				case "LatestProvableHeight":
					_, _, err = prover.LatestProvableHeight(ctx)
				case "ClientUpdatePayloads":
					_, err = prover.ClientUpdatePayloads(ctx, 1)
				case "PacketProofs":
					_, err = prover.PacketProofs(ctx, 1, v2.ProofKindPacketCommitment,
						[]channeltypesv2.Packet{{Sequence: 1}}, nil)
				}
				require.Error(t, err)
				require.Equal(t, wantCode, connect.CodeOf(err))
				require.NotErrorIs(t, safetyCtx.Err(), context.DeadlineExceeded,
					"outer safety deadline must not expire")
			})
		}
	}
}

func TestPacketProofsRejectsAcknowledgementCountMismatch(t *testing.T) {
	for _, acks := range [][]channeltypesv2.Acknowledgement{nil, {{}, {}}} {
		prover := &Prover{}
		_, err := prover.PacketProofs(
			context.Background(),
			1,
			v2.ProofKindAcknowledgement,
			[]channeltypesv2.Packet{{Sequence: 1}},
			acks,
		)
		require.ErrorContains(t, err, "acknowledgement count must match packet count")
	}
}
