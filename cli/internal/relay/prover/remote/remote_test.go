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
	proverv2 "github.com/lfdt-ibc/ibc/cli/api/v2/prover"
	"github.com/lfdt-ibc/ibc/cli/internal/config"
	"github.com/lfdt-ibc/ibc/cli/internal/network"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

func TestProverNonpositiveRequestTimeout(t *testing.T) {
	_, handler := proverv2.NewProverServiceHandler(proverv2.UnimplementedProverServiceHandler{})
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	endpoint := network.Endpoint{
		URL: server.URL,
		TLS: server.Client().Transport.(*http.Transport).TLSClientConfig.Clone(),
	}

	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			for _, prover := range []*Prover{
				New(server.Client(), server.URL, "chain-a", "client-0", timeout, slog.Default()),
				NewFromEndpoint(endpoint, "chain-a", "client-0", timeout, slog.Default()),
			} {
				require.Equal(t, config.DefaultRemoteProverRequestTimeout, prover.requestTimeout)
				_, _, err := prover.LatestProvableHeight(t.Context())
				require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err),
					"nonpositive timeout must allow the request to reach the server")
			}
			require.Equal(t, config.DefaultRemoteProverRequestTimeout, newHTTPClient(nil, timeout).Timeout)
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
				prover := NewFromEndpoint(endpoint, "chain-a", "client-0", timeout, slog.Default())
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
