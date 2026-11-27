// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"math/big"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// writeRequest is an ibc_ift_send call before authorization. ibc_ift_send is
// the only tool that signs.
type writeRequest struct {
	chainID, ift, from, to, amount, clientID, relay string
	timeout                                         time.Duration
}

// authorizedWrite is a send the relayer's client signer may make.
type authorizedWrite struct {
	from, to string
	amount   *big.Int
}

// authorizeWrite checks the send against this relayer's connections, then returns ask to get
// user approval via a multi round-trip request; the checks rerun on the client's retry.
func (s *Server) authorizeWrite(
	ctx context.Context, req *mcp.CallToolRequest, w writeRequest,
) (_ authorizedWrite, ask *mcp.CallToolResult, _ error) {
	if err := requireIFT(w.ift); err != nil {
		return authorizedWrite{}, nil, err
	}
	amount, ok := parseAmount(w.amount)
	// ABI packing reduces a uint256 modulo 2^256, so a larger amount would be
	// approved and audited as one number and sent as another.
	if !ok || amount.Sign() == 0 || amount.BitLen() > 256 {
		return authorizedWrite{}, nil, NewError(CodeInvalidArgument,
			"amount %q must be a positive integer in base units, at most 2^256-1", w.amount)
	}
	end, found, err := s.clientEnd(ctx, w.chainID, w.clientID)
	if err != nil {
		return authorizedWrite{}, nil, asToolError(err, CodeConfigError)
	}
	if !found {
		return authorizedWrite{}, nil, NewError(
			CodePolicyDenied, "client %q on chain %q is not on this relayer; pick a connection end from ibc_get_context", w.clientID, w.chainID)
	}
	if w.from == "" {
		w.from = end.Signer
	}
	if w.from != end.Signer {
		return authorizedWrite{}, nil, NewError(
			CodePolicyDenied, "signer %q is not configured on client %q; omit from to use the client's signer", w.from, w.clientID)
	}

	to, err := s.chain.ResolveAddress(ctx, w.to)
	if err != nil {
		return authorizedWrite{}, nil, asToolError(err, CodeInvalidArgument)
	}

	if !s.skipConfirmation {
		fields := []string{
			writeAction, w.chainID, strings.ToLower(w.ift), w.from, strings.ToLower(to),
			amount.String(), w.clientID, w.timeout.String(), w.relay,
		}
		if len(req.Params.InputResponses) == 0 {
			ask, askErr := s.askConfirmation(ctx, req, w, to, amount, fields)
			return authorizedWrite{}, ask, askErr
		}
		if confirmErr := s.checkConfirmation(req, fields); confirmErr != nil {
			return authorizedWrite{}, nil, confirmErr
		}
	}

	return authorizedWrite{from: w.from, to: to, amount: amount}, nil, nil
}

// clientEnd finds the connection end for clientID on chainID; an empty clientID matches any.
func (s *Server) clientEnd(ctx context.Context, chainID, clientID string) (ClientEndInfo, bool, error) {
	conns, err := s.chain.Connections(ctx)
	if err != nil {
		return ClientEndInfo{}, false, err
	}
	for _, c := range conns {
		for _, end := range []ClientEndInfo{c.ClientA, c.ClientB} {
			if end.ChainID == chainID && (clientID == "" || end.ClientID == clientID) {
				return end, true, nil
			}
		}
	}
	return ClientEndInfo{}, false, nil
}
