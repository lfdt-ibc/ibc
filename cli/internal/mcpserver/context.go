// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ContextInput is the (empty) input of ibc_get_context.
type ContextInput struct{}

// WritableChain is a configured chain and whether this relayer has a connection on it.
type WritableChain struct {
	ChainInfo
	Writable bool `json:"writable" jsonschema:"a relayer connection includes this chain, so ibc_ift_send and ibc_relay_tx may use it"`
}

// RelayerStatus says whether the relayer answered a packet query.
type RelayerStatus struct {
	Ready bool   `json:"ready" jsonschema:"the relayer answered a packet query; packet tools work"`
	Error string `json:"error,omitempty" jsonschema:"why the packet query failed"`
}

// ContextOutput is the result of ibc_get_context.
type ContextOutput struct {
	Chains              []WritableChain  `json:"chains"               jsonschema:"chains in ibc.yml; reads work on all of them"`
	Connections         []ConnectionInfo `json:"connections"          jsonschema:"relayed connections; a send names one end as chain_id and client_id"`
	Signers             []SignerInfo     `json:"signers"              jsonschema:"signer aliases this relayer uses on its clients"`
	Tokens              []TokenEntry     `json:"tokens"               jsonschema:"IFT tokens configured for this server"`
	RequireConfirmation bool             `json:"require_confirmation" jsonschema:"ibc_ift_send asks the user to approve each transfer"`
	CanSend             bool             `json:"can_send"             jsonschema:"this client can call ibc_ift_send; false when approval needs elicitation and the client lacks it"`
	CanSendReason       string           `json:"can_send_reason,omitempty" jsonschema:"why this client cannot send; tell the user"`
	Relayer             RelayerStatus    `json:"relayer"              jsonschema:"whether packet queries work"`
}

// TokenEntry is a configured IFT token with its metadata.
type TokenEntry struct {
	ChainID  string `json:"chain_id"`
	IFT      string `json:"ift"`
	Symbol   string `json:"symbol,omitempty"`
	Decimals uint8  `json:"decimals"`
	Error    string `json:"error,omitempty" jsonschema:"why symbol and decimals are missing"`
}

func (s *Server) getContext(
	ctx context.Context,
	req *mcp.CallToolRequest,
	_ ContextInput,
) (*mcp.CallToolResult, ContextOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	out := ContextOutput{
		Chains:              []WritableChain{},
		Connections:         []ConnectionInfo{},
		Signers:             []SignerInfo{},
		RequireConfirmation: !s.skipConfirmation,
		CanSend:             s.skipConfirmation || clientElicits(req),
	}
	if !out.CanSend {
		out.CanSendReason = noElicitationMessage
	}

	chains, err := s.chain.Chains(ctx)
	if err != nil {
		return nil, out, asToolError(err, CodeConfigError)
	}
	conns, err := s.chain.Connections(ctx)
	if err != nil {
		return nil, out, asToolError(err, CodeConfigError)
	}
	out.Connections = append(out.Connections, conns...)
	relayed := map[string]struct{}{}
	used := map[string]struct{}{}
	for _, c := range conns {
		for _, end := range []ClientEndInfo{c.ClientA, c.ClientB} {
			if end.ChainID != "" {
				relayed[end.ChainID] = struct{}{}
			}
			if end.Signer != "" {
				used[end.Signer] = struct{}{}
			}
		}
	}
	for _, c := range chains {
		_, ok := relayed[c.ChainID]
		out.Chains = append(out.Chains, WritableChain{ChainInfo: c, Writable: ok})
	}

	signers, err := s.chain.Signers(ctx)
	if err != nil {
		return nil, out, asToolError(err, CodeConfigError)
	}
	for _, sg := range signers {
		if _, ok := used[sg.Alias]; ok {
			out.Signers = append(out.Signers, sg)
		}
	}

	out.Tokens = make([]TokenEntry, len(s.tokens))
	var wg sync.WaitGroup
	for i, t := range s.tokens {
		wg.Go(func() {
			// A slow chain must not hold up the rest of the context.
			ctx, cancel := context.WithTimeout(ctx, tokenReadTimeout)
			defer cancel()
			e := TokenEntry{ChainID: t.ChainID, IFT: t.IFT}
			if info, err := s.token(ctx, t.ChainID, t.IFT); err != nil {
				e.Error = asToolError(err, CodeChainRPCError).Message
			} else {
				e.Symbol, e.Decimals = info.Symbol, info.Decimals
			}
			out.Tokens[i] = e
		})
	}
	wg.Wait()

	if _, err := s.relayer.Packets(ctx, PacketFilter{}, 1, ""); err != nil {
		out.Relayer.Error = asToolError(err, CodeRelayerUnavailable).Message
	} else {
		out.Relayer.Ready = true
	}

	return nil, out, nil
}
