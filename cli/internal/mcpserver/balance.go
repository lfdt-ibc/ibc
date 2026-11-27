// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// BalanceInput is the input of ibc_ift_balance.
type BalanceInput struct {
	ChainID string `json:"chain_id" jsonschema:"chain the IFT token is deployed on"`
	IFT     string `json:"ift,omitempty" jsonschema:"IFT token address; optional when the chain has one configured token"`
	Address string `json:"address"  jsonschema:"hex address or a configured signer alias"`
}

// BalanceOutput is the result of ibc_ift_balance.
type BalanceOutput struct {
	ChainID  string `json:"chain_id"`
	IFT      string `json:"ift"`
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals uint8  `json:"decimals"`
	Balance  string `json:"balance"  jsonschema:"base units"`
	Display  string `json:"balance_display" jsonschema:"balance in whole tokens with symbol, e.g. 1.5 USDC"`
}

func (s *Server) iftBalance(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in BalanceInput,
) (*mcp.CallToolResult, BalanceOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	ift, err := s.resolveIFT(in.ChainID, in.IFT)
	if err != nil {
		return nil, BalanceOutput{}, err
	}
	in.IFT = ift
	addr, err := s.chain.ResolveAddress(ctx, in.Address)
	if err != nil {
		return nil, BalanceOutput{}, asToolError(err, CodeInvalidArgument)
	}
	info, err := s.token(ctx, in.ChainID, in.IFT)
	if err != nil {
		return nil, BalanceOutput{}, asToolError(err, CodeChainRPCError)
	}
	bal, err := s.chain.IFTBalance(ctx, in.ChainID, in.IFT, addr)
	if err != nil {
		return nil, BalanceOutput{}, asToolError(err, CodeChainRPCError)
	}
	return nil, BalanceOutput{
		ChainID: in.ChainID, IFT: in.IFT, Address: addr,
		Symbol: info.Symbol, Decimals: info.Decimals, Balance: bal.String(), Display: formatToken(bal, info),
	}, nil
}

// resolveIFT returns ift, or the chain's only configured token when ift is empty.
func (s *Server) resolveIFT(chainID, ift string) (string, error) {
	if ift != "" {
		return ift, requireIFT(ift)
	}
	var found []string
	for _, t := range s.tokens {
		if t.ChainID == chainID {
			found = append(found, t.IFT)
		}
	}
	if len(found) != 1 {
		return "", NewError(CodeInvalidArgument,
			"ift is required: chain %q has %d configured tokens; pass an ift address, for example one from ibc_get_context", chainID, len(found))
	}
	return found[0], nil
}

// token returns IFT metadata, cached after the first successful read.
func (s *Server) token(ctx context.Context, chainID, ift string) (TokenInfo, error) {
	key := chainID + "/" + strings.ToLower(ift)
	s.tokenMu.Lock()
	info, ok := s.tokenInfo[key]
	s.tokenMu.Unlock()
	if ok {
		return info, nil
	}
	info, err := s.chain.TokenInfo(ctx, chainID, ift)
	if err != nil {
		return TokenInfo{}, err
	}
	s.tokenMu.Lock()
	s.tokenInfo[key] = info
	s.tokenMu.Unlock()
	return info, nil
}

func requireIFT(addr string) error {
	if !common.IsHexAddress(addr) {
		return NewError(CodeInvalidArgument, "ift %q is not a hex address", addr)
	}
	return nil
}

// displayAmount renders amount in whole tokens of that IFT, or "" when its metadata cannot be read.
func (s *Server) displayAmount(ctx context.Context, chainID, ift string, amount *big.Int) string {
	ctx, cancel := context.WithTimeout(ctx, tokenReadTimeout)
	defer cancel()
	info, err := s.token(ctx, chainID, ift)
	if err != nil {
		return ""
	}
	return formatToken(amount, info)
}

// formatToken renders amount in whole tokens followed by the symbol.
func formatToken(amount *big.Int, info TokenInfo) string {
	return strings.TrimSpace(formatUnits(amount, info.Decimals) + " " + info.Symbol)
}

// formatUnits renders base units with decimals, trimming trailing zeros.
func formatUnits(v *big.Int, decimals uint8) string {
	if decimals == 0 {
		return v.String()
	}
	s := v.String()
	d := int(decimals)
	if len(s) <= d {
		s = strings.Repeat("0", d-len(s)+1) + s
	}
	whole, frac := s[:len(s)-d], strings.TrimRight(s[len(s)-d:], "0")
	if frac == "" {
		return whole
	}
	return whole + "." + frac
}
