// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var txHashPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

// RelayInput is the input of ibc_relay_tx.
type RelayInput struct {
	ChainID string           `json:"chain_id"          jsonschema:"source chain of the tx"`
	TxHash  string           `json:"tx_hash"           jsonschema:"source tx that emitted the packets"`
	Packets []PacketSelector `json:"packets,omitempty" jsonschema:"packets to relay; omit for all packets in the tx"`
}

// RelayOutput is the result of ibc_relay_tx.
type RelayOutput struct {
	Packets  []ObservedPacket `json:"packets"            jsonschema:"send packets of the tx and whether this relayer delivers them"`
	Warnings []string         `json:"warnings,omitempty"`
	Next     NextStep         `json:"next"`
}

// PacketsInput is the input of ibc_get_packets.
type PacketsInput struct {
	SourceChainID       string `json:"source_chain_id,omitempty"       jsonschema:"chain the packets were sent from"`
	DestinationChainID  string `json:"destination_chain_id,omitempty"  jsonschema:"chain the packets are delivered to"`
	SourceClientID      string `json:"source_client_id,omitempty"      jsonschema:"client on the source chain"`
	DestinationClientID string `json:"destination_client_id,omitempty" jsonschema:"client on the destination chain"`
	SourceTxHash        string `json:"source_tx_hash,omitempty"        jsonschema:"usually what you filter on"`
	Sequence            string `json:"sequence,omitempty"              jsonschema:"packet sequence; pair it with source_client_id"`
	State               string `json:"state,omitempty"                 jsonschema:"packet state"`
	Limit               int    `json:"limit,omitempty"                 jsonschema:"page size; default 20"`
	Cursor              string `json:"cursor,omitempty"                jsonschema:"next_cursor from the previous page"`
}

// at fills the chain_id and tx_hash arguments of a step that names a tool.
func (n NextStep) at(tx *TxRef) NextStep {
	if n.Tool == "" || tx == nil {
		return n
	}
	n.Arguments = map[string]string{"chain_id": tx.ChainID, "tx_hash": tx.TxHash}
	return n
}

// unconfiguredNext says this relayer will not deliver packets ids.
func unconfiguredNext(ids []string) string {
	if len(ids) == 1 {
		return fmt.Sprintf("This relayer has no route for packet %s and will not deliver it.", ids[0])
	}
	return fmt.Sprintf("This relayer has no route for packets %s and will not deliver them.", strings.Join(ids, ", "))
}

func (s *Server) relayTx(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in RelayInput,
) (*mcp.CallToolResult, RelayOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	if err := requireTxHash(in.TxHash); err != nil {
		return s.rejectRelay(in, err)
	}
	_, ok, err := s.clientEnd(ctx, in.ChainID, "")
	if err != nil {
		return s.rejectRelay(in, asToolError(err, CodeConfigError))
	}
	if !ok {
		return s.rejectRelay(in, NewError(CodePolicyDenied, "chain %q has no connection on this relayer; use a chain that ibc_get_context lists as writable", in.ChainID))
	}

	if !s.beginWrite() {
		return s.rejectRelay(in, NewError(CodeRelayerUnavailable, "the MCP server is stopping"))
	}
	defer s.endWrite()
	rec, err := s.beginAudit(auditRecord{
		Tool: "ibc_relay_tx", ChainID: in.ChainID, TxHash: in.TxHash,
	})
	if err != nil {
		return nil, RelayOutput{}, err
	}
	packets, used, err := s.relayFamily(ctx, in)
	if err != nil {
		_ = s.finish(rec, asToolError(err, CodeRelayerError))
		return nil, RelayOutput{}, asToolError(err, CodeRelayerError)
	}
	if packets == nil {
		packets = []ObservedPacket{}
	}
	out := RelayOutput{Packets: packets, Next: relayNext(packets).at(&TxRef{ChainID: in.ChainID, TxHash: used})}
	if len(packets) > 0 {
		// Packets exist only once the source tx mined.
		s.noteMinedLate(s.replaced.family(in.TxHash), TxRef{ChainID: in.ChainID, TxHash: used})
	}
	s.sends.relayed(s.replaced.family(in.TxHash), out.Next)
	if note := replacedNote(in.TxHash, used); note != "" {
		out.Warnings = append(out.Warnings, note)
	}
	rec.Outcome = outcomeRelayed
	rec.TxHash = used
	if err := s.record(rec); err != nil {
		out.Warnings = append(out.Warnings, "audit log update failed: "+err.Error())
	}
	return nil, out, nil
}

// relayFamily relays in.TxHash, or the mined hash of the same transfer if the relayer
// re-signed it, and returns the hash relayed.
func (s *Server) relayFamily(ctx context.Context, in RelayInput) ([]ObservedPacket, string, error) {
	hashes := s.replaced.family(in.TxHash)
	if len(hashes) == 1 {
		packets, err := s.relayer.Relay(ctx, in.ChainID, in.TxHash, in.Packets)
		return packets, in.TxHash, err
	}
	var askedPackets []ObservedPacket
	var askedErr error
	for _, h := range hashes {
		packets, err := s.relayer.Relay(ctx, in.ChainID, h, in.Packets)
		if err == nil && len(packets) > 0 {
			return packets, h, nil
		}
		if strings.EqualFold(h, in.TxHash) {
			askedPackets, askedErr = packets, err
		}
		if ctx.Err() != nil {
			return nil, in.TxHash, ctx.Err()
		}
	}
	return askedPackets, in.TxHash, askedErr
}

// relayNext is the next step after a relay that returned.
func relayNext(packets []ObservedPacket) NextStep {
	return sendNext(relayAlways, true, false, false, packets)
}

// rejectRelay audits a relay refused before the relayer was asked.
func (s *Server) rejectRelay(in RelayInput, err error) (*mcp.CallToolResult, RelayOutput, error) {
	s.note(auditRecord{Tool: "ibc_relay_tx", ChainID: in.ChainID, TxHash: in.TxHash}, err)
	return nil, RelayOutput{}, err
}

func (s *Server) getPackets(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in PacketsInput,
) (*mcp.CallToolResult, PacketsPage, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	filter := PacketFilter{
		SourceChainID: in.SourceChainID, DestinationChainID: in.DestinationChainID,
		SourceClientID: in.SourceClientID, DestinationClientID: in.DestinationClientID,
		SourceTxHash: in.SourceTxHash, State: PacketState(in.State),
	}
	if in.Sequence != "" {
		seq, ok := parseAmount(in.Sequence)
		if !ok || !seq.IsUint64() {
			return nil, PacketsPage{}, NewError(CodeInvalidArgument, "invalid sequence %q", in.Sequence)
		}
		v := seq.Uint64()
		filter.Sequence = &v
	}
	limit := in.Limit
	if limit == 0 {
		limit = defaultPacketsLimit
	}
	if limit < 1 || limit > maxPacketsLimit {
		return nil, PacketsPage{}, NewError(CodeInvalidArgument, "limit must be between 1 and %d", maxPacketsLimit)
	}

	page, err := s.relayer.Packets(ctx, filter, uint32(limit), in.Cursor)
	if err != nil {
		return nil, PacketsPage{}, asToolError(err, CodeRelayerError)
	}
	// A send the relayer re-signed: its packets are on whichever hash was mined.
	if len(page.Packets) == 0 && in.SourceTxHash != "" && in.Cursor == "" {
		for _, h := range s.replaced.family(in.SourceTxHash) {
			if strings.EqualFold(h, in.SourceTxHash) {
				continue
			}
			filter.SourceTxHash = h
			other, err := s.relayer.Packets(ctx, filter, uint32(limit), "")
			if err != nil {
				return nil, PacketsPage{}, asToolError(err, CodeRelayerError)
			}
			if len(other.Packets) > 0 {
				page = other
				break
			}
		}
	}
	if page.Packets == nil {
		page.Packets = []PacketStatus{}
	}
	return nil, page, nil
}

func (s *Server) readPacketsResource(
	ctx context.Context,
	req *mcp.ReadResourceRequest,
) (*mcp.ReadResourceResult, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	uri := req.Params.URI
	rest, ok := strings.CutPrefix(uri, "ibc://packets/")
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 2 || parts[0] == "" || !txHashPattern.MatchString(parts[1]) {
		return nil, mcp.ResourceNotFoundError(uri)
	}
	packets, _, err := s.txPackets(ctx, parts[0], parts[1])
	if err != nil {
		return nil, err
	}
	bz, err := json.Marshal(map[string]any{"packets": packets})
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(bz)}},
	}, nil
}

func requireTxHash(h string) error {
	if !txHashPattern.MatchString(h) {
		return NewError(CodeInvalidArgument, "tx_hash %q must be 0x followed by 64 hex characters", h)
	}
	return nil
}
