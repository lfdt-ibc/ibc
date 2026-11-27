// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// replacements groups hashes the relayer signed for one transfer at one nonce,
// so the agent's hash may never mine; only one does, not always the newest.
type replacements struct {
	mu     sync.Mutex
	groups map[string]*[]string // lower-case hash → its group, oldest first
}

// add records that replacement replaced previous. Pairs may arrive in any order;
// appending replacement's run to previous's keeps the chain in signing order.
func (r *replacements) add(previous, replacement string) {
	previous, replacement = strings.ToLower(previous), strings.ToLower(replacement)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.groups == nil {
		r.groups = map[string]*[]string{}
	}
	head, ok := r.groups[previous]
	if !ok {
		head = &[]string{previous}
		r.groups[previous] = head
	}
	tail, ok := r.groups[replacement]
	if !ok {
		tail = &[]string{replacement}
	}
	if head == tail {
		return
	}
	*head = append(*head, *tail...)
	for _, h := range *tail {
		r.groups[h] = head
	}
}

// family returns every hash signed for hash's transfer, newest first.
func (r *replacements) family(hash string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[strings.ToLower(hash)]
	if !ok {
		return []string{hash}
	}
	out := make([]string, 0, len(*g))
	for i := len(*g) - 1; i >= 0; i-- {
		out = append(out, (*g)[i])
	}
	return out
}

// noteReplaced records a relayer re-sign so tx-hash lookups, the audit log and
// approval replays all see the new hash.
func (s *Server) noteReplaced(rec auditRecord, req *mcp.CallToolRequest, chainID, previous, replacement string) {
	s.replaced.add(previous, replacement)
	s.noteBroadcast(req, TxRef{ChainID: chainID, TxHash: replacement})
	_ = s.record(auditRecord{
		ID: rec.ID, Tool: rec.Tool, ChainID: chainID, Outcome: outcomeReplaced, TxHash: replacement,
		Message: "re-signed at the same nonce with higher fees; replaces " + previous +
			"; only one of them can be mined",
	})
}

// replacedNote warns when a lookup used a different hash than the caller passed.
func replacedNote(asked, used string) string {
	if strings.EqualFold(asked, used) {
		return ""
	}
	return "The relayer re-signed this transfer at the same nonce; " + used +
		" is the transaction that carries its packets, not " + asked + "."
}
