// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// AuditFileName is the audit log created in the IBC home when mcp.auditLog is empty.
const AuditFileName = "mcp-audit.jsonl"

const (
	outcomeIntent       = "intent"
	outcomeMined        = "mined"
	outcomeNotConfirmed = "not_confirmed"
	outcomeReverted     = "reverted"
	outcomeFailed       = "failed"
	outcomeDenied       = "denied"
	outcomeDeclined     = "declined"
	outcomeRelayed      = "relayed"
	// outcomeReplaced is a send the relayer re-signed at the same nonce.
	outcomeReplaced = "replaced"
)

// auditRecord is one JSON line. An intent line is synced before a send or a
// relay is submitted. A later line with the same id records how it ended.
type auditRecord struct {
	Time     time.Time `json:"time"`
	ID       string    `json:"id"`
	Tool     string    `json:"tool"`
	Outcome  string    `json:"outcome"`
	ChainID  string    `json:"chain_id,omitempty"`
	IFT      string    `json:"ift,omitempty"`
	From     string    `json:"from,omitempty"`
	To       string    `json:"to,omitempty"`
	Amount   string    `json:"amount,omitempty"`
	ClientID string    `json:"client_id,omitempty"`
	Relay    string    `json:"relay,omitempty"`
	TxHash   string    `json:"tx_hash,omitempty"`
	Code     string    `json:"code,omitempty"`
	Message  string    `json:"message,omitempty"`
}

// auditSink appends audit records. A nil sink discards them.
type auditSink interface {
	write(auditRecord) error
	Close() error
}

// AuditLog is an append-only JSONL file of MCP writes. Close and write share
// one lock, so a stop cannot close the file under a line that is still syncing.
type AuditLog struct {
	mu   sync.Mutex
	file *os.File
}

// OpenAuditLog creates or appends to path, tightening an existing file to 0600.
func OpenAuditLog(path string) (*AuditLog, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log %s: %w", path, err)
	}
	if err := restrictToOwner(f, path); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &AuditLog{file: f}, nil
}

// restrictToOwner sets f to 0600 if group or others have access (the create mode
// only covers new files). Failing to is an error rather than a file left readable.
func restrictToOwner(f *os.File, path string) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	// Checked before any chmod: a path that names a directory, such as the
	// IBC home, must not lose its execute bits.
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 == 0 {
		return nil
	}
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("%s is accessible to group or others (mode %04o) and could not be set to 0600: %w",
			path, info.Mode().Perm(), err)
	}
	return nil
}

func (a *AuditLog) write(rec auditRecord) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return fmt.Errorf("audit log closed")
	}
	bz, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := a.file.Write(append(bz, '\n')); err != nil {
		return err
	}
	return a.file.Sync()
}

// Close closes the file; a second call is a no-op.
func (a *AuditLog) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

func newAuditID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// beginAudit syncs an intent line. On error the write must not be submitted,
// or a crash would leave no record of it.
func (s *Server) beginAudit(rec auditRecord) (auditRecord, error) {
	id, err := newAuditID()
	if err != nil {
		return auditRecord{}, NewError(CodeConfigError, "audit log: %s", err.Error())
	}
	rec.ID = id
	rec.Outcome = outcomeIntent
	if err := s.record(rec); err != nil {
		return auditRecord{}, NewError(CodeConfigError, "audit log: %s", err.Error())
	}
	return rec, nil
}

func (s *Server) record(rec auditRecord) error {
	if s.audit == nil {
		return nil
	}
	if rec.Time.IsZero() {
		rec.Time = s.now().UTC()
	}
	// The file is the durable record. The process log is what a container
	// operator sees: slog writes stderr, and the image does not redirect it.
	logAudit(rec)
	if err := s.audit.write(rec); err != nil {
		Logger().Error("MCP audit log write failed",
			"err", err, "tool", rec.Tool, "outcome", rec.Outcome)
		return err
	}
	return nil
}

// logAudit logs rec to the process log, omitting message, addresses and token,
// since a message can echo an RPC URL or approval token.
func logAudit(rec auditRecord) {
	attrs := []any{
		"tool", rec.Tool,
		"outcome", rec.Outcome,
		"chain", rec.ChainID,
		"code", rec.Code,
	}
	if rec.Amount != "" {
		attrs = append(attrs, "amount", rec.Amount)
	}
	if rec.ClientID != "" {
		attrs = append(attrs, "client", rec.ClientID)
	}
	if rec.TxHash != "" {
		attrs = append(attrs, "tx", rec.TxHash)
	}
	Logger().Info("MCP audit", attrs...)
}

// note records a call that submitted nothing, ignoring a log failure.
func (s *Server) note(rec auditRecord, err error) {
	id, idErr := newAuditID()
	if idErr != nil {
		return
	}
	rec.ID = id
	_ = s.finish(rec, err)
}

// finish records a failed call, taking outcome, code, message and tx hash from err.
func (s *Server) finish(rec auditRecord, err error) error {
	te := asToolError(err, CodeChainRPCError)
	rec.Code = string(te.Code)
	rec.Message = te.Message
	if te.Tx != nil {
		rec.TxHash = te.Tx.TxHash
	}
	switch te.Code {
	case CodeTxNotConfirmed:
		rec.Outcome = outcomeNotConfirmed
	case CodeTxReverted:
		rec.Outcome = outcomeReverted
	case CodeUserDeclined:
		rec.Outcome = outcomeDeclined
	case CodePolicyDenied:
		rec.Outcome = outcomeDenied
	default:
		rec.Outcome = outcomeFailed
	}
	return s.record(rec)
}
