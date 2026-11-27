// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuditLogAppendAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	require.NoError(t, log.write(auditRecord{ID: "1", Tool: "ibc_ift_send", Outcome: outcomeIntent}))
	require.NoError(t, log.write(auditRecord{ID: "1", Tool: "ibc_ift_send", Outcome: outcomeMined, TxHash: txHash}))
	require.NoError(t, log.Close())
	require.NoError(t, log.Close())
	require.ErrorContains(t, log.write(auditRecord{ID: "2", Outcome: outcomeIntent}), "closed")

	recs := readAudit(t, path)
	require.Len(t, recs, 2)
	require.Equal(t, "1", recs[0].ID)
	require.Equal(t, outcomeIntent, recs[0].Outcome)
	require.Equal(t, outcomeMined, recs[1].Outcome)
	require.Equal(t, txHash, recs[1].TxHash)
}

func TestAuditCloseDuringWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	log, err := OpenAuditLog(path)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = log.write(auditRecord{ID: "n", Tool: "ibc_ift_send", Outcome: outcomeIntent})
		}()
	}
	wg.Wait()
	require.NoError(t, log.Close())

	for _, rec := range readAudit(t, path) {
		require.Equal(t, outcomeIntent, rec.Outcome)
		require.Equal(t, "ibc_ift_send", rec.Tool)
	}
}

func readAudit(t *testing.T, path string) []auditRecord {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var out []auditRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec auditRecord
		require.NoError(t, json.Unmarshal(sc.Bytes(), &rec))
		out = append(out, rec)
	}
	require.NoError(t, sc.Err())
	return out
}

func TestOpenAuditLogRestrictsExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), AuditFileName)
	require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o600))
	require.NoError(t, os.Chmod(path, 0o644))

	log, err := OpenAuditLog(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{}\n", string(data), "existing lines are kept")
}
