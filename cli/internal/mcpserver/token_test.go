// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

const testToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// bearerClient sends "Authorization: Bearer <token>" on every request.
func bearerClient(token string) *http.Client {
	return &http.Client{Transport: bearerTransport{token: token, next: http.DefaultTransport}}
}

type bearerTransport struct {
	token string
	next  http.RoundTripper
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

func TestListenHTTPRequiresBearerToken(t *testing.T) {
	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	h, err := srv.ListenHTTP("127.0.0.1:0", testToken)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Stop() })

	for name, client := range map[string]*http.Client{
		"no token":    http.DefaultClient,
		"wrong token": bearerClient(strings.Repeat("x", len(testToken))),
	} {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, h.URL(), strings.NewReader(
				`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			resp, err := client.Do(req)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

			_, err = mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(
				context.Background(),
				&mcp.StreamableClientTransport{HTTPClient: client, Endpoint: h.URL(), MaxRetries: -1}, nil)
			require.Error(t, err)
		})
	}

	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(
		context.Background(),
		&mcp.StreamableClientTransport{HTTPClient: bearerClient(testToken), Endpoint: h.URL()}, nil)
	require.NoError(t, err)
	require.NoError(t, cs.Close())
}

func TestListenHTTPRejectsShortToken(t *testing.T) {
	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	_, err = srv.ListenHTTP("127.0.0.1:0", "")
	require.ErrorContains(t, err, "bearer token")
	_, err = srv.ListenHTTP("127.0.0.1:0", "short")
	require.ErrorContains(t, err, "bearer token")
}

func TestLoadOrCreateToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), TokenFileName)
	token, err := LoadOrCreateToken(path)
	require.NoError(t, err)
	require.Len(t, token, 64)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	again, err := LoadOrCreateToken(path)
	require.NoError(t, err)
	require.Equal(t, token, again, "a restart keeps the token clients were given")

	require.NoError(t, os.WriteFile(path, []byte("  "+testToken+"\n"), 0o600))
	configured, err := LoadOrCreateToken(path)
	require.NoError(t, err)
	require.Equal(t, testToken, configured)

	require.NoError(t, os.WriteFile(path, []byte("guessable\n"), 0o600))
	_, err = LoadOrCreateToken(path)
	require.ErrorContains(t, err, "fewer than")

	require.NoError(t, os.WriteFile(path, []byte("correct horse battery staple and more words\n"), 0o600))
	_, err = LoadOrCreateToken(path)
	require.ErrorContains(t, err, "whitespace")

	// An operator-written file readable by others is narrowed before use.
	require.NoError(t, os.WriteFile(path, []byte(testToken+"\n"), 0o600))
	require.NoError(t, os.Chmod(path, 0o644))
	_, err = LoadOrCreateToken(path)
	require.NoError(t, err)
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	// A path that names a directory is refused, and the directory is left alone.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err = LoadOrCreateToken(dir)
	require.ErrorContains(t, err, "not a regular file")
	info, err = os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())

	_, err = LoadOrCreateToken(filepath.Join(t.TempDir(), "missing-dir", TokenFileName))
	require.ErrorContains(t, err, "create MCP token file")
}
