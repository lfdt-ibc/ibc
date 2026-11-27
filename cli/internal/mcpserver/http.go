// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	pkgerrors "github.com/pkg/errors"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpSessionTimeout    = 30 * time.Minute
)

// httpShutdownTimeout bounds Stop's graceful wait; httpSessionCloseTimeout is the
// share for closing sessions, leaving the relayer most of it. Tests shorten both.
var (
	httpShutdownTimeout     = 5 * time.Second
	httpSessionCloseTimeout = time.Second
)

// HTTPServer serves MCP streamable HTTP on its own listener, since the relayer
// server's short read timeouts would cut long packet waits.
type HTTPServer struct {
	server   *http.Server
	listener net.Listener
	stop     context.CancelFunc
	done     chan struct{}
	// closeSessions ends every MCP session, which ends its open event stream.
	// Shutdown waits for those streams and does not close them.
	closeSessions func()
}

// ListenHTTP binds addr synchronously, so a taken port fails startup, then serves
// in the background. It can sign transactions, so it needs loopback and a bearer token.
func (s *Server) ListenHTTP(addr, token string) (*HTTPServer, error) {
	if len(token) < minTokenLength {
		return nil, pkgerrors.Errorf("MCP bearer token must be at least %d characters", minTokenLength)
	}
	if err := RequireLoopback(addr); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "MCP listen on %s", addr)
	}
	if err := requireBoundLoopback(listener.Addr()); err != nil {
		_ = listener.Close()
		return nil, err
	}

	logger := Logger()
	handler := requireBearer(token, mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return s.mcp },
		&mcp.StreamableHTTPOptions{Logger: logger, SessionTimeout: httpSessionTimeout},
	))
	h := startHTTP(listener, handler, s.stop, logger)
	h.closeSessions = func() {
		for ss := range s.mcp.Sessions() {
			_ = ss.Close()
		}
	}
	return h, nil
}

// startHTTP serves handler on listener in the background.
func startHTTP(listener net.Listener, handler http.Handler, stop context.CancelFunc, logger *slog.Logger) *HTTPServer {
	h := &HTTPServer{
		server: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: httpReadHeaderTimeout,
			ErrorLog:          processErrorLog(logger),
		},
		listener: listener,
		stop:     stop,
		done:     make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		if err := h.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("MCP server stopped", "err", err)
		}
	}()
	return h
}

// Addr is the bound address, useful when listening on port 0.
func (h *HTTPServer) Addr() net.Addr { return h.listener.Addr() }

// URL is the endpoint MCP clients connect to.
func (h *HTTPServer) URL() string { return "http://" + h.Addr().String() }

// Stop cancels tool calls, ends sessions and shuts down, force-closing
// connections still open after httpShutdownTimeout.
func (h *HTTPServer) Stop() error {
	h.stop()
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	if h.closeSessions != nil {
		// Bounded because a session close waits for outstanding calls, and an
		// unanswered approval prompt stays outstanding even after cancellation.
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			h.closeSessions()
		}()
		sessions, sessionsCancel := context.WithTimeout(ctx, httpSessionCloseTimeout)
		select {
		case <-closed:
		case <-sessions.Done():
		}
		sessionsCancel()
	}
	err := h.server.Shutdown(ctx)
	if err != nil {
		// Shutdown leaves connections open past its deadline (e.g. a handler that
		// ignored cancellation); Close drops them.
		_ = h.server.Close()
	}
	select {
	case <-h.done:
	case <-time.After(httpShutdownTimeout):
	}
	return err
}

// RequireLoopback refuses any address that is not host:port on a loopback host.
func RequireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return pkgerrors.Wrapf(err, "invalid MCP listen address %q", addr)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return pkgerrors.Errorf(
			"MCP listen address %q is not loopback; only local clients may connect", addr)
	}
	return nil
}

// requireBoundLoopback checks the bound address, so localhost must resolve to loopback.
func requireBoundLoopback(addr net.Addr) error {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp == nil || tcp.IP == nil || !tcp.IP.IsLoopback() {
		return pkgerrors.Errorf(
			"MCP listen address %q is not loopback; only local clients may connect", addr)
	}
	return nil
}
