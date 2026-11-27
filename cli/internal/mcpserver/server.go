// SPDX-License-Identifier: Apache-2.0

// Package mcpserver serves IBC CLI operations as MCP tools inside `ibc relayer run`,
// so an agent can send IFT transfers with the relayer's signers and follow their packets.
package mcpserver

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/metric"
)

const (
	serverName = "ibc"

	// defaultSendTimeout is the packet lifetime when packet_timeout_seconds is omitted.
	defaultSendTimeout = 15 * time.Minute
	minSendTimeout     = time.Minute
	maxSendTimeout     = 24 * time.Hour

	defaultPacketsLimit = 20
	maxPacketsLimit     = 1000
)

// Tool names.
const (
	toolContext   = "ibc_get_context"
	toolBalance   = "ibc_ift_balance"
	toolSend      = "ibc_ift_send"
	toolListSends = "ibc_list_sends"
	toolRelay     = "ibc_relay_tx"
	toolPackets   = "ibc_get_packets"
	toolWait      = "ibc_wait_for_packets"
)

// Values of the relay and until enum inputs.
const (
	relayAuto     = "auto"
	relayAlways   = "always"
	relayNever    = "never"
	untilFinal    = "final"
	untilReceived = "received"
)

const instructions = `Tools for sending IFT tokens across IBC and following the packets.
Call ibc_get_context first: it lists chains, clients, signers and IFT tokens with their decimals, and can_send says whether this client can send.
Amounts are decimal strings in the token's base unit: whole tokens times 10^decimals. *_display fields show whole tokens.
Results carry next: call next.tool with next.arguments; no tool means nothing is left to call.
Never call ibc_ift_send again for a transfer that has a tx hash. If a send call was cut off, find it with ibc_list_sends.
TX_NOT_CONFIRMED is not retryable.`

const (
	defaultConfirmTimeout = 5 * time.Minute
	// defaultWaitTimeout fits within the common 60 second client call timeout.
	defaultWaitTimeout = 45 * time.Second
	maxWaitTimeout     = 30 * time.Minute
	// maxConcurrentWaits bounds the waits that poll the relayer at once. Others
	// queue without polling.
	maxConcurrentWaits  = 16
	defaultPollInterval = 5 * time.Second
)

// Reasons of the next step after a relay request.
const (
	nextWait        = "Packets are queued for delivery."
	nextRelayFailed = "The relay request failed; retry it."
	nextNever       = "relay=never: delivery starts only after ibc_relay_tx."
	nextAutoRelay   = "The relayer picks these packets up itself."
	nextNoPackets   = "The relay found no packets yet; retry."
	nextNotSelected = "No packet was queued; retry."
	nextNotQueued   = "This relayer did not queue these packets; retry."
)

// writeDrainTimeout bounds Close's wait, from shutdown start, for writes to log their outcome.
// It is below httpShutdownTimeout so MCP shutdown stays within that one deadline.
var writeDrainTimeout = 3 * time.Second

// tokenReadTimeout bounds each token metadata read, so a slow chain cannot hold up a call. Tests shorten it.
var tokenReadTimeout = 5 * time.Second

// Options tune a Server; the zero value is usable.
type Options struct {
	Version string
	// Now overrides the clock (tests).
	Now func() time.Time
	// PollInterval overrides how often a wait polls. Zero uses the default.
	PollInterval time.Duration
	// Audit records MCP writes. Nil discards them; tests do that. `ibc relayer run`
	// opens the file before the server starts.
	Audit *AuditLog
	// Meter counts tool calls on the relayer's meter; nil skips metrics.
	// This server does not listen for scrapes.
	Meter metric.Meter
	// Tokens are the IFT tokens ibc_get_context lists.
	Tokens []TokenRef
}

// Server holds the MCP server and the state behind its tools.
type Server struct {
	chain          ChainBackend
	relayer        RelayerBackend
	confirm        *confirmations
	now            func() time.Time
	mcp            *mcp.Server
	confirmTimeout time.Duration
	waitDefault    time.Duration
	waitMax        time.Duration
	poll           time.Duration
	// waitSlots holds one token per wait that is polling.
	waitSlots chan struct{}
	// skipConfirmation skips the approval prompt. Unexported so only tests here
	// can set it; the bearer token, not the prompt, keeps other callers out.
	skipConfirmation bool
	audit            auditSink
	toolCalls        metric.Int64Counter
	knownTools       map[string]struct{}
	// replaced groups the hashes of sends the relayer re-signed.
	replaced replacements
	sends    sendLog
	tokens   []TokenRef
	// tokenInfo caches token metadata, which does not change, by chain and lower-case address.
	tokenMu   sync.Mutex
	tokenInfo map[string]TokenInfo
	// writes and inFlight count audited writes without an outcome line yet; Close
	// waits for them. writesMu guards stopping, inFlight and writes.Add.
	writesMu    sync.Mutex
	stopping    bool
	stopStarted time.Time
	writes      sync.WaitGroup
	inFlight    int
	// shutdown cancels in-flight tool calls when the server stops.
	shutdown context.Context
	stop     context.CancelFunc
}

// New builds a Server and registers its tools. Sends use the signer configured on the relayer's client.
func New(chain ChainBackend, rel RelayerBackend, opts Options) (*Server, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	poll := opts.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	toolCalls, err := newToolCallCounter(opts.Meter)
	if err != nil {
		return nil, fmt.Errorf("register mcp_tool_calls_total: %w", err)
	}
	shutdown, cancel := context.WithCancel(context.Background())
	var audit auditSink
	if opts.Audit != nil {
		audit = opts.Audit
	}
	s := &Server{
		chain: chain, relayer: rel, confirm: newConfirmations(now), now: now,
		confirmTimeout: defaultConfirmTimeout, waitDefault: defaultWaitTimeout,
		waitMax: maxWaitTimeout, poll: poll, waitSlots: make(chan struct{}, maxConcurrentWaits),
		audit: audit, toolCalls: toolCalls, knownTools: map[string]struct{}{},
		tokens: opts.Tokens, tokenInfo: map[string]TokenInfo{},
		shutdown: shutdown,
	}
	// stop is called by HTTPServer.Stop and Close. The first call starts the
	// write drain's clock, so Close's wait counts from the start of shutdown.
	s.stop = func() {
		s.writesMu.Lock()
		if s.stopStarted.IsZero() {
			s.stopStarted = time.Now()
		}
		s.writesMu.Unlock()
		cancel()
	}

	version := opts.Version
	if version == "" {
		version = "dev"
	}
	s.mcp = mcp.NewServer(
		&mcp.Implementation{Name: serverName, Title: "IBC CLI", Version: version},
		&mcp.ServerOptions{
			Instructions: instructions,
			// A nil logger is discarded by the SDK, so session and jsonrpc
			// errors never reach stderr. This is the relayer logger.
			Logger: Logger(),
		},
	)
	s.register()
	// Outside the SDK's multi round-trip middleware, which prompts older clients
	// with the request context, so bindRequest also ends that wait on stop.
	s.mcp.AddReceivingMiddleware(s.observeToolCall, s.recoverPanic, s.bindRequest)
	return s, nil
}

// recoverPanic turns a handler panic into a call error: the SDK does not recover,
// and this server shares the relayer's process.
func (s *Server) recoverPanic(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (res mcp.Result, err error) {
		defer func() {
			if v := recover(); v != nil {
				Logger().Error("MCP handler panic",
					"method", method, "panic", v, "stack", string(debug.Stack()))
				res, err = nil, fmt.Errorf("internal error handling %s", method)
			}
		}()
		return next(ctx, method, req)
	}
}

// bindRequest ends a request's context on stop, including the SDK's elicitation wait outside handlers.
func (s *Server) bindRequest(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		ctx, cancel := s.bind(ctx)
		defer cancel()
		return next(ctx, method, req)
	}
}

// bind returns a context that ends when ctx ends or the server stops.
func (s *Server) bind(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.shutdown, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// MCP returns the underlying server, to run on a transport.
func (s *Server) MCP() *mcp.Server { return s.mcp }

// Close cancels in-flight calls, waits up to writeDrainTimeout from shutdown start
// for sends and relays to log their outcome, then closes the audit log.
func (s *Server) Close() error {
	s.stop()
	s.writesMu.Lock()
	s.stopping = true
	deadline := s.stopStarted.Add(writeDrainTimeout)
	// No write can begin once stopping is set. Skip the wait when none runs:
	// an expired timer could otherwise win select over a drained channel.
	running := s.inFlight > 0
	s.writesMu.Unlock()
	if running {
		drained := make(chan struct{})
		go func() {
			s.writes.Wait()
			close(drained)
		}()
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-drained:
		case <-timer.C:
			Logger().Warn(
				"MCP write still running at shutdown; its outcome line may be missing from the audit log")
		}
	}
	if s.audit == nil {
		return nil
	}
	return s.audit.Close()
}

// beginWrite registers a send or relay before its audit intent line; it refuses once Close has started.
func (s *Server) beginWrite() bool {
	s.writesMu.Lock()
	defer s.writesMu.Unlock()
	if s.stopping {
		return false
	}
	s.inFlight++
	s.writes.Add(1)
	return true
}

// endWrite finishes a write registered by beginWrite.
func (s *Server) endWrite() {
	s.writesMu.Lock()
	s.inFlight--
	s.writesMu.Unlock()
	s.writes.Done()
}

func (s *Server) register() {
	f, t := false, true

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        toolContext,
		Description: "This relayer's chains, connections (clients and signers), IFT tokens with decimals, and relayer status. Call first.",
		Annotations: &mcp.ToolAnnotations{Title: "Get IBC context", ReadOnlyHint: true, OpenWorldHint: &t},
	}), s.getContext)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        toolBalance,
		Description: "IFT balance of a hex address or signer alias.",
		Annotations: &mcp.ToolAnnotations{Title: "IFT balance", ReadOnlyHint: true, OpenWorldHint: &t},
	}), s.iftBalance)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name: toolSend,
		Description: "Send IFT tokens over IBC to the counterparty chain of client_id with that client's signer. " +
			"Blocks until the user approves (a client without elicitation gets POLICY_DENIED), then returns once the tx is broadcast; " +
			"mining and the relay request continue in the background. Follow next.",
		Annotations: &mcp.ToolAnnotations{Title: "Send IFT", DestructiveHint: &t, OpenWorldHint: &t},
		InputSchema: mustSchema[SendInput](func(s *jsonschema.Schema) {
			s.Properties["relay"].Enum = []any{relayAuto, relayAlways, relayNever}
			s.Properties["packet_timeout_seconds"].Minimum = ptr(minSendTimeout.Seconds())
			s.Properties["packet_timeout_seconds"].Maximum = ptr(maxSendTimeout.Seconds())
		}),
	}), s.iftSend)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        toolListSends,
		Description: "This run's sends, newest first, with tx, status and next step. Finds a send whose call was cut off.",
		Annotations: &mcp.ToolAnnotations{Title: "List sends", ReadOnlyHint: true, OpenWorldHint: &f},
		InputSchema: mustSchema[ListSendsInput](func(s *jsonschema.Schema) {
			s.Properties["limit"].Minimum = ptr(1.0)
			s.Properties["limit"].Maximum = ptr(float64(maxSends))
		}),
	}), s.listSends)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name: toolRelay,
		Description: "Ask the relayer to deliver the packets of a source tx on a chain it relays. Safe to repeat. " +
			"Use when next names it, or for a tx sent outside this server. Follow next.",
		Annotations: &mcp.ToolAnnotations{
			Title: "Relay tx", DestructiveHint: &f, IdempotentHint: true, OpenWorldHint: &f,
		},
	}), s.relayTx)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        toolPackets,
		Description: "Packets the relayer knows, newest first, with state and send/recv/ack/timeout txs. Filters combine with AND.",
		Annotations: &mcp.ToolAnnotations{Title: "Get packets", ReadOnlyHint: true, OpenWorldHint: &f},
		InputSchema: mustSchema[PacketsInput](func(s *jsonschema.Schema) {
			s.Properties["state"].Enum = statesAsAny()
			s.Properties["limit"].Minimum = ptr(1.0)
			s.Properties["limit"].Maximum = ptr(float64(maxPacketsLimit))
		}),
	}), s.getPackets)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name: toolWait,
		Description: "Wait until every packet of a source tx is final (ack, timeout, rejection or relay failure), " +
			"or received with until=received, with progress notifications. " +
			"Returns early when another call is needed, and with done=false after timeout_seconds (default 45); neither is an error. Follow next.",
		Annotations: &mcp.ToolAnnotations{Title: "Wait for packets", ReadOnlyHint: true, OpenWorldHint: &f},
		InputSchema: mustSchema[WaitInput](func(sc *jsonschema.Schema) {
			sc.Properties["until"].Enum = []any{untilFinal, untilReceived}
			sc.Properties["timeout_seconds"].Minimum = ptr(1.0)
			sc.Properties["timeout_seconds"].Maximum = ptr(s.waitMax.Seconds())
		}),
	}), s.waitForPackets)

	s.mcp.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "packets",
		Title:       "Packets of a source tx",
		Description: "Relay status of every packet emitted by a source transaction, as JSON.",
		URITemplate: "ibc://packets/{chain_id}/{tx_hash}",
		MIMEType:    "application/json",
	}, s.readPacketsResource)
}

func mustSchema[T any](patch func(*jsonschema.Schema)) *jsonschema.Schema {
	sc, err := jsonschema.For[T](nil)
	if err != nil {
		panic(err)
	}
	patch(sc)
	return sc
}

func statesAsAny() []any {
	out := make([]any, 0, len(PacketStates))
	for _, st := range PacketStates {
		out = append(out, string(st))
	}
	return out
}

func ptr[T any](v T) *T { return &v }
