// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/lfdt-ibc/ibc/cli/api/v2/relayer"
	"github.com/lfdt-ibc/ibc/cli/internal/config"
	"github.com/lfdt-ibc/ibc/cli/internal/mcpserver"
	"github.com/lfdt-ibc/ibc/cli/internal/service/signer"
	"github.com/lfdt-ibc/ibc/cli/internal/txsubmitter"
	evmsub "github.com/lfdt-ibc/ibc/cli/internal/txsubmitter/evm"
)

func TestStartRelayerMCP(t *testing.T) {
	previous := globalFlags
	t.Cleanup(func() {
		globalFlags = previous
		flagRelayerMCPListen = ""
	})
	globalFlags = config.DefaultFlagSet()
	globalFlags.Home = t.TempDir()
	api := relayer.UnimplementedRelayerApiServiceHandler{}

	// A failed assertion between start and stop must still put os.Stdout back.
	var stop func() error
	t.Cleanup(func() {
		if stop != nil {
			_ = stop()
		}
	})

	// neither the flag nor mcp.listenAddr: the relayer runs without MCP
	var err error
	stop, err = startRelayerMCP(config.Config{}, api, nil)
	require.NoError(t, err)
	require.Nil(t, stop)

	stop, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{}}, api, nil)
	require.NoError(t, err)
	require.Nil(t, stop)

	stop, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{ListenAddr: "127.0.0.1:0"}}, api, nil)
	require.NoError(t, err)
	require.NotNil(t, stop)
	require.NoError(t, stop())
	_, err = os.Stat(filepath.Join(globalFlags.Home, mcpserver.AuditFileName))
	require.NoError(t, err, "MCP writes go to mcp-audit.jsonl in the IBC home")

	custom := filepath.Join(t.TempDir(), "custom.jsonl")
	stop, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{
		ListenAddr: "127.0.0.1:0",
		AuditLog:   custom,
	}}, api, nil)
	require.NoError(t, err)
	require.NoError(t, stop())
	_, err = os.Stat(custom)
	require.NoError(t, err)

	_, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{
		ListenAddr: "127.0.0.1:0",
		AuditLog:   filepath.Join(t.TempDir(), "missing", "audit.jsonl"),
	}}, api, nil)
	require.ErrorContains(t, err, "open audit log")

	_, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{ListenAddr: "0.0.0.0:3001"}}, api, nil)
	require.ErrorContains(t, err, "not loopback")

	// the flag overrides mcp.listenAddr for this run
	flagRelayerMCPListen = "127.0.0.1:0"
	stop, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{ListenAddr: "0.0.0.0:3001"}}, api, nil)
	require.NoError(t, err)
	require.NotNil(t, stop)
	require.NoError(t, stop())

	flagRelayerMCPListen = "0.0.0.0:0"
	_, err = startRelayerMCP(config.Config{MCP: config.MCPConfig{ListenAddr: "127.0.0.1:3001"}}, api, nil)
	require.ErrorContains(t, err, "not loopback")
}

func TestStartRelayerMCPLogsLifecycle(t *testing.T) {
	previous := globalFlags
	t.Cleanup(func() {
		globalFlags = previous
		flagRelayerMCPListen = ""
	})
	globalFlags = config.DefaultFlagSet()
	globalFlags.Home = t.TempDir()

	var logs bytes.Buffer
	previousLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previousLog) })

	stop, err := startRelayerMCP(config.Config{MCP: config.MCPConfig{ListenAddr: "127.0.0.1:0"}},
		relayer.UnimplementedRelayerApiServiceHandler{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop() })
	require.Contains(t, logs.String(), `msg="MCP server listening"`)
	require.Contains(t, logs.String(), "url=http://127.0.0.1:")
	tokenPath := filepath.Join(globalFlags.Home, mcpserver.TokenFileName)
	require.Contains(t, logs.String(), "tokenFile="+tokenPath)
	info, err := os.Stat(tokenPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	token, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	require.NotContains(t, logs.String(), strings.TrimSpace(string(token)), "the token is not logged")
	require.NotContains(t, logs.String(), "MCP server stopped")

	require.NoError(t, stop())
	require.Contains(t, logs.String(), `msg="MCP server stopped"`)
}

func TestEVMBackendSharesRPCClients(t *testing.T) {
	b := &evmBackend{cfg: config.Config{Chains: config.Chains{
		{ChainID: "1", EVM: &config.EVMChainConfig{RPC: "http://127.0.0.1:8545"}},
	}}}
	first, err := b.dial(t.Context(), "1")
	require.NoError(t, err)
	second, err := b.dial(t.Context(), "1")
	require.NoError(t, err)
	require.Same(t, first, second, "one client per chain")

	b.Close()
	_, err = b.dial(t.Context(), "1")
	require.ErrorContains(t, err, "stopping", "no client outlives Close")
}

func TestRedactRPC(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "http://user:secret@rpc.example/v1", Err: errors.New("connection refused")}
	require.Equal(t, "connection refused", redactRPC(err))
	require.NotContains(t, redactRPC(errors.New(`dial http://user:secret@host/path: timeout`)), "secret")
}

func TestSignerLockSerializes(t *testing.T) {
	agentRPC := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
	otherRPC := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
	agentKey, err := signer.GenerateLocalSecp256k1Signer()
	require.NoError(t, err)
	otherKey, err := signer.GenerateLocalSecp256k1Signer()
	require.NoError(t, err)
	agent, err := evmsub.New("1", agentRPC, agentKey, evmsub.ChainOptions{})
	require.NoError(t, err)
	other, err := evmsub.New("1", otherRPC, otherKey, evmsub.ChainOptions{})
	require.NoError(t, err)
	b := &evmBackend{submitters: txsubmitter.NewSet(map[config.ChainSignerPair]txsubmitter.TxSubmitter{
		{ChainID: "1", SignerAlias: "agent"}: agent,
		{ChainID: "1", SignerAlias: "other"}: other,
	})}

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	entered := make(chan struct{})
	agentRPC.send = func(tx *types.Transaction) error {
		agentRPC.record(tx)
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		agentRPC.mu.Lock()
		agentRPC.pending = max(agentRPC.pending, tx.Nonce()+1)
		agentRPC.mu.Unlock()
		return nil
	}
	agentRPC.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful}

	ctx := context.Background()
	firstDone := make(chan error, 1)
	go func() {
		_, sendErr := b.IFTSend(ctx, sendReq("agent"))
		firstDone <- sendErr
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first send did not reach the shared submitter")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, sendErr := b.IFTSend(ctx, sendReq("agent"))
		secondDone <- sendErr
	}()
	select {
	case sendErr := <-secondDone:
		t.Fatal("second send did not wait for the shared nonce allocator", sendErr)
	case <-time.After(50 * time.Millisecond):
	}

	// A different signer has its own allocator and must not wait.
	otherRPC.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful}
	_, err = b.IFTSend(ctx, sendReq("other"))
	require.NoError(t, err)
	require.Len(t, otherRPC.sentCopy(), 1)

	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	sent := agentRPC.sentCopy()
	require.Len(t, sent, 2)
	require.Equal(t, uint64(0), sent[0].Nonce())
	require.Equal(t, uint64(1), sent[1].Nonce())
	require.Equal(t, 2, agentRPC.pendingNonceCalls, "nonce reads stay on the relayer submitter")

	// A signer the relayer does not submit with is refused. No second key is loaded.
	_, err = b.IFTSend(ctx, sendReq("stranger"))
	var denied *mcpserver.ToolError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, mcpserver.CodePolicyDenied, denied.Code)
	require.Nil(t, denied.Tx)
	require.False(t, denied.Retryable)

	t.Run("lost broadcast keeps the hash", func(t *testing.T) {
		lost := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		key, err := signer.GenerateLocalSecp256k1Signer()
		require.NoError(t, err)
		sub, err := evmsub.New("1", lost, key, evmsub.ChainOptions{})
		require.NoError(t, err)
		backend := &evmBackend{submitters: txsubmitter.NewSet(map[config.ChainSignerPair]txsubmitter.TxSubmitter{
			{ChainID: "1", SignerAlias: "agent"}: sub,
		})}
		lost.send = func(tx *types.Transaction) error {
			lost.record(tx)
			return context.DeadlineExceeded
		}
		req := sendReq("agent")
		req.ConfirmTimeout = 300 * time.Millisecond
		_, err = backend.IFTSend(ctx, req)
		var te *mcpserver.ToolError
		require.ErrorAs(t, err, &te)
		require.Equal(t, mcpserver.CodeTxNotConfirmed, te.Code)
		require.False(t, te.Retryable)
		require.NotNil(t, te.Tx)
		require.Equal(t, lost.sentCopy()[0].Hash().Hex(), te.Tx.TxHash)
		require.NotContains(t, te.Error(), "http://")

		// The nonce stays reserved, so the next send cannot replace the first.
		lost.send = func(tx *types.Transaction) error {
			lost.record(tx)
			lost.pending = max(lost.pending, tx.Nonce()+1)
			return nil
		}
		lost.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful}
		_, err = backend.IFTSend(ctx, sendReq("agent"))
		require.NoError(t, err)
		sent := lost.sentCopy()
		require.Len(t, sent, 2)
		require.Equal(t, uint64(0), sent[0].Nonce())
		require.Equal(t, uint64(1), sent[1].Nonce())
	})

	t.Run("rejected broadcast has no transaction", func(t *testing.T) {
		rejected := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		key, err := signer.GenerateLocalSecp256k1Signer()
		require.NoError(t, err)
		sub, err := evmsub.New("1", rejected, key, evmsub.ChainOptions{})
		require.NoError(t, err)
		backend := &evmBackend{submitters: txsubmitter.NewSet(map[config.ChainSignerPair]txsubmitter.TxSubmitter{
			{ChainID: "1", SignerAlias: "agent"}: sub,
		})}
		rejected.send = func(tx *types.Transaction) error {
			rejected.record(tx)
			return errors.New("transaction pool full")
		}
		_, err = backend.IFTSend(ctx, sendReq("agent"))
		var te *mcpserver.ToolError
		require.ErrorAs(t, err, &te)
		require.Equal(t, mcpserver.CodeChainRPCError, te.Code)
		require.True(t, te.Retryable)
		require.Nil(t, te.Tx)
		require.NotContains(t, te.Error(), "http://")
	})

	t.Run("simulation revert is not retryable", func(t *testing.T) {
		reverting := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, reverting)
		reverting.gasErr = errors.New("execution reverted: ERC20InsufficientBalance")
		_, err := backend.IFTSend(ctx, sendReq("agent"))
		requireToolError(t, err, mcpserver.CodeTxReverted, false, "")
		require.Contains(t, err.Error(), "not broadcast")
		require.Contains(t, err.Error(), "ERC20InsufficientBalance")
		require.Empty(t, reverting.sentCopy())

		reverting.gasErr = errors.New("header not found")
		_, err = backend.IFTSend(ctx, sendReq("agent"))
		requireToolError(t, err, mcpserver.CodeChainRPCError, true, "")
	})

	t.Run("refused dial is retryable", func(t *testing.T) {
		dialed := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, dialed)
		dialed.send = func(tx *types.Transaction) error {
			dialed.record(tx)
			return &url.Error{
				Op:  "Post",
				URL: "http://user:secret@127.0.0.1:8545",
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")},
			}
		}
		_, err := backend.IFTSend(ctx, sendReq("agent"))
		requireToolError(t, err, mcpserver.CodeChainRPCError, true, "")
		require.NotContains(t, err.Error(), "secret")
		require.NotContains(t, err.Error(), "http://")

		acceptSend(dialed)
		dialed.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful}
		_, err = backend.IFTSend(ctx, sendReq("agent"))
		require.NoError(t, err)
		sent := dialed.sentCopy()
		require.Len(t, sent, 2)
		require.Equal(t, uint64(0), sent[0].Nonce())
		require.Equal(t, uint64(0), sent[1].Nonce(), "a refused dial does not reserve the nonce")
	})

	t.Run("read error keeps the hash", func(t *testing.T) {
		read := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, read)
		read.send = func(tx *types.Transaction) error {
			read.record(tx)
			read.mu.Lock()
			read.pending = max(read.pending, tx.Nonce()+1)
			read.mu.Unlock()
			return &url.Error{
				Op:  "Post",
				URL: "http://127.0.0.1:8545",
				Err: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")},
			}
		}
		req := sendReq("agent")
		req.ConfirmTimeout = 50 * time.Millisecond
		_, err := backend.IFTSend(ctx, req)
		requireToolError(t, err, mcpserver.CodeTxNotConfirmed, false, read.sentCopy()[0].Hash().Hex())
		require.NotContains(t, err.Error(), "http://")

		acceptSend(read)
		read.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful}
		_, err = backend.IFTSend(ctx, sendReq("agent"))
		require.NoError(t, err)
		sent := read.sentCopy()
		require.Len(t, sent, 2)
		require.Equal(t, uint64(0), sent[0].Nonce())
		require.Equal(t, uint64(1), sent[1].Nonce())
	})

	t.Run("http 502 keeps the hash", func(t *testing.T) {
		gateway := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, gateway)
		gateway.send = func(tx *types.Transaction) error {
			gateway.record(tx)
			gateway.mu.Lock()
			gateway.pending = max(gateway.pending, tx.Nonce()+1)
			gateway.mu.Unlock()
			return rpc.HTTPError{StatusCode: http.StatusBadGateway, Status: "502 Bad Gateway"}
		}
		req := sendReq("agent")
		req.ConfirmTimeout = 50 * time.Millisecond
		_, err := backend.IFTSend(ctx, req)
		requireToolError(t, err, mcpserver.CodeTxNotConfirmed, false, gateway.sentCopy()[0].Hash().Hex())
	})

	t.Run("http 504 keeps the hash", func(t *testing.T) {
		gateway := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, gateway)
		gateway.send = func(tx *types.Transaction) error {
			gateway.record(tx)
			return rpc.HTTPError{StatusCode: http.StatusGatewayTimeout, Status: "504 Gateway Timeout"}
		}
		req := sendReq("agent")
		req.ConfirmTimeout = 50 * time.Millisecond
		_, err := backend.IFTSend(ctx, req)
		requireToolError(t, err, mcpserver.CodeTxNotConfirmed, false, gateway.sentCopy()[0].Hash().Hex())
	})

	t.Run("http 4xx is retryable", func(t *testing.T) {
		rejected := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, rejected)
		rejected.send = func(tx *types.Transaction) error {
			rejected.record(tx)
			return rpc.HTTPError{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"}
		}
		_, err := backend.IFTSend(ctx, sendReq("agent"))
		requireToolError(t, err, mcpserver.CodeChainRPCError, true, "")

		acceptSend(rejected)
		rejected.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful}
		_, err = backend.IFTSend(ctx, sendReq("agent"))
		require.NoError(t, err)
		sent := rejected.sentCopy()
		require.Len(t, sent, 2)
		require.Equal(t, uint64(0), sent[1].Nonce())
	})

	t.Run("mined revert", func(t *testing.T) {
		reverted := &nonceRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
		backend := agentBackend(t, reverted)
		reverted.receipt = &types.Receipt{Status: types.ReceiptStatusFailed}
		_, err := backend.IFTSend(ctx, sendReq("agent"))
		requireToolError(t, err, mcpserver.CodeTxReverted, false, reverted.sentCopy()[0].Hash().Hex())
		require.Len(t, reverted.sentCopy(), 1)
	})
}

func agentBackend(t *testing.T, rpc *nonceRPC) *evmBackend {
	t.Helper()
	key, err := signer.GenerateLocalSecp256k1Signer()
	require.NoError(t, err)
	sub, err := evmsub.New("1", rpc, key, evmsub.ChainOptions{})
	require.NoError(t, err)
	return &evmBackend{submitters: txsubmitter.NewSet(map[config.ChainSignerPair]txsubmitter.TxSubmitter{
		{ChainID: "1", SignerAlias: "agent"}: sub,
	})}
}

func acceptSend(rpc *nonceRPC) {
	rpc.send = func(tx *types.Transaction) error {
		rpc.record(tx)
		rpc.mu.Lock()
		rpc.pending = max(rpc.pending, tx.Nonce()+1)
		rpc.mu.Unlock()
		return nil
	}
}

func requireToolError(t *testing.T, err error, code mcpserver.ErrorCode, retryable bool, hash string) {
	t.Helper()
	var te *mcpserver.ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, code, te.Code)
	require.Equal(t, retryable, te.Retryable)
	if hash == "" {
		require.Nil(t, te.Tx)
		return
	}
	require.NotNil(t, te.Tx)
	require.Equal(t, hash, te.Tx.TxHash)
}

func sendReq(from string) mcpserver.SendRequest {
	return mcpserver.SendRequest{
		ChainID:        "1",
		IFT:            "0x00000000000000000000000000000000000000aa",
		ClientID:       "base-0",
		To:             "0x00000000000000000000000000000000000000bb",
		Amount:         big.NewInt(1),
		From:           from,
		Timeout:        time.Minute,
		ConfirmTimeout: time.Second,
	}
}

// nonceRPC is the relayer submitter's chain client. Sends and nonce reads are counted.
type nonceRPC struct {
	mu                sync.Mutex
	height            uint64
	baseFee, tip      int64
	mined, pending    uint64
	gas               uint64
	sent              []*types.Transaction
	send              func(*types.Transaction) error
	receipt           *types.Receipt
	pendingNonceCalls int
	gasErr            error
}

func (r *nonceRPC) record(tx *types.Transaction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, tx)
}

func (r *nonceRPC) sentCopy() []*types.Transaction {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*types.Transaction, len(r.sent))
	copy(out, r.sent)
	return out
}

func (r *nonceRPC) BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error) {
	return big.NewInt(1), nil
}

func (r *nonceRPC) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return &types.Header{Number: new(big.Int).SetUint64(r.height), BaseFee: big.NewInt(r.baseFee)}, nil
}

func (r *nonceRPC) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return big.NewInt(r.tip), nil
}

func (r *nonceRPC) PendingCodeAt(context.Context, common.Address) ([]byte, error) {
	return []byte{1}, nil
}

func (r *nonceRPC) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	return nil, errors.New("unexpected call")
}

func (r *nonceRPC) EstimateGasAtBlock(_ context.Context, _ ethereum.CallMsg, block *big.Int) (uint64, error) {
	if block != nil {
		return 0, errors.New("expected mined state")
	}
	return r.gas, r.gasErr
}

func (r *nonceRPC) NonceAt(_ context.Context, _ common.Address, block *big.Int) (uint64, error) {
	if block == nil || block.Uint64() != r.height {
		return 0, errors.New("nonce query must be pinned to the observed block height")
	}
	return r.mined, nil
}

func (r *nonceRPC) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	r.mu.Lock()
	r.pendingNonceCalls++
	pending := r.pending
	r.mu.Unlock()
	return pending, nil
}

func (r *nonceRPC) TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	if r.receipt != nil {
		return r.receipt, nil
	}
	return nil, ethereum.NotFound
}

func (r *nonceRPC) SendTransaction(_ context.Context, tx *types.Transaction) error {
	if r.send != nil {
		return r.send(tx)
	}
	r.record(tx)
	r.pending = max(r.pending, tx.Nonce()+1)
	return nil
}

func TestRemoteSignerAddressFromSubmitter(t *testing.T) {
	backend := agentBackend(t, &nonceRPC{height: 1, baseFee: 1, tip: 1, gas: 21_000})
	backend.cfg.Signers = []config.SignerConfig{
		{Alias: "agent", Type: config.SignerRemote, GRPC: "localhost:1", RemoteKeyID: "key"},
	}
	sub, ok := backend.submitters.Get("1", "agent")
	require.True(t, ok)
	want := sub.(*evmsub.TxSubmitter).Address()

	signers, err := backend.Signers(t.Context())
	require.NoError(t, err)
	require.Equal(t, []mcpserver.SignerInfo{{Alias: "agent", Address: want}}, signers)

	got, err := backend.ResolveAddress(t.Context(), "agent")
	require.NoError(t, err)
	require.Equal(t, want, got)
}
