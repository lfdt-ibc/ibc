// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/cosmos/ibc-contracts/packages/go-abigen/besulightclient"
	"github.com/lfdt-ibc/ibc/cli/internal/service/signer"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

const chainIDEth = "1"

// Submission is serialized by the submitter, including access to this RPC stub.
type testRPC struct {
	height         uint64
	baseFee, tip   int64
	mined, pending uint64
	sent           []*types.Transaction
	send           func(*types.Transaction) error
	headerCalls    int
	tipCalls       int
	codeCalls      int
	gasCalls       int
	gas            uint64
	gasErr         error
	// gasErrFor, when set, fails estimation per call.
	gasErrFor  func(ethereum.CallMsg) error
	callOutput []byte
	nonceErr   error
	receipt    *types.Receipt
	receiptErr error
	// receiptFor, when set, answers TransactionReceipt per hash.
	receiptFor func(common.Hash) (*types.Receipt, error)
}

func (r *testRPC) BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error) {
	return big.NewInt(1), nil
}

func (r *testRPC) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	r.headerCalls++
	return &types.Header{Number: new(big.Int).SetUint64(r.height), BaseFee: big.NewInt(r.baseFee)}, nil
}

func (r *testRPC) SuggestGasTipCap(context.Context) (*big.Int, error) {
	r.tipCalls++
	return big.NewInt(r.tip), nil
}

func (r *testRPC) PendingCodeAt(context.Context, common.Address) ([]byte, error) {
	r.codeCalls++
	return []byte{1}, nil
}

func (r *testRPC) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	if r.callOutput != nil {
		return r.callOutput, nil
	}
	return nil, errors.New("unexpected call")
}

func (r *testRPC) EstimateGasAtBlock(_ context.Context, msg ethereum.CallMsg, block *big.Int) (uint64, error) {
	r.gasCalls++
	if block != nil {
		return 0, errors.New("expected mined state")
	}
	if r.gasErrFor != nil {
		if err := r.gasErrFor(msg); err != nil {
			return 0, err
		}
	}
	return r.gas, r.gasErr
}

func (r *testRPC) NonceAt(_ context.Context, _ common.Address, block *big.Int) (uint64, error) {
	if block == nil || block.Uint64() != r.height {
		return 0, errors.New("nonce query must be pinned to the observed block height")
	}
	return r.mined, r.nonceErr
}

func (r *testRPC) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	return r.pending, nil
}

func (r *testRPC) TransactionReceipt(_ context.Context, hash common.Hash) (*types.Receipt, error) {
	if r.receiptFor != nil {
		return r.receiptFor(hash)
	}
	if r.receipt != nil || r.receiptErr != nil {
		return r.receipt, r.receiptErr
	}
	return nil, ethereum.NotFound
}

func (r *testRPC) SendTransaction(_ context.Context, tx *types.Transaction) error {
	r.sent = append(r.sent, tx)
	if r.send != nil {
		return r.send(tx)
	}
	r.pending = max(r.pending, tx.Nonce()+1)
	return nil
}

func newSubmitter(t *testing.T) (*TxSubmitter, *testRPC) {
	t.Helper()
	key, err := signer.GenerateLocalSecp256k1Signer()
	require.NoError(t, err)
	rpc := &testRPC{height: 100, baseFee: 100, tip: 10, gas: 100_000}
	c, err := New(chainIDEth, rpc, key, ChainOptions{})
	require.NoError(t, err)
	return c, rpc
}

func testIntent() v2.TxIntent {
	return v2.TxIntent{To: "0x1111111111111111111111111111111111111111", Data: []byte{1}}
}

func expireNonce(c *TxSubmitter) {
	c.lowestUnminedNoncePendingSince = time.Now().Add(-2 * c.opts.PendingTxTimeout)
}

func submit(t *testing.T, c *TxSubmitter) error {
	t.Helper()
	submission, err := c.Submit(t.Context(), testIntent())
	if err != nil {
		require.Nil(t, submission)
	}
	return err
}

func TestConcurrentSubmissionsAllocateDistinctNonces(t *testing.T) {
	c, rpc := newSubmitter(t)
	const count = maxInFlight
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for range count {
		wg.Go(func() { errs <- submit(t, c) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, rpc.sent, count)
	for i, tx := range rpc.sent {
		require.Equal(t, uint64(i), tx.Nonce())
	}
	require.ErrorIs(t, submit(t, c), ErrBusy)
}

func TestStalledNonceRecoveredAtCapacity(t *testing.T) {
	c, rpc := newSubmitter(t)
	for range maxInFlight {
		require.NoError(t, submit(t, c))
	}
	first := rpc.sent[0]
	require.ErrorIs(t, submit(t, c), ErrBusy)
	expireNonce(c)
	require.NoError(t, submit(t, c))
	replacement := rpc.sent[maxInFlight]
	require.Equal(t, first.Nonce(), replacement.Nonce())
	require.Positive(t, replacement.GasFeeCap().Cmp(first.GasFeeCap()))
	require.Positive(t, replacement.GasTipCap().Cmp(first.GasTipCap()))
	require.ErrorIs(t, submit(t, c), ErrBusy, "give the replacement time to mine")
	rpc.mined = maxInFlight
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(maxInFlight), rpc.sent[maxInFlight+1].Nonce())
}

func TestRetryBumpsOnUnderpricedAndPreservesProgress(t *testing.T) {
	for _, message := range []string{
		"replacement transaction underpriced",
		"Replacement transaction underpriced",
		"rpc error: Replacement transaction underpriced",
	} {
		t.Run(message, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.pending = 1
			rpc.send = func(tx *types.Transaction) error {
				if tx.GasTipCap().Cmp(big.NewInt(100)) < 0 {
					return errors.New(message)
				}
				return nil
			}
			// A fresh process gives the unknown transaction a grace period.
			_, _, err := c.selectNonce(t.Context(), time.Now())
			require.NoError(t, err)
			expireNonce(c)
			require.ErrorContains(t, submit(t, c), "still underpriced")
			require.Len(t, rpc.sent, maxBroadcastAttempts)
			require.Equal(t, 1, rpc.codeCalls)
			require.Equal(t, 1, rpc.gasCalls)
			require.Equal(t, 1, rpc.tipCalls)
			require.Equal(t, 2, rpc.headerCalls, "one initial observation and one submission")
			// The next call continues escalating rather than restarting at the estimate.
			rpc.gas = 150_000
			require.NoError(t, submit(t, c))
			require.Greater(t, len(rpc.sent), maxBroadcastAttempts)
			require.Equal(t, 2, rpc.codeCalls)
			require.Equal(t, 2, rpc.gasCalls)
			require.Equal(t, 2, rpc.tipCalls)
			require.Equal(t, 3, rpc.headerCalls)
			for i, tx := range rpc.sent {
				require.Zero(t, tx.Nonce())
				require.LessOrEqual(t, tx.GasFeeCap().Int64(), int64(840))
				if i < maxBroadcastAttempts {
					require.Equal(t, uint64(100_000), tx.Gas())
				} else {
					require.Equal(t, uint64(150_000), tx.Gas())
				}
				if i > 0 {
					require.GreaterOrEqual(t, tx.GasFeeCap().Cmp(rpc.sent[i-1].GasFeeCap()), 0)
					require.Positive(t, tx.GasTipCap().Cmp(rpc.sent[i-1].GasTipCap()))
				}
			}
		})
	}
}

func TestAcceptedTransactionStillGetsHigherFeesAfterStalling(t *testing.T) {
	c, rpc := newSubmitter(t)
	for range 3 {
		require.NoError(t, submit(t, c))
		expireNonce(c)
	}
	require.Len(t, rpc.sent, 3)
	for i, tx := range rpc.sent {
		require.Zero(t, tx.Nonce())
		if i > 0 {
			require.Positive(t, tx.GasTipCap().Cmp(rpc.sent[i-1].GasTipCap()))
		}
	}
}

func TestBroadcastErrorDoesNotAdvanceNonceOrFees(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "rejected", err: errors.New("transaction pool full")},
		{name: "insufficient funds", err: errors.New("insufficient funds for gas * price + value")},
		{name: "underpriced without replacement", err: errors.New("transaction underpriced")},
		{name: "lost response", err: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.send = func(*types.Transaction) error { return tt.err }
			submission, err := c.Submit(t.Context(), testIntent())
			require.ErrorIs(t, err, tt.err)
			require.Nil(t, submission)
			require.Len(t, rpc.sent, 1)
			require.Zero(t, c.nextNonce)

			for range 2 {
				require.ErrorIs(t, submit(t, c), tt.err)
				require.Zero(t, c.nextNonce)
			}
			rpc.send = nil
			submission, err = c.Submit(t.Context(), testIntent())
			require.NoError(t, err)
			require.NotNil(t, submission)
			require.Len(t, rpc.sent, 4)
			require.Equal(t, rpc.sent[3].Hash().Hex(), submission.TxHash)
			require.Equal(t, c.address.Hex(), submission.RelayerAddress)
			require.Equal(t, testIntent().Data, rpc.sent[3].Data())
			require.WithinDuration(t, time.Now(), submission.SubmittedAt, time.Second)
			for _, tx := range rpc.sent {
				require.Zero(t, tx.Nonce())
				require.Equal(t, rpc.sent[0].GasFeeCap(), tx.GasFeeCap())
				require.Equal(t, rpc.sent[0].GasTipCap(), tx.GasTipCap())
			}
			require.Equal(t, uint64(1), c.nextNonce)
		})
	}
}

func TestRejectedReplacementPreservesAcceptedFees(t *testing.T) {
	c, rpc := newSubmitter(t)
	require.NoError(t, submit(t, c))
	original := rpc.sent[0]
	expireNonce(c)
	rejected := errors.New("transaction pool full")
	rpc.send = func(*types.Transaction) error { return rejected }
	for range 3 {
		require.ErrorIs(t, submit(t, c), rejected)
	}
	rpc.send = nil
	require.NoError(t, submit(t, c))
	require.Len(t, rpc.sent, 5)
	firstReplacement := rpc.sent[1]
	require.Positive(t, firstReplacement.GasFeeCap().Cmp(original.GasFeeCap()))
	require.Positive(t, firstReplacement.GasTipCap().Cmp(original.GasTipCap()))
	for _, tx := range rpc.sent[1:] {
		require.Equal(t, original.Nonce(), tx.Nonce())
		require.Equal(t, firstReplacement.GasFeeCap(), tx.GasFeeCap())
		require.Equal(t, firstReplacement.GasTipCap(), tx.GasTipCap())
	}
}

func TestLostResponseUsesObservedPendingNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.send = func(tx *types.Transaction) error {
		rpc.pending = tx.Nonce() + 1
		return context.DeadlineExceeded
	}
	require.ErrorIs(t, submit(t, c), context.DeadlineExceeded)
	require.Zero(t, c.nextNonce)
	rpc.send = nil
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(1), rpc.sent[1].Nonce())
}

func TestAlreadyKnownTransactionAdvancesNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.send = func(*types.Transaction) error { return errors.New("already known") }
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(1), c.nextNonce)
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(1), rpc.sent[1].Nonce())
}

func TestRestartRecoversUnknownGap(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.mined, rpc.pending = 7, 9
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(9), rpc.sent[0].Nonce())
	restarted, err := New(chainIDEth, rpc, c.signer, c.opts)
	require.NoError(t, err)
	_, _, err = restarted.selectNonce(t.Context(), time.Now())
	require.NoError(t, err)
	expireNonce(restarted)
	require.NoError(t, submit(t, restarted))
	require.Equal(t, uint64(7), rpc.sent[1].Nonce())
}

func TestConfirmKeepsHashWhenBroadcastMayHaveLanded(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.send = func(*types.Transaction) error { return context.DeadlineExceeded }
	hash, receipt, err := c.Confirm(t.Context(), testIntent(), 300*time.Millisecond, nil, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, receipt)
	require.Equal(t, rpc.sent[0].Hash().Hex(), hash)
	require.Equal(t, uint64(1), c.nextNonce, "a broadcast that may have landed reserves the nonce")

	rpc.send = nil
	rpc.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: common.HexToHash(hash)}
	next, _, err := c.Confirm(t.Context(), testIntent(), time.Second, nil, nil)
	require.NoError(t, err)
	require.NotEqual(t, hash, next)
	require.Equal(t, uint64(1), rpc.sent[1].Nonce())
}

func TestConfirmReportsBroadcastBeforeMining(t *testing.T) {
	c, rpc := newSubmitter(t)
	var broadcast []string
	onBroadcast := func(hash string) {
		require.Nil(t, rpc.receipt, "reported before the receipt")
		broadcast = append(broadcast, hash)
		rpc.receipt = &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: common.HexToHash(hash)}
	}
	hash, receipt, err := c.Confirm(t.Context(), testIntent(), time.Second, onBroadcast, nil)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, []string{hash}, broadcast)

	rejected := errors.New("transaction pool full")
	rpc.send = func(*types.Transaction) error { return rejected }
	broadcast = nil
	_, _, err = c.Confirm(t.Context(), testIntent(), time.Second, onBroadcast, nil)
	require.ErrorIs(t, err, rejected)
	require.Empty(t, broadcast, "a rejected broadcast is not reported")
}

func TestConfirmRejectedBroadcastDoesNotReserveNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	rejected := errors.New("transaction pool full")
	rpc.send = func(*types.Transaction) error { return rejected }
	hash, receipt, err := c.Confirm(t.Context(), testIntent(), time.Second, nil, nil)
	require.ErrorIs(t, err, rejected)
	require.Empty(t, hash)
	require.Nil(t, receipt)
	require.Zero(t, c.nextNonce)
}

func TestConfirmClassifiesBroadcastFailures(t *testing.T) {
	dialRefused := &url.Error{
		Op:  "Post",
		URL: "http://127.0.0.1:8545",
		Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")},
	}
	dialTimeout := &url.Error{
		Op:  "Post",
		URL: "http://127.0.0.1:8545",
		Err: &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded},
	}
	readReset := &url.Error{
		Op:  "Post",
		URL: "http://127.0.0.1:8545",
		Err: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")},
	}
	writeTimeout := &url.Error{
		Op:  "Post",
		URL: "http://127.0.0.1:8545",
		Err: &net.OpError{Op: "write", Net: "tcp", Err: errors.New("i/o timeout")},
	}
	// These fail before the signed transaction is written. *url.Error
	// implements net.Error, so a blanket network check would reserve the nonce.
	handshake := &url.Error{
		Op:  "Post",
		URL: "https://127.0.0.1:8545",
		Err: tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"},
	}
	certificate := &url.Error{
		Op:  "Post",
		URL: "https://127.0.0.1:8545",
		Err: errors.New("tls: failed to verify certificate: x509: certificate signed by unknown authority"),
	}
	// net/http's real handshake failures also implement net.Error. RecordHeaderError
	// and a plain string do not, so they miss this path.
	handshakeTimeout := netHTTPHandshakeTimeout(t)
	remoteAlert := netHTTPTLSAlert(t)
	localAlert := &url.Error{
		Op:  "Post",
		URL: "https://127.0.0.1:8545",
		Err: &net.OpError{Op: "local error", Err: errors.New("tls: handshake failure")},
	}
	var remoteCause *url.Error
	require.ErrorAs(t, remoteAlert, &remoteCause)
	connectionBroken := &url.Error{
		Op:  "Post",
		URL: "https://127.0.0.1:8545",
		Err: fmt.Errorf("net/http: HTTP/1.x transport connection broken: %w", remoteCause.Err),
	}
	// GoAwayError replaces the read after the server has received the stream.
	// It is not a net.Error and does not unwrap to that read.
	goAwayClosed := &url.Error{
		Op:  "Post",
		URL: "https://127.0.0.1:8545",
		Err: errors.New(`http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=0, debug=""`),
	}
	// The server has not received the stream. The HTTP/2 client retries this
	// itself, so the nonce stays free.
	goAwayGraceful := &url.Error{
		Op:  "Post",
		URL: "https://127.0.0.1:8545",
		Err: errors.New("http2: Transport received Server's graceful shutdown GOAWAY"),
	}
	for _, tt := range []struct {
		name     string
		err      error
		keepHash bool
	}{
		{name: "refused dial", err: dialRefused},
		{name: "dial timeout", err: dialTimeout},
		{name: "tls handshake", err: handshake},
		{name: "tls handshake timeout", err: handshakeTimeout},
		{name: "tls remote alert", err: remoteAlert},
		{name: "tls local alert", err: localAlert},
		{name: "certificate", err: certificate},
		{name: "transport connection broken", err: connectionBroken, keepHash: true},
		{name: "http2 goaway closed connection", err: goAwayClosed, keepHash: true},
		{name: "http2 graceful shutdown goaway", err: goAwayGraceful},
		{name: "deadline exceeded", err: context.DeadlineExceeded, keepHash: true},
		{name: "read error", err: readReset, keepHash: true},
		{name: "write error", err: writeTimeout, keepHash: true},
		{name: "eof", err: io.EOF, keepHash: true},
		{name: "http 500", err: rpc.HTTPError{StatusCode: 500, Status: "500 Internal Server Error"}, keepHash: true},
		{name: "http 502", err: rpc.HTTPError{StatusCode: 502, Status: "502 Bad Gateway"}, keepHash: true},
		{name: "http 504", err: rpc.HTTPError{StatusCode: 504, Status: "504 Gateway Timeout"}, keepHash: true},
		{name: "http 400", err: rpc.HTTPError{StatusCode: 400, Status: "400 Bad Request"}},
		{name: "http 429", err: rpc.HTTPError{StatusCode: 429, Status: "429 Too Many Requests"}},
		{name: "nonce too low", err: errors.New("nonce too low")},
		{name: "execution reverted", err: errors.New("execution reverted")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.send = func(*types.Transaction) error { return tt.err }
			timeout := time.Duration(0)
			if tt.keepHash {
				timeout = 30 * time.Millisecond
			}
			hash, receipt, err := c.Confirm(t.Context(), testIntent(), timeout, nil, nil)
			require.Error(t, err)
			require.Nil(t, receipt)
			if !tt.keepHash {
				require.Empty(t, hash)
				require.Zero(t, c.nextNonce)
				require.Empty(t, c.confirmReserved)
				rpc.send = nil
				require.NoError(t, submit(t, c))
				require.Zero(t, rpc.sent[len(rpc.sent)-1].Nonce())
				return
			}
			require.Equal(t, rpc.sent[0].Hash().Hex(), hash)
			require.Equal(t, uint64(1), c.nextNonce)
			_, reserved := c.confirmReserved[0]
			require.True(t, reserved)
		})
	}
}

// netHTTPHandshakeTimeout returns http.DefaultTransport's TLS handshake timeout:
// an unexported net.Error produced before the POST body is written.
func netHTTPHandshakeTimeout(t *testing.T) error {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		<-done
	}()
	client := &http.Client{Transport: &http.Transport{
		Proxy:               nil,
		TLSHandshakeTimeout: 100 * time.Millisecond,
	}}
	resp, err := client.Get("https://" + ln.Addr().String())
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.Error(t, err)
	var ue *url.Error
	require.ErrorAs(t, err, &ue)
	require.Equal(t, "net/http: TLS handshake timeout", ue.Err.Error())
	var ne net.Error
	require.ErrorAs(t, ue.Err, &ne)
	require.True(t, ne.Timeout())
	return err
}

// netHTTPTLSAlert is the error crypto/tls returns for a fatal handshake alert:
// an unexported net.Error wrapping *net.OpError with Op "remote error".
func netHTTPTLSAlert(t *testing.T) error {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 512)
		_, _ = conn.Read(buf)
		// TLS record: content-type alert, version TLS 1.2, fatal handshake_failure.
		_, _ = conn.Write([]byte{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28})
		_, _ = conn.Read(buf)
	}()
	client := &http.Client{Transport: &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		TLSHandshakeTimeout: 2 * time.Second,
	}}
	resp, err := client.Get("https://" + ln.Addr().String())
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.Error(t, err)
	var ue *url.Error
	require.ErrorAs(t, err, &ue)
	var opErr *net.OpError
	require.ErrorAs(t, ue.Err, &opErr)
	require.Equal(t, "remote error", opErr.Op)
	var ne net.Error
	require.ErrorAs(t, ue.Err, &ne)
	return err
}

func TestSubmitReplacesStalledConfirmTransfer(t *testing.T) {
	for _, tt := range []struct {
		name string
		send func(*testRPC, *types.Transaction) error
	}{
		{name: "receipt times out"},
		{
			name: "broadcast may have landed",
			send: func(rpc *testRPC, tx *types.Transaction) error {
				rpc.pending = tx.Nonce() + 1
				return context.DeadlineExceeded
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			if tt.send != nil {
				rpc.send = func(tx *types.Transaction) error { return tt.send(rpc, tx) }
			}
			hash, receipt, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, nil)
			require.Error(t, err)
			require.Nil(t, receipt)
			require.Equal(t, rpc.sent[0].Hash().Hex(), hash)
			require.Equal(t, uint64(1), c.nextNonce)
			require.Equal(t, uint64(1), rpc.pending)

			// Before the stall timeout the transfer keeps its nonce.
			rpc.send = nil
			relay := testIntent()
			relay.Data = []byte{9}
			require.NoError(t, submitIntent(t, c, relay))
			require.Len(t, rpc.sent, 2)
			require.Equal(t, uint64(1), rpc.sent[1].Nonce())

			// After it, the transfer is re-signed at its nonce with higher fees
			// and the relay intent still gets the next nonce.
			expireNonce(c)
			require.NoError(t, submitIntent(t, c, relay))
			require.Len(t, rpc.sent, 4)
			replacement := rpc.sent[2]
			require.Equal(t, uint64(0), replacement.Nonce())
			require.Equal(t, []byte{1}, replacement.Data())
			require.Equal(t, 1, replacement.GasFeeCap().Cmp(rpc.sent[0].GasFeeCap()))
			require.Equal(t, 1, replacement.GasTipCap().Cmp(rpc.sent[0].GasTipCap()))
			require.Equal(t, uint64(2), rpc.sent[3].Nonce())
			require.Equal(t, []byte{9}, rpc.sent[3].Data())
			r, reserved := c.confirmReserved[0]
			require.True(t, reserved, "pending transfer keeps the Confirm reservation")
			require.Equal(t, []string{hash, replacement.Hash().Hex()}, r.hashes)

			// A node that no longer has the transfer frees the nonce.
			rpc.mined, rpc.pending = 0, 0
			expireNonce(c)
			require.NoError(t, submitIntent(t, c, relay))
			require.Equal(t, uint64(0), rpc.sent[4].Nonce())
			require.Equal(t, []byte{9}, rpc.sent[4].Data())
			_, reserved = c.confirmReserved[0]
			require.False(t, reserved)
		})
	}
}

func TestConfirmReportsMinedReplacement(t *testing.T) {
	c, rpc := newSubmitter(t)
	replaced := make(chan [2]string, 1)
	first, _, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, func(previous, replacement string) {
		replaced <- [2]string{previous, replacement}
	})
	require.Error(t, err)
	// Confirm's wait holds this pointer, not a lookup by nonce.
	reserved := c.confirmReserved[0]
	require.NotNil(t, reserved)

	expireNonce(c)
	require.NoError(t, submit(t, c))
	replacement := rpc.sent[1].Hash()
	require.Equal(t, []byte{1}, rpc.sent[1].Data())
	require.Equal(t, uint64(0), rpc.sent[1].Nonce())
	// Confirm has returned; the caller still hears about the new hash.
	select {
	case got := <-replaced:
		require.Equal(t, [2]string{first, replacement.Hex()}, got)
	case <-time.After(2 * time.Second):
		t.Fatal("replacement not reported")
	}

	// The replacement is mined, and the next submission drops the map entry
	// before the wait polls again.
	rpc.mined = 1
	require.NoError(t, submit(t, c))
	require.Empty(t, c.confirmReserved)
	for _, h := range []string{first, replacement.Hex()} {
		require.False(t, trackedTx(c.chainID, h), "a dropped reservation leaves no gas-metrics entry for %s", h)
	}

	rpc.receiptFor = func(hash common.Hash) (*types.Receipt, error) {
		if hash == replacement {
			return &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: hash}, nil
		}
		return nil, ethereum.NotFound
	}
	mined, receipt, err := c.waitReceipt(t.Context(), reserved, first)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, replacement.Hex(), mined)

	// A wait that times out reports the latest signed hash.
	rpc.receiptFor = func(common.Hash) (*types.Receipt, error) { return nil, ethereum.NotFound }
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	latest, receipt, err := c.waitReceipt(ctx, reserved, first)
	require.ErrorIs(t, err, ethereum.NotFound)
	require.Nil(t, receipt)
	require.Equal(t, replacement.Hex(), latest)

	// Without a reservation only the first hash is polled.
	ctx2, cancel2 := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel2()
	latest, _, err = c.waitReceipt(ctx2, nil, first)
	require.Error(t, err)
	require.Equal(t, first, latest)
}

func TestFailedStallReplacementRestartsTimer(t *testing.T) {
	c, rpc := newSubmitter(t)
	_, _, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, nil)
	require.Error(t, err)

	expireNonce(c)
	rpc.send = func(tx *types.Transaction) error {
		if tx.Nonce() == 0 {
			return errors.New("transaction pool full")
		}
		rpc.pending = max(rpc.pending, tx.Nonce()+1)
		return nil
	}
	require.NoError(t, submit(t, c))
	require.Len(t, rpc.sent, 3)
	require.Equal(t, uint64(0), rpc.sent[1].Nonce(), "the replacement was attempted")
	require.Equal(t, uint64(1), rpc.sent[2].Nonce(), "a failed replacement does not block the next intent")
	require.False(t, c.lowestUnminedNoncePendingSince.Before(time.Now().Add(-time.Second)))
	_, reserved := c.confirmReserved[0]
	require.True(t, reserved)
}

func TestRevertingStallReplacementFreesNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	_, _, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, nil)
	require.Error(t, err)
	require.Equal(t, uint64(1), rpc.pending)

	// The transfer would now revert, for example because the balance moved.
	rpc.gasErrFor = func(msg ethereum.CallMsg) error {
		if len(msg.Data) == 1 && msg.Data[0] == 1 {
			return rpcCodeError{code: 3, msg: "execution reverted: insufficient balance"}
		}
		return nil
	}
	expireNonce(c)
	relay := testIntent()
	relay.Data = []byte{9}
	require.NoError(t, submitIntent(t, c, relay))
	require.Len(t, rpc.sent, 2)
	require.Equal(t, uint64(0), rpc.sent[1].Nonce(), "the relay replaces the transfer that would revert")
	require.Equal(t, []byte{9}, rpc.sent[1].Data())
	require.Empty(t, c.confirmReserved)
}

func submitIntent(t *testing.T, c *TxSubmitter, intent v2.TxIntent) error {
	t.Helper()
	submission, err := c.Submit(t.Context(), intent)
	if err == nil {
		require.NotNil(t, submission)
	}
	return err
}

func TestPreparationFailureDoesNotAllocateNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.gasErr = errors.New("execution reverted")
	submission, err := c.Submit(t.Context(), testIntent())
	require.ErrorIs(t, err, rpc.gasErr)
	require.Nil(t, submission)
	require.Empty(t, rpc.sent)
	require.Zero(t, c.nextNonce)
	rpc.gasErr = nil
	require.NoError(t, submit(t, c))
	require.Zero(t, rpc.sent[0].Nonce())
}

type stalledSigner struct{ signer.Signer }

func (s stalledSigner) Sign(ctx context.Context, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSignerDeadlineDoesNotConsumeNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	key := c.signer
	c.signer = stalledSigner{key}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	submission, err := c.Submit(ctx, testIntent())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, submission)
	require.Empty(t, rpc.sent)
	c.signer = key
	require.NoError(t, submit(t, c))
	require.Zero(t, rpc.sent[0].Nonce())
}

func TestSubmissionUsesNodeFeeEstimate(t *testing.T) {
	for _, tt := range []struct {
		name             string
		baseFee, tip     int64
		wantFee, wantTip int64
	}{
		{name: "standard fees", baseFee: 100, tip: 10, wantFee: 210, wantTip: 10},
		{name: "zero fees"},
		{
			name: "above old fixed cap", baseFee: 20_000_000_000, tip: 1_000_000_000,
			wantFee: 41_000_000_000, wantTip: 1_000_000_000,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.baseFee, rpc.tip = tt.baseFee, tt.tip
			require.NoError(t, submit(t, c))
			require.Len(t, rpc.sent, 1)
			require.Equal(t, big.NewInt(tt.wantFee), rpc.sent[0].GasFeeCap())
			require.Equal(t, big.NewInt(tt.wantTip), rpc.sent[0].GasTipCap())
			require.Equal(t, 1, rpc.tipCalls)
		})
	}
}

func TestStalledReplacementRefreshesFeeCeiling(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.send = func(*types.Transaction) error { return errors.New("replacement transaction underpriced") }
	require.ErrorContains(t, submit(t, c), "still underpriced")
	rpc.send = nil
	for i, prices := range []struct{ baseFee, tip, wantFee int64 }{
		{100, 10, 840},
		{200, 20, 1008},
		{50, 5, 420},
		{50, 5, 420},
		{0, 2, 8},
	} {
		rpc.baseFee, rpc.tip = prices.baseFee, prices.tip
		require.NoError(t, submit(t, c))
		require.Len(t, rpc.sent, maxBroadcastAttempts+i+1)
		tx := rpc.sent[maxBroadcastAttempts+i]
		require.Zero(t, tx.Nonce())
		require.Equal(t, big.NewInt(prices.wantFee), tx.GasFeeCap())
		require.LessOrEqual(t, tx.GasTipCap().Cmp(tx.GasFeeCap()), 0)
		expireNonce(c)
	}
	require.Equal(t, 6, rpc.tipCalls)
}

func TestUnderpricedRetriesStopAtFeeCeiling(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.baseFee, rpc.tip = 0, 2
	underpriced := errors.New("replacement transaction underpriced")
	rpc.send = func(*types.Transaction) error {
		rpc.baseFee, rpc.tip = 1, 4
		return underpriced
	}

	submission, err := c.Submit(t.Context(), testIntent())
	require.ErrorIs(t, err, underpriced)
	require.Nil(t, submission)
	require.ErrorContains(t, err, "fee ceiling 8 wei (4x fresh estimate)")
	require.Len(t, rpc.sent, 6)
	require.Equal(t, 1, rpc.tipCalls)
	for i, tx := range rpc.sent {
		require.Zero(t, tx.Nonce())
		require.LessOrEqual(t, tx.GasFeeCap().Int64(), int64(8))
		if i > 0 {
			require.Positive(t, tx.GasTipCap().Cmp(rpc.sent[i-1].GasTipCap()))
		}
	}
	last := rpc.sent[5]
	require.Equal(t, big.NewInt(8), last.GasFeeCap())
	require.Equal(t, big.NewInt(8), last.GasTipCap())

	// The next submission gets a fresh ceiling and can increase fees again.
	rpc.send = nil
	require.NoError(t, submit(t, c))
	require.Len(t, rpc.sent, 7)
	require.Equal(t, big.NewInt(10), rpc.sent[6].GasFeeCap())
	require.Equal(t, big.NewInt(10), rpc.sent[6].GasTipCap())
	require.Equal(t, 2, rpc.tipCalls)
}

func TestNonceQueryFailureDoesNotBroadcast(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.nonceErr = errors.New("unavailable")
	require.ErrorIs(t, submit(t, c), rpc.nonceErr)
	require.Empty(t, rpc.sent)
}

func TestLaggingHeadDoesNotResetNonceRecovery(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.mined, rpc.pending = 7, 7
	for range maxInFlight {
		require.NoError(t, submit(t, c))
	}
	require.ErrorIs(t, submit(t, c), ErrBusy)
	expireNonce(c)
	lowestUnminedNoncePendingSince := c.lowestUnminedNoncePendingSince
	feeCap := new(big.Int).Set(c.lastAttemptedFeeCap)
	tipCap := new(big.Int).Set(c.lastAttemptedTipCap)

	for range 3 {
		rpc.height, rpc.mined = 99, 6
		require.ErrorContains(t, submit(t, c), "behind last nonce observation")
		require.Equal(t, uint64(7), c.lowestUnminedNonce)
		require.Equal(t, lowestUnminedNoncePendingSince, c.lowestUnminedNoncePendingSince)
		require.Equal(t, feeCap, c.lastAttemptedFeeCap)
		require.Equal(t, tipCap, c.lastAttemptedTipCap)

		rpc.height, rpc.mined = 100, 7
		nonce, _, err := c.selectNonce(t.Context(), time.Now())
		require.NoError(t, err)
		require.Equal(t, uint64(7), nonce)
	}

	require.Len(t, rpc.sent, maxInFlight)
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(7), rpc.sent[maxInFlight].Nonce())
	require.Positive(t, rpc.sent[maxInFlight].GasFeeCap().Cmp(feeCap))
}

func TestFailedNonceQueryDoesNotAdvanceObservedHeight(t *testing.T) {
	c, rpc := newSubmitter(t)
	require.NoError(t, submit(t, c))
	rpc.height = 101
	rpc.nonceErr = errors.New("unavailable")
	require.ErrorIs(t, submit(t, c), rpc.nonceErr)
	rpc.height = 100
	rpc.nonceErr = nil
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(1), rpc.sent[1].Nonce())
}

func TestReorgAllowsLowestNonceReuse(t *testing.T) {
	for _, tt := range []struct {
		name   string
		height uint64
	}{
		{name: "same height", height: 101},
		{name: "higher height", height: 102},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.mined, rpc.pending = 7, 7
			require.NoError(t, submit(t, c))
			rpc.height, rpc.mined, rpc.pending = 101, 8, 8
			require.NoError(t, submit(t, c))
			expireNonce(c)
			rpc.height, rpc.mined, rpc.pending = tt.height, 7, 7
			now := time.Now()
			nonce, _, err := c.selectNonce(t.Context(), now)
			require.NoError(t, err)
			require.Equal(t, uint64(9), nonce)
			require.Equal(t, now, c.lowestUnminedNoncePendingSince)
			expireNonce(c)
			require.NoError(t, submit(t, c))
			require.Equal(t, uint64(7), rpc.sent[2].Nonce())
		})
	}
}

func TestDroppedNonceRecoversBelowLocalAllocation(t *testing.T) {
	c, rpc := newSubmitter(t)
	rpc.mined, rpc.pending = 7, 7
	for range 3 {
		require.NoError(t, submit(t, c))
	}
	require.Equal(t, uint64(10), c.nextNonce)
	rpc.pending = 7
	nonce, _, err := c.selectNonce(t.Context(), time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(10), nonce, "allow the existing transactions time to mine")
	expireNonce(c)
	require.NoError(t, submit(t, c))
	require.Equal(t, uint64(7), rpc.sent[3].Nonce())
}

func TestShouldRetry(t *testing.T) {
	for _, tt := range []struct {
		name    string
		receipt *types.Receipt
		err     error
		expired bool
		retry   bool
		wantErr error
	}{
		{name: "pending", wantErr: v2.ErrTxNotFound},
		{name: "expired missing receipt", expired: true, retry: true},
		{name: "mined success", receipt: &types.Receipt{Status: types.ReceiptStatusSuccessful}},
		{name: "old mined success", receipt: &types.Receipt{Status: types.ReceiptStatusSuccessful}, expired: true},
		{name: "mined failure", receipt: &types.Receipt{Status: types.ReceiptStatusFailed}, retry: true},
		{name: "RPC unavailable", err: context.DeadlineExceeded, expired: true, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.receipt, rpc.receiptErr = tt.receipt, tt.err
			sentAt := time.Now()
			if tt.expired {
				sentAt = sentAt.Add(-2 * c.opts.PendingTxTimeout)
			}
			retry, err := c.ShouldRetry(t.Context(), common.Hash{}.Hex(), sentAt)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.retry, retry)
		})
	}
}

type revertError string

func (revertError) Error() string    { return "execution reverted" }
func (revertError) ErrorCode() int   { return -32000 }
func (e revertError) ErrorData() any { return string(e) }

func TestSubmitRejectsEstimationRevert(t *testing.T) {
	submitter, eth := newSubmitter(t)
	contractABI, err := besulightclient.ContractMetaData.GetAbi()
	require.NoError(t, err)
	customErr := contractABI.Errors["ConsensusStateExpired"]
	args, err := customErr.Inputs.Pack(uint64(100), big.NewInt(121), uint64(20))
	require.NoError(t, err)
	revert := revertError(hexutil.Encode(append(customErr.ID[:4], args...)))
	eth.gasErr = revert
	result, err := submitter.Submit(t.Context(), testIntent())
	require.Nil(t, result)
	require.ErrorIs(t, err, revert)
	require.ErrorIs(t, err, ErrEstimationReverted)
	require.EqualError(t, err, "estimating gas: ConsensusStateExpired(100, 121, 20): execution reverted")
	require.Empty(t, eth.sent)
	require.Zero(t, submitter.nextNonce)
}

func TestEstimationRevertClassification(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		reverted bool
	}{
		{name: "geth revert code 3", err: rpcCodeError{code: 3, msg: "execution reverted: insufficient balance"}, reverted: true},
		{name: "besu revert -32000", err: rpcCodeError{code: -32000, msg: "Execution reverted"}, reverted: true},
		{name: "node failure -32000", err: rpcCodeError{code: -32000, msg: "header not found"}},
		{name: "transport", err: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, rpc := newSubmitter(t)
			rpc.gasErr = tt.err
			hash, _, err := c.Confirm(t.Context(), testIntent(), 0, nil, nil)
			require.Empty(t, hash)
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, tt.reverted, errors.Is(err, ErrEstimationReverted))
			require.Empty(t, rpc.sent)
		})
	}
}

type rpcCodeError struct {
	code int
	msg  string
}

func (e rpcCodeError) Error() string  { return e.msg }
func (e rpcCodeError) ErrorCode() int { return e.code }

// trackedTx reports whether the gas metrics still hold txHash.
func trackedTx(chainID, txHash string) bool {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	_, ok := metrics.txOwners[transactionKey{chainID: chainID, txHash: txHash}]
	return ok
}

func TestConfirmCountsGasWhenReservationDroppedDuringWait(t *testing.T) {
	c, rpc := newSubmitter(t)
	polled := make(chan struct{}, 1)
	var mined atomic.Bool
	rpc.receiptFor = func(h common.Hash) (*types.Receipt, error) {
		select {
		case polled <- struct{}{}:
		default:
		}
		if mined.Load() {
			return &types.Receipt{
				Status: types.ReceiptStatusSuccessful, TxHash: h, GasUsed: 21_000, EffectiveGasPrice: big.NewInt(7),
			}, nil
		}
		return nil, ethereum.NotFound
	}
	before := walletCost(c)

	type result struct {
		hash    string
		receipt *types.Receipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		hash, receipt, err := c.Confirm(t.Context(), testIntent(), 10*time.Second, nil, nil)
		done <- result{hash, receipt, err}
	}()
	<-polled // Confirm broadcast and is waiting for the receipt.

	// The transfer is mined, and a relayer submission drops the reservation
	// before Confirm's next poll.
	rpc.mined = 1
	require.NoError(t, submit(t, c))
	c.mu.Lock()
	require.Empty(t, c.confirmReserved)
	c.mu.Unlock()
	mined.Store(true)

	res := <-done
	require.NoError(t, res.err)
	require.NotNil(t, res.receipt)
	require.Equal(t, big.NewInt(21_000*7), new(big.Int).Sub(walletCost(c), before),
		"a receipt seen within the wait is counted")
	require.False(t, trackedTx(c.chainID, res.hash))
}

func TestConfirmCountsGasWhenOriginalMinesAfterReplacement(t *testing.T) {
	c, rpc := newSubmitter(t)
	polled := make(chan struct{}, 1)
	var minedHash atomic.Value
	rpc.receiptFor = func(h common.Hash) (*types.Receipt, error) {
		select {
		case polled <- struct{}{}:
		default:
		}
		if m, ok := minedHash.Load().(common.Hash); ok && m == h {
			return &types.Receipt{
				Status: types.ReceiptStatusSuccessful, TxHash: h, GasUsed: 21_000, EffectiveGasPrice: big.NewInt(7),
			}, nil
		}
		return nil, ethereum.NotFound
	}
	before := walletCost(c)

	type result struct {
		hash string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		hash, _, err := c.Confirm(t.Context(), testIntent(), 10*time.Second, nil, nil)
		done <- result{hash, err}
	}()
	<-polled // Confirm broadcast and is waiting for the receipt.

	// The transfer stalls and is re-signed, then the original is mined.
	c.mu.Lock()
	expireNonce(c)
	c.mu.Unlock()
	require.NoError(t, submit(t, c))
	original := rpc.sent[0].Hash()
	require.Equal(t, uint64(0), rpc.sent[1].Nonce())
	require.NotEqual(t, original, rpc.sent[1].Hash())
	minedHash.Store(original)

	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, original.Hex(), res.hash)
	require.Equal(t, big.NewInt(21_000*7), new(big.Int).Sub(walletCost(c), before),
		"the replaced hash is counted when it is the one mined")
	for _, tx := range rpc.sent[:2] {
		require.False(t, trackedTx(c.chainID, tx.Hash().Hex()))
	}
}

func TestReplacementsReportedInSigningOrder(t *testing.T) {
	c, rpc := newSubmitter(t)
	release := make(chan struct{})
	got := make(chan [2]string, 2)
	var calls atomic.Int32
	first, _, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, func(previous, replacement string) {
		// Hold the first report until the second replacement is signed.
		if calls.Add(1) == 1 {
			<-release
		}
		got <- [2]string{previous, replacement}
	})
	require.Error(t, err)

	expireNonce(c)
	require.NoError(t, submit(t, c))
	expireNonce(c)
	require.NoError(t, submit(t, c))
	var replacements []string
	for _, tx := range rpc.sent {
		if tx.Nonce() == 0 && tx.Hash().Hex() != first {
			replacements = append(replacements, tx.Hash().Hex())
		}
	}
	require.Len(t, replacements, 2)
	close(release)

	for _, want := range [][2]string{{first, replacements[0]}, {replacements[0], replacements[1]}} {
		select {
		case report := <-got:
			require.Equal(t, want, report)
		case <-time.After(2 * time.Second):
			t.Fatal("replacement not reported")
		}
	}
}

// walletCost is the cumulative gas cost the metrics hold for c's wallet.
func walletCost(c *TxSubmitter) *big.Int {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	w := metrics.wallets[chainWallet{chainID: c.chainID, wallet: c.address.Hex()}]
	return new(big.Int).Set(w.cumulativeGasCostWei)
}

// cancellingSigner signs, then cancels the caller's context, as an MCP client
// that disconnects between signing and broadcast.
type cancellingSigner struct {
	signer.Signer
	cancel context.CancelFunc
}

func (s cancellingSigner) Sign(ctx context.Context, digest []byte) ([]byte, error) {
	sig, err := s.Signer.Sign(ctx, digest)
	s.cancel()
	return sig, err
}

func TestConfirmCancelledBeforeBroadcastDoesNotReserveNonce(t *testing.T) {
	c, rpc := newSubmitter(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.signer = cancellingSigner{Signer: c.signer, cancel: cancel}
	hash, receipt, err := c.Confirm(ctx, testIntent(), time.Second, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, hash, "nothing was sent, so there is no hash to report")
	require.Nil(t, receipt)
	require.Empty(t, rpc.sent)
	require.Zero(t, c.nextNonce)
	require.Empty(t, c.confirmReserved)
}

// cancelOnSendRPC cancels the caller's context while SendTransaction runs and
// records whether the send's own context ended with it.
type cancelOnSendRPC struct {
	*testRPC
	cancel  context.CancelFunc
	sendErr error
}

func (r *cancelOnSendRPC) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	r.cancel()
	r.sendErr = ctx.Err()
	return r.testRPC.SendTransaction(ctx, tx)
}

func TestBroadcastOutlivesCallerCancel(t *testing.T) {
	c, rpc := newSubmitter(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wrapped := &cancelOnSendRPC{testRPC: rpc, cancel: cancel}
	c.eth = wrapped
	submission, err := c.Submit(ctx, testIntent())
	require.NoError(t, err)
	require.NotNil(t, submission)
	require.NoError(t, wrapped.sendErr, "a cancel mid-request must not cut the broadcast")
}

func TestStallReplacementOutbidsTransferAfterLowerNoncesMine(t *testing.T) {
	c, rpc := newSubmitter(t)
	require.NoError(t, submit(t, c))
	require.NoError(t, submit(t, c))
	hash, _, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, nil)
	require.Error(t, err)
	original := rpc.sent[2]
	require.Equal(t, uint64(2), original.Nonce())

	// Nonces 0 and 1 mine, which clears lastAttemptedFeeCap. The fee estimate
	// is unchanged.
	rpc.mined = 2
	relay := testIntent()
	relay.Data = []byte{9}
	require.NoError(t, submitIntent(t, c, relay))
	expireNonce(c)
	require.NoError(t, submitIntent(t, c, relay))

	var replacement *types.Transaction
	for _, tx := range rpc.sent[3:] {
		if tx.Nonce() == 2 {
			replacement = tx
		}
	}
	require.NotNil(t, replacement)
	require.NotEqual(t, hash, replacement.Hash().Hex())
	require.Equal(t, 1, replacement.GasFeeCap().Cmp(original.GasFeeCap()))
	require.Equal(t, 1, replacement.GasTipCap().Cmp(original.GasTipCap()))
	require.Equal(t, []string{hash, replacement.Hash().Hex()}, c.confirmReserved[2].hashes)
}

func TestIdenticalStallReplacementIsNotReported(t *testing.T) {
	c, rpc := newSubmitter(t)
	// No base fee: fee cap equals tip, 8 wei.
	rpc.baseFee, rpc.tip = 0, 8
	var replaced atomic.Int32
	hash, _, err := c.Confirm(t.Context(), testIntent(), 30*time.Millisecond, nil, func(string, string) {
		replaced.Add(1)
	})
	require.Error(t, err)

	// A fresh estimate of 2 wei caps the bid at 4x, 8 wei: the same transaction.
	rpc.tip = 2
	expireNonce(c)
	relay := testIntent()
	relay.Data = []byte{9}
	require.NoError(t, submitIntent(t, c, relay))
	require.Len(t, rpc.sent, 3)
	require.Equal(t, hash, rpc.sent[1].Hash().Hex(), "the re-signed transfer is identical")
	require.Equal(t, uint64(1), rpc.sent[2].Nonce())
	require.Equal(t, []string{hash}, c.confirmReserved[0].hashes)
	require.Less(t, time.Since(c.lowestUnminedNoncePendingSince), c.opts.PendingTxTimeout,
		"the stall timer restarts")
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, replaced.Load(), "an identical transaction is not a replacement")
}
