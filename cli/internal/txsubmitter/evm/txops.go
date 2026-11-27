// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"context"
	"io"
	"math/big"
	"net"
	"net/url"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"

	chainsevm "github.com/lfdt-ibc/ibc/cli/internal/chains/evm"
	"github.com/lfdt-ibc/ibc/cli/internal/service/signer"
)

// ErrEstimationReverted marks an unbroadcast intent whose gas estimate reverted.
// It is not transient: it reverts again until chain state changes.
var ErrEstimationReverted = errors.New("transaction reverts in simulation")

// estimationReverted keeps the RPC error in the chain and adds
// ErrEstimationReverted to it without changing the message.
type estimationReverted struct{ error }

func estimateFees(
	ctx context.Context,
	eth ETHClient,
	baseFee *big.Int,
) (fee, tip *big.Int, err error) {
	if baseFee == nil {
		return nil, nil, errors.New("chain must support EIP-1559")
	}
	tip, err = eth.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting suggested gas tip cap")
	}
	fee = new(big.Int).Add(tip, new(big.Int).Mul(baseFee, big.NewInt(2)))
	return fee, tip, nil
}

func capFees(fee, tip, limit *big.Int) (*big.Int, *big.Int) {
	if fee.Cmp(limit) > 0 {
		fee = limit
	}
	if tip.Cmp(fee) > 0 {
		tip = fee
	}
	return fee, tip
}

func signTransaction(
	ctx context.Context,
	tx *types.Transaction,
	chainSigner signer.Signer,
	ethSigner types.Signer,
	address common.Address,
) (*types.Transaction, error) {
	sig, err := chainSigner.Sign(ctx, ethSigner.Hash(tx).Bytes())
	if err != nil {
		return nil, errors.Wrapf(err, "signing transaction with address %s", address)
	}
	signed, err := tx.WithSignature(ethSigner, sig)
	if err != nil {
		return nil, errors.Wrap(err, "attaching transaction signature")
	}
	sender, err := types.Sender(ethSigner, signed)
	if err != nil {
		return nil, errors.Wrap(err, "recovering transaction sender")
	}
	if sender != address {
		return nil, errors.New("transaction signature does not match configured sender")
	}
	return signed, nil
}

// broadcast detaches from ctx so a mid-flight cancel cannot leave it unknown
// whether the node has tx; broadcastLocked checks ctx just before.
func broadcast(ctx context.Context, eth ETHClient, tx *types.Transaction) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
	defer cancel()
	err := eth.SendTransaction(ctx, tx)
	if err != nil {
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "already known") || strings.HasPrefix(message, "known transaction") {
			return nil
		}
	}
	return err
}

func isReplacementUnderpriced(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "replacement transaction underpriced")
}

// broadcastMayHaveLanded reports whether err (timeout, cancel, EOF, I/O, 5xx, post-stream GOAWAY)
// may follow the node accepting tx, unlike a dial or a TLS failure before the body.
func broadcastMayHaveLanded(err error) bool {
	if err == nil {
		return false
	}
	// Not a net.Error and does not unwrap, so match the text on the outer
	// error (covers a *url.Error wrap).
	if http2ServerClosedConnection(err) {
		return true
	}
	// A dial timeout is still a dial: the signed transaction was not written.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var httpErr rpc.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode >= 500
	}
	// *url.Error is a net.Error: judge its cause first, or a handshake failure
	// would look like a broadcast that may have landed.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return broadcastMayHaveLanded(urlErr.Err)
	}
	// addTLS returns these before the request body is written. A later alert
	// on a connection that already accepted bytes is wrapped by net/http.
	if !transportConnectionBroken(err) && tlsFailedBeforeBody(err) {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// transportConnectionBroken reports a broken HTTP/1 connection that may have
// already written the request.
func transportConnectionBroken(err error) bool {
	return err != nil && strings.Contains(err.Error(), "net/http: HTTP/1.x transport connection broken")
}

// http2ServerClosedConnection reports a GOAWAY after the server received the
// stream; a graceful-shutdown GOAWAY (not received, auto-retried) does not match.
func http2ServerClosedConnection(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http2: server sent GOAWAY and closed the connection")
}

// tlsFailedBeforeBody reports a TLS handshake timeout (matched by text; the type
// is unexported) or a crypto/tls alert, both raised before the body is sent.
func tlsFailedBeforeBody(err error) bool {
	for cur := err; cur != nil; {
		if cur.Error() == "net/http: TLS handshake timeout" {
			return true
		}
		next := errors.Unwrap(cur)
		if next == nil || next == cur {
			break
		}
		cur = next
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && (opErr.Op == "remote error" || opErr.Op == "local error")
}

func (c *TxSubmitter) prepareTransaction(
	ctx context.Context,
	destination string,
	data []byte,
	nonce uint64,
	fee, tip *big.Int,
) (*types.DynamicFeeTx, error) {
	if !common.IsHexAddress(destination) {
		return nil, errors.New("invalid transaction destination")
	}
	to := common.HexToAddress(destination)
	code, err := c.eth.PendingCodeAt(ctx, to)
	if err != nil {
		return nil, errors.Wrapf(err, "getting code at %s", to)
	}
	if len(code) == 0 {
		return nil, errors.Errorf("no contract code at %s", to)
	}
	// Pending state may already include the transaction this submission replaces.
	gas, err := c.eth.EstimateGasAtBlock(
		ctx,
		ethereum.CallMsg{From: c.address, To: &to, Data: data, GasFeeCap: fee, GasTipCap: tip},
		nil,
	)
	if err != nil {
		explained := chainsevm.ExplainRevert(err)
		if isRevert(err) {
			explained = estimationReverted{explained}
		}
		return nil, errors.Wrap(explained, "estimating gas")
	}
	return &types.DynamicFeeTx{
		ChainID:   c.ethSigner.ChainID(),
		Nonce:     nonce,
		To:        &to,
		Data:      data,
		Gas:       gas,
		GasFeeCap: fee,
		GasTipCap: tip,
	}, nil
}

func (e estimationReverted) Unwrap() error { return e.error }

func (estimationReverted) Is(target error) bool { return target == ErrEstimationReverted }

// isRevert reports whether an eth_estimateGas error is an EVM revert: JSON-RPC
// code 3, or by message for Besu before 26.x, which uses -32000.
func isRevert(err error) bool {
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) && rpcErr.ErrorCode() == 3 {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "execution reverted")
}

// Round up by 20%, including when the previous tip was zero.
func bump(v *big.Int) *big.Int {
	out := new(big.Int).Mul(v, big.NewInt(120))
	out.Add(out, big.NewInt(99))
	out.Div(out, big.NewInt(100))
	return maxBig(out, new(big.Int).Add(v, big.NewInt(1)))
}

func maxBig(a, b *big.Int) *big.Int {
	if a.Cmp(b) >= 0 {
		return a
	}
	return b
}
