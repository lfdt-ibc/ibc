// SPDX-License-Identifier: Apache-2.0

// Package evm signs and broadcasts EVM transactions.
package evm

import (
	"context"
	"log/slog"
	"math/big"
	"slices"
	"sync"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"

	chainsevm "github.com/lfdt-ibc/ibc/cli/internal/chains/evm"
	"github.com/lfdt-ibc/ibc/cli/internal/service/signer"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

// Transaction limits.
const (
	DefaultPendingTxTimeout = 2 * time.Minute
	// requestTimeout bounds Submit and ShouldRetry.
	requestTimeout = 10 * time.Second
	// maxInFlight limits outstanding nonces per sender; stalled nonces can still be reused.
	maxInFlight = 16
	// maxBroadcastAttempts limits broadcasts per Submit, including fee replacements.
	maxBroadcastAttempts = 8
	// feeCeilingMultiplier caps fees relative to the fresh estimate for each Submit.
	feeCeilingMultiplier = 4
	// receiptPollInterval doubles up to maxReceiptPollInterval (about block
	// time) so a long Confirm wait does not drain the relayer's RPC quota.
	receiptPollInterval    = 200 * time.Millisecond
	maxReceiptPollInterval = 2 * time.Second
)

// ErrBusy leaves the packet available for a later dispatch.
var ErrBusy = errors.New("transaction submission capacity reached")

// ChainOptions controls transaction retry timing.
type ChainOptions struct {
	// PendingTxTimeout sets the wait before retrying a missing transaction or reusing a stalled nonce.
	PendingTxTimeout time.Duration
}

// ETHClient go-ethereum methods used by the EVM tx submitter.
type ETHClient interface {
	BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	SuggestGasTipCap(context.Context) (*big.Int, error)
	PendingCodeAt(context.Context, common.Address) ([]byte, error)
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
	EstimateGasAtBlock(context.Context, ethereum.CallMsg, *big.Int) (uint64, error)
	NonceAt(context.Context, common.Address, *big.Int) (uint64, error)
	PendingNonceAt(context.Context, common.Address) (uint64, error)
	SendTransaction(context.Context, *types.Transaction) error
	TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error)
}

// TxSubmitter simulates, signs, and broadcasts transactions on one EVM chain.
type TxSubmitter struct {
	chainID                        string
	eth                            ETHClient
	signer                         signer.Signer
	address                        common.Address
	ethSigner                      types.Signer
	opts                           ChainOptions
	mu                             sync.Mutex
	nextNonce                      uint64
	lowestUnminedNonceHeight       uint64
	lowestUnminedNonce             uint64
	lowestUnminedNoncePendingSince time.Time
	lastAttemptedFeeCap            *big.Int
	lastAttemptedTipCap            *big.Int
	// confirmReserved holds Confirm's nonces, kept from later intents while
	// still pending; a stalled one is re-signed at higher fees.
	confirmReserved map[uint64]*reservation
}

// reservation is the transfer Confirm broadcast at one nonce.
type reservation struct {
	intent v2.TxIntent
	// hashes are the transactions signed for intent at this nonce, oldest first.
	// A stall replacement appends one.
	hashes []string
	// feeCap and tipCap are the fees of the newest hash. A stall replacement
	// bids above them, or it would re-sign the same transaction.
	feeCap, tipCap *big.Int
	// onReplace, when set, is told each replacement hash, in signing order. It
	// runs without c.mu and can fire after Confirm has returned.
	onReplace ReplaceFunc
	// notices queue replacements for onReplace; notifying is set while one
	// deliverReplacements goroutine drains them in signing order.
	notices   [][2]string
	notifying bool
	// waiting is set during Confirm's receipt wait; gas metrics keep the hashes
	// until it ends so the mined one is counted even if the reservation drops.
	waiting bool
}

// ReplaceFunc receives the previous and the new hash when a Confirm transfer
// is re-signed at the same nonce. Only one of them can be mined.
type ReplaceFunc func(previous, replacement string)

func NewFromRPC(chainID, rpcURL string, chainSigner signer.Signer, opts ChainOptions) (*TxSubmitter, error) {
	eth, err := chainsevm.Dial(chainID, rpcURL)
	if err != nil {
		return nil, err
	}
	return New(chainID, eth, chainSigner, opts)
}

func New(chainID string, eth ETHClient, chainSigner signer.Signer, opts ChainOptions) (*TxSubmitter, error) {
	if opts.PendingTxTimeout == 0 {
		opts.PendingTxTimeout = DefaultPendingTxTimeout
	}
	if opts.PendingTxTimeout <= 0 {
		return nil, errors.New("invalid pending transaction timeout")
	}
	id, ok := new(big.Int).SetString(chainID, 10)
	if !ok || id.Sign() <= 0 {
		return nil, errors.Errorf("invalid EVM chain ID %q", chainID)
	}
	if chainSigner.Type() != signer.ECDSA {
		return nil, errors.New("EVM transactions require an ECDSA signer")
	}
	pub, err := crypto.DecompressPubkey(chainSigner.PublicKey())
	if err != nil {
		return nil, errors.Wrapf(err, "decompressing signer public key for chain %q", chainID)
	}
	c := &TxSubmitter{
		chainID: chainID, eth: eth, signer: chainSigner,
		address: crypto.PubkeyToAddress(*pub), ethSigner: types.LatestSignerForChainID(id), opts: opts,
	}
	metrics.setWallet(chainID, c.address.Hex(), eth)
	return c, nil
}

// Address is the checksummed address this submitter signs from.
func (c *TxSubmitter) Address() string { return c.address.Hex() }

func (c *TxSubmitter) Submit(ctx context.Context, intent v2.TxIntent) (_ *v2.Submission, err error) {
	defer func() {
		metrics.attempt(ctx, c.chainID, c.address.Hex(), err)
		if err != nil {
			level := slog.LevelWarn
			if errors.Is(err, ErrBusy) {
				level = slog.LevelInfo
			}
			slog.Log(ctx, level, "Transaction submission unsuccessful",
				"chainID", c.chainID, "sender", c.address.Hex(), "err", err.Error())
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	// The relayer retries a lost broadcast itself. Do not reserve the nonce here:
	// the next attempt reuses it unless the node already reports it pending.
	sub, _, err := c.submitLocked(ctx, intent, false)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Confirm broadcasts intent at a reserved nonce (onBroadcast gets the hash), awaits it off c.mu
// and re-signs stalls (onReplace). A hash with an error may have landed: report it.
func (c *TxSubmitter) Confirm(
	ctx context.Context, intent v2.TxIntent, confirmTimeout time.Duration,
	onBroadcast func(hash string), onReplace ReplaceFunc,
) (hash string, receipt *types.Receipt, err error) {
	defer func() {
		metrics.attempt(ctx, c.chainID, c.address.Hex(), err)
		if err != nil && hash == "" {
			slog.WarnContext(ctx, "Transaction submission unsuccessful",
				"chainID", c.chainID, "sender", c.address.Hex(), "err", err.Error())
		}
	}()
	c.mu.Lock()

	// A failed wait restarts the stall timer noteAccepted may move, so the next
	// submission does not instantly replace a transfer still in flight.
	pendingSince := c.lowestUnminedNoncePendingSince
	sub, nonce, submitErr := c.submitLocked(ctx, intent, true)
	if sub == nil {
		c.mu.Unlock()
		return "", nil, submitErr
	}
	hash = sub.TxHash
	// Hold the reservation itself: selectNonce may delete the map entry before
	// this wait sees a mined replacement.
	reserved := c.confirmReserved[nonce]
	if reserved != nil {
		reserved.onReplace = onReplace
		reserved.waiting = true
	}
	restartStallTimer := nonce == c.lowestUnminedNonce && !c.lowestUnminedNoncePendingSince.Equal(pendingSince)
	if confirmTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, confirmTimeout)
		defer cancel()
	}
	// Other submissions can use later nonces while this receipt is outstanding.
	// confirmReserved keeps them off this nonce until the node reports it free.
	c.mu.Unlock()
	if onBroadcast != nil {
		onBroadcast(hash)
	}
	hash, receipt, err = c.waitReceipt(ctx, reserved, hash)
	c.mu.Lock()
	defer c.mu.Unlock()
	// Count the mined hash, then forget the rest of the group. A transfer that
	// mines after this wait is not counted.
	if err == nil {
		metrics.endTx(c.chainID, receipt)
	}
	c.forgetWaitedHashes(reserved, hash)

	if err != nil {
		if restartStallTimer && c.lowestUnminedNonce == nonce {
			c.lowestUnminedNoncePendingSince = time.Now()
		}
		if submitErr != nil {
			err = errors.Wrap(submitErr, err.Error())
		}
		slog.WarnContext(ctx, "Transaction broadcast but not confirmed",
			"chainID", c.chainID, "sender", c.address.Hex(), "txHash", hash)
		return hash, nil, err
	}
	return hash, receipt, nil
}

// submitLocked signs and broadcasts; caller holds c.mu. With reserveUnconfirmed,
// a broadcast that may have landed is reserved and returned with its error.
func (c *TxSubmitter) submitLocked(
	ctx context.Context, intent v2.TxIntent, reserveUnconfirmed bool,
) (*v2.Submission, uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	nonce, head, err := c.selectNonce(ctx, time.Now())
	if err != nil {
		return nil, 0, err
	}
	if r, ok := c.confirmReserved[nonce]; ok {
		// A reserved nonce here means a stall: replace it and take the next
		// nonce, unless it now reverts, in which case intent takes its nonce.
		if c.replaceStalledLocked(ctx, nonce, head, r) {
			if nonce, head, err = c.selectNonce(ctx, time.Now()); err != nil {
				return nil, 0, err
			}
		}
	}
	return c.broadcastLocked(ctx, intent, nonce, head, reserveUnconfirmed)
}

// replaceStalledLocked outbids a stalled Confirm transfer so it cannot block later nonces; caller
// holds c.mu. Returns false, dropping the reservation and freeing nonce, if it now reverts.
func (c *TxSubmitter) replaceStalledLocked(
	ctx context.Context, nonce uint64, head *types.Header, r *reservation,
) (keepNonce bool) {
	previous := r.hashes[len(r.hashes)-1]
	sub, _, err := c.broadcastLocked(ctx, r.intent, nonce, head, true)
	if sub == nil && errors.Is(err, ErrEstimationReverted) {
		c.dropReservation(nonce)
		slog.WarnContext(ctx, "Stalled confirmed transaction now reverts; its nonce goes to the next transaction",
			"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce, "txHash", previous, "err", err)
		return false
	}
	if sub == nil {
		// Try again after another PendingTxTimeout. The pending transfer keeps
		// the nonce meanwhile.
		c.lowestUnminedNoncePendingSince = time.Now()
		slog.WarnContext(ctx, "Stalled confirmed transaction not replaced",
			"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce, "txHash", previous, "err", err)
		return true
	}
	if sub.TxHash == previous {
		// The fee ceiling produced an identical transaction. Retry after another
		// PendingTxTimeout, when a fresh estimate may allow a higher bid.
		c.lowestUnminedNoncePendingSince = time.Now()
		slog.WarnContext(ctx, "Stalled confirmed transaction not replaced: fee ceiling reached",
			"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce, "txHash", previous)
		return true
	}
	// previous stays in the gas metrics: it can still be the one mined, and
	// Confirm's wait or dropReservation forgets every hash of the group.
	slog.WarnContext(ctx, "Stalled confirmed transaction replaced with higher fees",
		"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce,
		"replacedTxHash", previous, "txHash", sub.TxHash)
	return true
}

// broadcastLocked signs and broadcasts intent at nonce, bumping fees while the
// replacement is underpriced; caller holds c.mu.
func (c *TxSubmitter) broadcastLocked(
	ctx context.Context, intent v2.TxIntent, nonce uint64, head *types.Header, reserveUnconfirmed bool,
) (*v2.Submission, uint64, error) {
	fee, tip, err := estimateFees(ctx, c.eth, head.BaseFee)
	if err != nil {
		return nil, 0, err
	}
	feeCeiling := new(big.Int).Mul(fee, big.NewInt(feeCeilingMultiplier))
	if nonce == c.lowestUnminedNonce && c.lastAttemptedFeeCap != nil {
		fee, tip = maxBig(fee, bump(c.lastAttemptedFeeCap)), maxBig(tip, bump(c.lastAttemptedTipCap))
	}
	// A stall replacement must outbid its predecessor: lastAttemptedFeeCap may
	// have been reset, and an unchanged estimate would re-sign identical bytes.
	if r, ok := c.confirmReserved[nonce]; ok && r.feeCap != nil {
		fee, tip = maxBig(fee, bump(r.feeCap)), maxBig(tip, bump(r.tipCap))
	}
	fee, tip = capFees(fee, tip, feeCeiling)
	txData, err := c.prepareTransaction(ctx, intent.To, intent.Data, nonce, fee, tip)
	if err != nil {
		return nil, 0, err
	}
	for range maxBroadcastAttempts {
		txData.GasFeeCap, txData.GasTipCap = fee, tip
		tx, err := signTransaction(ctx, types.NewTx(txData), c.signer, c.ethSigner, c.address)
		if err != nil {
			return nil, 0, err
		}
		if err := ctx.Err(); err != nil {
			// Nothing was sent, so the nonce is not reserved. broadcast itself
			// is detached from ctx.
			return nil, 0, errors.Wrapf(err, "broadcasting transaction %s at nonce %d", tx.Hash(), nonce)
		}
		submittedAt := time.Now().UTC()
		err = broadcast(ctx, c.eth, tx)
		if err != nil {
			slog.WarnContext(ctx, "Transaction broadcast rejected or unconfirmed",
				append(c.transactionAttributes(tx, head.BaseFee), "err", chainsevm.ExplainRevert(err).Error())...)
		}
		if !isReplacementUnderpriced(err) {
			// A broadcast that may have landed is recorded like an accepted one
			// and returned together with its error.
			if err != nil {
				landed := reserveUnconfirmed && broadcastMayHaveLanded(err)
				err = errors.Wrapf(err, "broadcasting transaction %s at nonce %d", tx.Hash(), nonce)
				if !landed {
					return nil, 0, err
				}
			}
			c.noteAccepted(nonce, tx, intent, reserveUnconfirmed)
			if err == nil {
				slog.InfoContext(ctx, "Transaction submitted", c.transactionAttributes(tx, head.BaseFee)...)
			}
			return &v2.Submission{
				TxHash:         tx.Hash().Hex(),
				SubmittedAt:    submittedAt,
				RelayerAddress: c.address.Hex(),
			}, nonce, err
		}
		if nonce == c.lowestUnminedNonce {
			c.lastAttemptedFeeCap, c.lastAttemptedTipCap = fee, tip
		}
		nextFee, nextTip := capFees(bump(fee), bump(tip), feeCeiling)
		if nextFee.Cmp(fee) == 0 && nextTip.Cmp(tip) == 0 {
			slog.WarnContext(
				ctx,
				"Transaction replacement fee ceiling reached",
				append(
					c.transactionAttributes(tx, head.BaseFee),
					"feeCeilingWei",
					feeCeiling.String(),
					"err",
					err.Error(),
				)...)
			return nil, 0, errors.Wrapf(err,
				"fee ceiling %s wei (%dx fresh estimate) reached for nonce %d",
				feeCeiling, feeCeilingMultiplier, nonce)
		}
		fee, tip = nextFee, nextTip
	}
	return nil, 0, errors.Errorf(
		"replacement still underpriced after %d attempts at nonce %d",
		maxBroadcastAttempts,
		nonce,
	)
}

// noteAccepted records a possibly-accepted broadcast and tracks its gas; caller
// holds c.mu. With reserve, the nonce stays Confirm's until mined or free.
func (c *TxSubmitter) noteAccepted(nonce uint64, tx *types.Transaction, intent v2.TxIntent, reserve bool) {
	hash, fee, tip := tx.Hash().Hex(), tx.GasFeeCap(), tx.GasTipCap()
	c.nextNonce = max(c.nextNonce, nonce+1)
	if reserve {
		if c.confirmReserved == nil {
			c.confirmReserved = make(map[uint64]*reservation)
		}
		r, ok := c.confirmReserved[nonce]
		switch {
		case !ok:
			c.confirmReserved[nonce] = &reservation{intent: intent, hashes: []string{hash}, feeCap: fee, tipCap: tip}
		case r.hashes[len(r.hashes)-1] != hash:
			previous := r.hashes[len(r.hashes)-1]
			r.hashes = append(r.hashes, hash)
			r.feeCap, r.tipCap = fee, tip
			if r.onReplace != nil {
				r.notices = append(r.notices, [2]string{previous, hash})
				if !r.notifying {
					// Off the signer lock: the callback may write the audit log.
					r.notifying = true
					go c.deliverReplacements(r)
				}
			}
		}
	}
	if nonce == c.lowestUnminedNonce {
		c.lastAttemptedFeeCap, c.lastAttemptedTipCap = fee, tip
		c.lowestUnminedNoncePendingSince = time.Now()
	}
	metrics.startTx(c.chainID, c.address.Hex(), hash)
}

// deliverReplacements drains r's queue to onReplace in signing order; one
// goroutine per reservation so B→C never arrives before A→B.
func (c *TxSubmitter) deliverReplacements(r *reservation) {
	for {
		c.mu.Lock()
		if len(r.notices) == 0 {
			r.notifying = false
			c.mu.Unlock()
			return
		}
		n, onReplace := r.notices[0], r.onReplace
		r.notices = r.notices[1:]
		c.mu.Unlock()
		onReplace(n[0], n[1])
	}
}

// dropReservation deletes the reservation at nonce and its gas-metric hashes,
// unless Confirm is still waiting on them; caller holds c.mu.
func (c *TxSubmitter) dropReservation(nonce uint64) {
	r, ok := c.confirmReserved[nonce]
	if !ok {
		return
	}
	if !r.waiting {
		for _, h := range r.hashes {
			metrics.forgetTx(c.chainID, h)
		}
	}
	delete(c.confirmReserved, nonce)
}

// forgetWaitedHashes ends Confirm's wait on r and forgets hash and r's hashes
// in the gas metrics; caller holds c.mu.
func (c *TxSubmitter) forgetWaitedHashes(r *reservation, hash string) {
	metrics.forgetTx(c.chainID, hash)
	if r == nil {
		return
	}
	r.waiting = false
	for _, h := range r.hashes {
		metrics.forgetTx(c.chainID, h)
	}
}

// waitReceipt polls until first or a replacement on r (may be nil) is mined,
// returning that hash or else the latest. Called without c.mu.
func (c *TxSubmitter) waitReceipt(
	ctx context.Context, r *reservation, first string,
) (string, *types.Receipt, error) {
	var last error
	known := []string{first}
	interval := receiptPollInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		known = c.replacementHashes(r, known)
		for _, hash := range known {
			receipt, err := c.eth.TransactionReceipt(ctx, common.HexToHash(hash))
			switch {
			case err == nil && receipt != nil:
				return hash, receipt, nil
			case err == nil:
				last = errors.New("empty receipt")
			default:
				last = err
			}
		}
		select {
		case <-ctx.Done():
			latest := known[len(known)-1]
			if last != nil {
				return latest, nil, last
			}
			return latest, nil, ctx.Err()
		case <-timer.C:
		}
		interval = min(2*interval, maxReceiptPollInterval)
		timer.Reset(interval)
	}
}

// replacementHashes returns a copy of r's hashes, under c.mu, if replacements
// have grown it past known.
func (c *TxSubmitter) replacementHashes(r *reservation, known []string) []string {
	if r == nil {
		return known
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(r.hashes) <= len(known) {
		return known
	}
	return slices.Clone(r.hashes)
}

func (c *TxSubmitter) transactionAttributes(tx *types.Transaction, baseFee *big.Int) []any {
	return []any{
		"chainID", c.chainID, "txHash", tx.Hash().Hex(), "sender", c.address.Hex(),
		"nonce", tx.Nonce(), "gasLimit", tx.Gas(),
		"maxFeePerGasWei", tx.GasFeeCap().String(), "maxPriorityFeePerGasWei", tx.GasTipCap().String(),
		"baseFeePerGasWei", baseFee.String(), "nextUnminedNonce", c.lowestUnminedNonce,
	}
}

func (c *TxSubmitter) selectNonce(ctx context.Context, now time.Time) (uint64, *types.Header, error) {
	head, err := c.eth.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, nil, errors.Wrap(err, "getting latest header for nonce selection")
	}
	height := head.Number.Uint64()
	if height < c.lowestUnminedNonceHeight {
		return 0, nil, errors.Errorf(
			"RPC block height %d is behind last nonce observation at %d",
			height,
			c.lowestUnminedNonceHeight,
		)
	}
	mined, err := c.eth.NonceAt(ctx, c.address, head.Number)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "getting mined nonce for %s at block %s", c.address, head.Number)
	}
	pending, err := c.eth.PendingNonceAt(ctx, c.address)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "getting pending nonce for %s", c.address)
	}
	c.lowestUnminedNonceHeight = height
	if mined != c.lowestUnminedNonce {
		c.lowestUnminedNonce, c.lowestUnminedNoncePendingSince = mined, time.Time{}
		c.lastAttemptedFeeCap, c.lastAttemptedTipCap = nil, nil
	}
	for reserved := range c.confirmReserved {
		if reserved < mined {
			c.dropReservation(reserved)
		}
	}
	c.nextNonce = max(c.nextNonce, mined, pending)
	if c.nextNonce > mined {
		if c.lowestUnminedNoncePendingSince.IsZero() {
			c.lowestUnminedNoncePendingSince = now
		}
		// After the stall timeout, reuse the lowest unmined nonce. A Confirm transfer
		// still pending keeps its reservation (submitLocked re-signs it); otherwise it is dropped.
		if now.Sub(c.lowestUnminedNoncePendingSince) >= c.opts.PendingTxTimeout {
			if pending <= mined {
				c.dropReservation(mined)
			}
			return mined, head, nil
		}
	}
	if c.nextNonce-mined >= maxInFlight {
		return 0, nil, ErrBusy
	}
	return c.nextNonce, head, nil
}

// ShouldRetry leaves missing transactions pending until the retry interval expires.
func (c *TxSubmitter) ShouldRetry(ctx context.Context, txHash string, sentAt time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	receipt, err := c.eth.TransactionReceipt(ctx, common.HexToHash(txHash))
	switch {
	case errors.Is(err, ethereum.NotFound):
		if time.Since(sentAt) >= c.opts.PendingTxTimeout {
			metrics.forgetTx(c.chainID, txHash)
			return true, nil
		}
		return false, v2.ErrTxNotFound
	case err != nil:
		return false, errors.Wrapf(err, "getting receipt for tx %s", txHash)
	default:
		metrics.endTx(c.chainID, receipt)
		return receipt.Status != types.ReceiptStatusSuccessful, nil
	}
}
