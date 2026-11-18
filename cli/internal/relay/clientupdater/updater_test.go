// SPDX-License-Identifier: Apache-2.0

package clientupdater_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/lfdt-ibc/ibc/cli/internal/relay/clientupdater"
	"github.com/lfdt-ibc/ibc/cli/internal/tests/mocks"
	"github.com/lfdt-ibc/ibc/cli/internal/txsubmitter"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

const interval = 20 * time.Millisecond

func TestUpdaterRetriesBeforeNextInterval(t *testing.T) {
	for _, failure := range []error{txsubmitter.ErrBusy, errors.New("temporary RPC failure")} {
		t.Run(failure.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prover := mocks.NewMockProver(t)
				prover.EXPECT().LatestProvableHeight(mock.Anything).Return(uint64(10), time.Now(), nil)
				prover.EXPECT().ClientUpdatePayloads(mock.Anything, uint64(10)).Return([][]byte{{1}}, nil)
				builder := mocks.NewMockTxBuilder(t)
				builder.EXPECT().BuildRelayTxs(mock.Anything, mock.Anything).
					Return([]v2.RelayTx{{To: []byte{1}, Data: []byte{1}}}, nil)
				submitter := mocks.NewMockTxSubmitter(t)
				submitter.EXPECT().SimulateRelayTx(mock.Anything, mock.Anything).Return(v2.RelaySimulation{
					Operations: []v2.OperationResult{{Type: v2.OperationTypeUpdate, Success: true}},
				}, nil)
				var attempts atomic.Int32
				submitter.EXPECT().Submit(mock.Anything, mock.Anything).RunAndReturn(
					func(context.Context, v2.TxIntent) (*v2.Submission, error) {
						attempt := attempts.Add(1)
						if attempt <= 7 || attempt == 9 {
							return nil, failure
						}
						return &v2.Submission{TxHash: "0x01", SubmittedAt: time.Now()}, nil
					})
				submitter.EXPECT().ShouldRetry(mock.Anything, "0x01", mock.Anything).Return(false, nil)
				updater := clientupdater.New("1", "client-0", time.Hour, prover, builder, submitter, slog.Default())
				updater.Start()
				defer updater.Stop()
				synctest.Wait()
				require.Equal(t, int32(1), attempts.Load())

				// Failures back off, capped at 30 seconds, rather than losing an hour.
				for i, delay := range []time.Duration{1, 2, 4, 8, 16, 30, 30} {
					time.Sleep(delay*time.Second - time.Nanosecond)
					synctest.Wait()
					require.Equal(t, int32(i+1), attempts.Load())
					time.Sleep(time.Nanosecond)
					synctest.Wait()
					require.Equal(t, int32(i+2), attempts.Load())
				}
				// Success restores the normal schedule and resets the retry backoff.
				time.Sleep(time.Hour - 91*time.Second)
				synctest.Wait()
				require.Equal(t, int32(9), attempts.Load())
				time.Sleep(time.Second)
				synctest.Wait()
				require.Equal(t, int32(10), attempts.Load())
				updater.Stop()
				time.Sleep(time.Hour)
				require.Equal(t, int32(10), attempts.Load())
			})
		})
	}
}

// An updater keeps updating on its interval with no packets involved, and a
// failed attempt does not stop the next one.
func TestUpdaterUpdatesOnInterval(t *testing.T) {
	var head atomic.Uint64
	head.Store(10)
	prover := mocks.NewMockProver(t)
	prover.EXPECT().LatestProvableHeight(mock.Anything).RunAndReturn(
		func(context.Context) (uint64, time.Time, error) {
			return head.Add(1), time.Now(), nil
		})
	prover.EXPECT().ClientUpdatePayloads(mock.Anything, uint64(11)).
		Return(nil, errors.New("counterparty unavailable")).Once()
	prover.EXPECT().ClientUpdatePayloads(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, height uint64) ([][]byte, error) {
			return [][]byte{{byte(height)}}, nil
		})

	txBuilder := mocks.NewMockTxBuilder(t)
	txBuilder.EXPECT().BuildRelayTxs(mock.Anything, mock.Anything).RunAndReturn(
		func(update v2.ClientUpdate, items []v2.PacketRelayItem) ([]v2.RelayTx, error) {
			require.Equal(t, "client-0", update.ClientID)
			require.Empty(t, items)
			return []v2.RelayTx{{To: []byte{1}, Data: update.Payloads[0]}}, nil
		})

	var mu sync.Mutex
	var submitted [][]byte
	txSubmitter := mocks.NewMockTxSubmitter(t)
	txSubmitter.EXPECT().SimulateRelayTx(mock.Anything, mock.Anything).Return(v2.RelaySimulation{
		Operations: []v2.OperationResult{{Type: v2.OperationTypeUpdate, Success: true}},
	}, nil)
	txSubmitter.EXPECT().Submit(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, intent v2.TxIntent) (*v2.Submission, error) {
			mu.Lock()
			defer mu.Unlock()
			submitted = append(submitted, intent.Data)
			return &v2.Submission{TxHash: "0x01", SubmittedAt: time.Now()}, nil
		})
	txSubmitter.EXPECT().ShouldRetry(mock.Anything, "0x01", mock.Anything).Return(false, nil)

	updater := clientupdater.New("1", "client-0", interval, prover, txBuilder, txSubmitter, slog.Default())
	updater.Start()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(submitted) >= 2
	}, time.Second, interval/4)
	updater.Stop()

	// the first attempt, at height 11, failed before submitting
	require.Equal(t, []byte{12}, submitted[0])
	require.Equal(t, []byte{13}, submitted[1])
}

// A client that already trusts the latest provable height is left alone.
func TestUpdaterSkipsTrustedHeight(t *testing.T) {
	var checks atomic.Int32
	prover := mocks.NewMockProver(t)
	prover.EXPECT().LatestProvableHeight(mock.Anything).RunAndReturn(
		func(context.Context) (uint64, time.Time, error) {
			checks.Add(1)
			return 10, time.Now(), nil
		})
	prover.EXPECT().ClientUpdatePayloads(mock.Anything, uint64(10)).Return(nil, nil)

	updater := clientupdater.New("1", "client-0", interval,
		prover, mocks.NewMockTxBuilder(t), mocks.NewMockTxSubmitter(t), slog.Default())
	updater.Start()
	require.Eventually(t, func() bool { return checks.Load() >= 2 }, time.Second, interval/4)
	updater.Stop()
}
