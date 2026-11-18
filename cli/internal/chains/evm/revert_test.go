// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"

	"github.com/cosmos/ibc-contracts/packages/go-abigen/attestation"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/besulightclient"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/ibcmulticall"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/ics26router"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/ift"
)

// overlappingRevert encodes bytes[] elements that all alias one payload.
func overlappingRevert(t testing.TB, count, elementSize int) []byte {
	t.Helper()
	contractABI, err := ics26router.ContractMetaData.GetAbi()
	require.NoError(t, err)
	customErr := contractABI.Errors["InvalidMerklePrefix"]
	data := make([]byte, 4+64+count*32+32+elementSize)
	copy(data, customErr.ID[:4])
	putWord := func(offset, n int) {
		binary.BigEndian.PutUint64(data[offset+24:offset+32], uint64(n))
	}
	putWord(4, 32)
	putWord(36, count)
	for i := range count {
		putWord(68+i*32, count*32)
	}
	putWord(68+count*32, elementSize)
	for i := 100 + count*32; i < len(data); i++ {
		data[i] = 0xff
	}
	return data
}

func TestExplainRevertDataInputCap(t *testing.T) {
	custom := hexutil.MustDecode(revertData(t, ics26router.ContractMetaData, "IBCClientNotFound", "client-0"))
	standard := append([]byte(nil), custom...)
	copy(standard, hexutil.MustDecode("0x08c379a0"))
	for _, tc := range []struct {
		name, want string
		data       []byte
	}{
		{"custom", "IBCClientNotFound(client-0)", custom},
		{"standard", "client-0", standard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Trailing padding counts toward the cap, along with the selector.
			data := make([]byte, maxRevertDataBytes)
			copy(data, tc.data)
			require.Equal(t, tc.want, ExplainRevertData(data))

			reason := ExplainRevertData(append(data, 0))
			require.Contains(t, reason, "1025 encoded bytes, truncated")
			require.Less(t, len(reason), 256)
		})
	}
}

func TestExplainRevertDataOverlappingOffsets(t *testing.T) {
	data := overlappingRevert(t, 512, 16*1024)
	reason := ExplainRevertData(data)
	require.Contains(t, reason, "InvalidMerklePrefix")
	require.Contains(t, reason, fmt.Sprintf("%d encoded bytes, truncated", len(data)))
	require.Less(t, len(reason), 256)
}

type revertError struct {
	code int
	data any
}

func (e revertError) Error() string  { return "execution reverted" }
func (e revertError) ErrorCode() int { return e.code }
func (e revertError) ErrorData() any { return e.data }

// revertData is the hex revert data of the named custom error.
func revertData(t testing.TB, metadata *bind.MetaData, name string, args ...any) string {
	t.Helper()
	contractABI, err := metadata.GetAbi()
	require.NoError(t, err)
	customErr, ok := contractABI.Errors[name]
	require.True(t, ok)
	packed, err := customErr.Inputs.Pack(args...)
	require.NoError(t, err)
	return hexutil.Encode(append(customErr.ID[:4], packed...))
}

// namedReverts are custom errors ExplainRevert names.
var namedReverts = []struct {
	metadata *bind.MetaData
	name     string
	args     []any
	want     string
}{
	{ics26router.ContractMetaData, "IBCClientNotFound", []any{"client-0"}, "IBCClientNotFound(client-0)"},
	{
		ics26router.ContractMetaData, "InvalidMerklePrefix",
		[]any{[][]byte{{1, 2}, {3}}},
		"InvalidMerklePrefix([[1 2] [3]])",
	},
	{attestation.ContractMetaData, "ConsensusTimestampNotFound", []any{uint64(7)}, "ConsensusTimestampNotFound(7)"},
	{ift.ContractMetaData, "RateLimitExceeded", nil, "RateLimitExceeded()"},
	{ibcmulticall.ContractMetaData, "IBCMulticallCallOutOfGas", []any{big.NewInt(2)}, "IBCMulticallCallOutOfGas(2)"},
	{
		besulightclient.ContractMetaData, "ConsensusStateExpired",
		[]any{uint64(100), big.NewInt(121), uint64(20)},
		"ConsensusStateExpired(100, 121, 20)",
	},
	{
		besulightclient.ContractMetaData, "InvalidStorageRoot",
		[]any{[32]byte{31: 1}},
		fmt.Sprintf("InvalidStorageRoot(%v)", [32]byte{31: 1}),
	},
}

func TestExplainRevert(t *testing.T) {
	for _, tc := range namedReverts {
		t.Run("names "+tc.name, func(t *testing.T) {
			data := revertData(t, tc.metadata, tc.name, tc.args...)
			require.Equal(t, tc.want, ExplainRevertData(hexutil.MustDecode(data)))
			// geth and Besu 26.x use code 3, older Besu -32000
			for _, code := range []int{3, -32000} {
				original := revertError{code: code, data: data}
				err := ExplainRevert(original)
				require.EqualError(t, err, tc.want+": execution reverted")
				require.ErrorIs(t, err, original)
			}
		})
	}

	t.Run("keeps unknown data", func(t *testing.T) {
		err := ExplainRevert(revertError{data: "0xdeadbeef"})
		require.EqualError(t, err, "0xdeadbeef: execution reverted")
	})

	t.Run("keeps errors without data", func(t *testing.T) {
		plain := errors.New("connection refused")
		require.Equal(t, plain, ExplainRevert(plain))
	})
}

func TestExplainRevertData(t *testing.T) {
	stringType, err := abi.NewType("string", "", nil)
	require.NoError(t, err)
	message, err := (abi.Arguments{{Type: stringType}}).Pack("rate limit exceeded")
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"empty", nil, "empty revert data"},
		{"short", []byte{1, 2}, "0x0102"},
		{"unknown", hexutil.MustDecode("0xdeadbeef"), "0xdeadbeef"},
		{
			"oversized unknown", make([]byte, 1025),
			"0x" + strings.Repeat("00", 64) + "... (1025 encoded bytes, truncated)",
		},
		{"revert string", append(hexutil.MustDecode("0x08c379a0"), message...), "rate limit exceeded"},
		{"malformed revert", hexutil.MustDecode("0x08c379a0"), "0x08c379a0"},
		{"panic", hexutil.MustDecode("0x4e487b710000000000000000000000000000000000000000000000000000000000000011"), "arithmetic underflow or overflow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ExplainRevertData(tc.data))
		})
	}
}
