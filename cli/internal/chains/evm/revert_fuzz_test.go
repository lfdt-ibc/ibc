// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

// FuzzExplainRevert feeds node-supplied revert data to ExplainRevert, which
// must not panic or drop the original error.
func FuzzExplainRevert(f *testing.F) {
	for _, tc := range namedReverts {
		f.Add(hexutil.MustDecode(revertData(f, tc.metadata, tc.name, tc.args...)))
	}

	f.Add(overlappingRevert(f, 8, 256))
	f.Add(overlappingRevert(f, 512, 16*1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		if _, err := abi.UnpackRevert(data); err != nil {
			// Allow the registered error name and punctuation beyond the argument budget.
			require.LessOrEqual(t, len(ExplainRevertData(data)), maxRevertReason+256)
		}
		original := revertError{code: 3, data: hexutil.Encode(data)}
		require.ErrorIs(t, ExplainRevert(original), original)
	})
}
