// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"testing"

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
	seed := namedReverts[0]
	encoded := hexutil.MustDecode(revertData(f, seed.metadata, seed.name, seed.args...))
	for _, size := range []int{1023, 1024, 1025} {
		data := make([]byte, size)
		copy(data, encoded)
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		reason := ExplainRevertData(data)
		if len(data) > maxRevertDataBytes {
			require.Less(t, len(reason), 256)
			require.Contains(t, reason, "encoded bytes, truncated")
		}
		original := revertError{code: 3, data: hexutil.Encode(data)}
		require.ErrorIs(t, ExplainRevert(original), original)
	})
}
