// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"math/big"
	"strings"
)

// parseAmount parses a non-negative decimal integer.
func parseAmount(s string) (*big.Int, bool) {
	if s == "" || strings.TrimLeft(s, "0123456789") != "" {
		return nil, false
	}
	v, ok := new(big.Int).SetString(s, 10)
	return v, ok
}
