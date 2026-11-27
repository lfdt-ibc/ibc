// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Error codes returned to agents; see the API doc for when each applies.
const (
	CodeInvalidArgument    ErrorCode = "INVALID_ARGUMENT"
	CodePolicyDenied       ErrorCode = "POLICY_DENIED"
	CodeUserDeclined       ErrorCode = "USER_DECLINED"
	CodeConfigError        ErrorCode = "CONFIG_ERROR"
	CodeChainRPCError      ErrorCode = "CHAIN_RPC_ERROR"
	CodeTxReverted         ErrorCode = "TX_REVERTED"
	CodeTxNotConfirmed     ErrorCode = "TX_NOT_CONFIRMED"
	CodeRelayerUnavailable ErrorCode = "RELAYER_UNAVAILABLE"
	CodeRelayerError       ErrorCode = "RELAYER_ERROR"
	// CodePacketsNotFound is a relay that found no packets in the tx yet, for
	// example before the node serves a just-mined receipt.
	CodePacketsNotFound ErrorCode = "PACKETS_NOT_FOUND"
)

// ErrorCode is a stable, machine-readable failure class for agents to branch on.
type ErrorCode string

// ToolError is returned to the agent as the JSON text of an error tool result.
type ToolError struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
	Tx        *TxRef    `json:"tx,omitempty"`
}

func (e *ToolError) Error() string {
	bz, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return string(bz)
}

// NewError builds a ToolError; backends use it to classify their failures.
func NewError(code ErrorCode, format string, args ...any) *ToolError {
	return &ToolError{Code: code, Message: fmt.Sprintf(format, args...), Retryable: retryable(code)}
}

// NewTxError builds a ToolError for a tx that reached the chain.
func NewTxError(code ErrorCode, tx TxRef, format string, args ...any) *ToolError {
	e := NewError(code, format, args...)
	e.Tx = &tx
	return e
}

func retryable(code ErrorCode) bool {
	switch code {
	case CodeChainRPCError, CodeRelayerUnavailable, CodePacketsNotFound:
		return true
	default:
		// TX_NOT_CONFIRMED is not retryable: the tx may still mine, and signing
		// again moves funds a second time. Check the hash first.
		return false
	}
}

// asToolError keeps a ToolError as is and classifies anything else with fallback.
func asToolError(err error, fallback ErrorCode) *ToolError {
	var te *ToolError
	if errors.As(err, &te) {
		return te
	}
	return NewError(fallback, "%s", err.Error())
}
