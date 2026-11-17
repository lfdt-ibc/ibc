// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"

	"github.com/cosmos/ibc-contracts/packages/go-abigen/attestation"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/besulightclient"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/ibcmulticall"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/ics26router"
	"github.com/cosmos/ibc-contracts/packages/go-abigen/ift"
)

const (
	maxRevertReason    = 2048
	revertPreviewBytes = 64
)

// revertContracts contains known custom errors from relay calls.
// One Besu light client verifies both QBFT and IBFT2.
var revertContracts = []*bind.MetaData{
	ics26router.ContractMetaData, attestation.ContractMetaData, besulightclient.ContractMetaData,
	ibcmulticall.ContractMetaData, ift.ContractMetaData,
}

// ExplainRevert adds decoded revert data to an RPC error.
// Any error code is accepted: Besu before 26.x reports
// reverts with -32000 rather than 3.
func ExplainRevert(err error) error {
	var dataErr rpc.DataError
	if !errors.As(err, &dataErr) {
		return err
	}
	hexData, ok := dataErr.ErrorData().(string)
	if !ok {
		return err
	}
	data, decodeErr := hexutil.Decode(hexData)
	if decodeErr != nil || len(data) == 0 {
		return err
	}
	return errors.Wrap(err, ExplainRevertData(data))
}

// ExplainRevertData decodes revert reasons and bounds custom-error diagnostics.
func ExplainRevertData(data []byte) string {
	if len(data) == 0 {
		return "empty revert data"
	}
	if reason, err := abi.UnpackRevert(data); err == nil {
		return reason
	}
	if len(data) < 4 {
		return hexutil.Encode(data)
	}
	for _, metadata := range revertContracts {
		contractABI, abiErr := metadata.GetAbi()
		if abiErr != nil {
			continue
		}
		customErr, idErr := contractABI.ErrorByID([4]byte(data[:4]))
		if idErr != nil {
			continue
		}
		args, unpackErr := customErr.Inputs.Unpack(data[4:])
		if unpackErr != nil {
			return fmt.Sprintf("%s (malformed data: %s)", customErr.Name, revertDataPreview(data))
		}
		// Check the expanded values before fmt traverses them: ABI offsets may
		// alias the same bytes repeatedly even when the encoded input is small.
		// The budget covers arguments; the name and punctuation add a small overhead.
		budget := maxRevertReason
		for _, arg := range args {
			if !revertValueFits(reflect.ValueOf(arg), &budget) {
				return customErr.Name + " (" + revertDataPreview(data) + ")"
			}
		}
		values := make([]string, len(args))
		for i, arg := range args {
			values[i] = fmt.Sprint(arg)
		}
		return fmt.Sprintf("%s(%s)", customErr.Name, strings.Join(values, ", "))
	}
	return revertDataPreview(data)
}

func revertDataPreview(data []byte) string {
	if len(data) <= revertPreviewBytes {
		return hexutil.Encode(data)
	}
	return fmt.Sprintf(
		"%s... (%d bytes, truncated)",
		hexutil.Encode(data[:revertPreviewBytes]), len(data),
	)
}

// revertValueFits conservatively budgets formatting of the registered ABI
// errors. Traversal stops as soon as the budget is exhausted, including when
// different slices alias the same payload. Scalars fit within 128 characters;
// byte elements need at most three decimal digits.
func revertValueFits(value reflect.Value, budget *int) bool {
	if *budget <= 0 {
		return false
	}
	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		*budget -= 2 // brackets
		for i := 0; i < value.Len(); i++ {
			*budget-- // separator
			if !revertValueFits(value.Index(i), budget) {
				return false
			}
		}
	case reflect.String:
		*budget -= value.Len()
	case reflect.Uint8:
		*budget -= 3
	default:
		*budget -= 128
	}
	return *budget >= 0
}
