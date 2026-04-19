// Package execution_policy — RED stub. Impl in next commit.
package execution_policy

import (
	"errors"

	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

type CallerContext string

const (
	CallerUserFacing      CallerContext = "user_facing"
	CallerSystemNMRuntime CallerContext = "system_nm_runtime"
)

var ErrSafetyClassDenied = errors.New("execution_policy: stub denies everything")

func Check(_ ebusstd.Command, _ CallerContext) error { return ErrSafetyClassDenied }

func IsDenied(err error) bool { return errors.Is(err, ErrSafetyClassDenied) }
