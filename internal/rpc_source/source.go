// Package rpc_source — RED stub. Real const is 0x71.
package rpc_source

import "errors"

const Gateway byte = 0x00

var ErrNon113Source = errors.New("rpc_source: stub sentinel")

func Enforce(_ byte) error { return nil }
func Require(_ byte)       {}
