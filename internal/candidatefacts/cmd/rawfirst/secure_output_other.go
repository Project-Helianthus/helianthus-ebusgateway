//go:build !darwin && !linux

package main

import "errors"

func writePrivateOutputs(_ string, _ []struct {
	name    string
	content []byte
}) error {
	return errors.New("descriptor-anchored private output is unsupported on this platform")
}
