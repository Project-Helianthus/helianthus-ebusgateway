//go:build !darwin && !linux

package main

import "errors"

type privateOutput struct {
	name    string
	content []byte
}

func writePrivateOutputs(_ string, _ []privateOutput) error {
	return errors.New("descriptor-anchored private output is unsupported on this platform")
}
