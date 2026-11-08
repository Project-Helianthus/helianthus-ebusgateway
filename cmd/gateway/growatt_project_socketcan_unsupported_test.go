//go:build !linux

package main

import "testing"

func TestCX18Unsupported(t *testing.T) {
	r, err := openGrowattProjectSocket("synthetic0")
	if r != nil || err == nil {
		t.Fatal("unsupported platform opened receiver")
	}
}
