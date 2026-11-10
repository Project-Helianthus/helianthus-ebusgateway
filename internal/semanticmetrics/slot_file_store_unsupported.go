//go:build !linux && !darwin

package semanticmetrics

import "fmt"

// SlotFileStore is unavailable on this platform.
type SlotFileStore struct{}

func NewSlotFileStore(string, string) (*SlotFileStore, error) {
	return nil, fmt.Errorf("slot file persistence unsupported on this platform")
}
func (*SlotFileStore) Load(string) ([]byte, error) {
	return nil, fmt.Errorf("slot file persistence unsupported on this platform")
}
func (*SlotFileStore) Commit(string, []byte) error {
	return fmt.Errorf("slot file persistence unsupported on this platform")
}
func (*SlotFileStore) Close() error { return nil }
