package matrix

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Per-setup adaptermux-v2 invocations share one output directory. Each one
// writes only its own case directories and then rebuilds index.json from every
// verdict.json in the directory. Anything that does not belong to the same
// session, candidate and target, a second hardware setup for one transport, or
// a directory that is not an inventory case fails the rebuild instead of
// merging.

type adapterMuxIdentity struct {
	Target    string
	SessionID string
	Candidate string
}

// ErrForeignVerdict marks content of the output directory that does not
// belong to the aggregated session.
var ErrForeignVerdict = errors.New("foreign matrix verdict")

// loadAdapterMuxVerdicts reads every case verdict in outputDir, in inventory
// order, skipping the case IDs in skip (they are about to be replaced).
func loadAdapterMuxVerdicts(outputDir string, inventory []TopologyCase, skip map[string]struct{}) ([]CaseVerdict, error) {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil, fmt.Errorf("read output dir: %w", err)
	}
	known := make(map[string]struct{}, len(inventory))
	for _, testCase := range inventory {
		known[testCase.ID] = struct{}{}
	}
	present := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if _, ok := known[name]; !ok {
			return nil, fmt.Errorf("%w: directory %q is not an %s case", ErrForeignVerdict, name, SuiteAdapterMuxV2)
		}
		present[name] = struct{}{}
	}

	verdicts := make([]CaseVerdict, 0, len(present))
	for _, testCase := range inventory {
		if _, ok := present[testCase.ID]; !ok {
			continue
		}
		if _, replaced := skip[testCase.ID]; replaced {
			continue
		}
		path := filepath.Join(outputDir, testCase.ID, "verdict.json")
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("case %s: %w", testCase.ID, err)
		}
		var verdict CaseVerdict
		if err := json.Unmarshal(data, &verdict); err != nil {
			return nil, fmt.Errorf("case %s: decode verdict: %w", testCase.ID, err)
		}
		if verdict.CaseID != testCase.ID {
			return nil, fmt.Errorf("%w: directory %s holds the verdict of %q", ErrForeignVerdict, testCase.ID, verdict.CaseID)
		}
		verdicts = append(verdicts, verdict)
	}
	return verdicts, nil
}

// validateAdapterMuxAggregate checks that verdicts form one session.
func validateAdapterMuxAggregate(verdicts []CaseVerdict, identity adapterMuxIdentity, inventory []TopologyCase) error {
	cells := make(map[string]*AdapterMuxCell, len(inventory))
	for _, testCase := range inventory {
		cells[testCase.ID] = testCase.AdapterMux
	}
	setupByRow := make(map[string]string)
	setupOwner := make(map[string]string)
	for _, verdict := range verdicts {
		cell, ok := cells[verdict.CaseID]
		if !ok {
			return fmt.Errorf("%w: unknown case %q", ErrForeignVerdict, verdict.CaseID)
		}
		if verdict.Suite != SuiteAdapterMuxV2 {
			return fmt.Errorf("%w: case %s has suite %q", ErrForeignVerdict, verdict.CaseID, verdict.Suite)
		}
		if verdict.SessionID != identity.SessionID {
			return fmt.Errorf("%w: case %s has session_id %q, this session is %q", ErrForeignVerdict, verdict.CaseID, verdict.SessionID, identity.SessionID)
		}
		if verdict.Candidate != identity.Candidate {
			return fmt.Errorf("%w: case %s has candidate %q, this session is %q", ErrForeignVerdict, verdict.CaseID, verdict.Candidate, identity.Candidate)
		}
		if verdict.Target != identity.Target {
			return fmt.Errorf("%w: case %s has target %q, this session is %q", ErrForeignVerdict, verdict.CaseID, verdict.Target, identity.Target)
		}
		if verdict.SouthboundTransport != string(cell.Southbound) || verdict.Invariant != cell.Invariant {
			return fmt.Errorf("%w: case %s carries %s/%s, the inventory says %s/%s", ErrForeignVerdict, verdict.CaseID,
				verdict.SouthboundTransport, verdict.Invariant, cell.Southbound, cell.Invariant)
		}
		executed := verdict.Status == actualStatusPassed || verdict.Status == actualStatusFailed
		if executed && verdict.HardwareSetup == "" {
			return fmt.Errorf("case %s was executed without a hardware setup label", verdict.CaseID)
		}
		if verdict.HardwareSetup == "" {
			continue
		}
		if existing, ok := setupByRow[cell.Row]; ok && existing != verdict.HardwareSetup {
			return fmt.Errorf("second hardware setup %q for transport %s (case %s); %s already ran on %q",
				verdict.HardwareSetup, cell.Row, verdict.CaseID, setupOwner[cell.Row], existing)
		}
		setupByRow[cell.Row] = verdict.HardwareSetup
		setupOwner[cell.Row] = verdict.CaseID
	}
	return nil
}

// rebuildAdapterMuxIndex writes index.json from every verdict in outputDir.
// Cells not run yet are absent, so the gate's exact-inventory rule rejects a
// partial aggregate.
func rebuildAdapterMuxIndex(outputDir string, identity adapterMuxIdentity, inventory []TopologyCase, now time.Time) error {
	verdicts, err := loadAdapterMuxVerdicts(outputDir, inventory, nil)
	if err != nil {
		return err
	}
	if err := validateAdapterMuxAggregate(verdicts, identity, inventory); err != nil {
		return err
	}
	return writeJSON(filepath.Join(outputDir, "index.json"), map[string]any{
		"generated_at":             now.Format(time.RFC3339),
		"target":                   identity.Target,
		"suite":                    SuiteAdapterMuxV2,
		"matrix_inventory_version": AdapterMuxV2InventoryVersion,
		"session_id":               identity.SessionID,
		"candidate":                identity.Candidate,
		"cases":                    verdicts,
	})
}
