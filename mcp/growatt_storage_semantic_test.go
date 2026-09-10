package mcp

import "testing"

func TestGrowattStorageEvaluatedTimestampUsesAuthoritativeEvaluation(t *testing.T) {
	timestamp, err := growattStorageEvaluatedTimestamp(map[string]any{
		"evaluation": map[string]any{"context": map[string]any{"evaluated_at": map[string]any{"unix_nanoseconds": "1800000000123456789"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if timestamp != "2027-01-15T08:00:00.123456789Z" {
		t.Fatalf("timestamp=%q", timestamp)
	}
}
