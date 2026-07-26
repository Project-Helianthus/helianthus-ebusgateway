package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestIssue743PublicPseudonymsAreKeyedEphemeralAndDomainSeparated(t *testing.T) {
	source := msp06Snapshot(t, "runtime-keyed-public")
	keyA := bytes.Repeat([]byte{0x31}, sha256.Size)
	keyB := bytes.Repeat([]byte{0x32}, sha256.Size)

	first, err := eebusV1ProjectSnapshotForBoundary(source, eebusV1PublicBoundary, keyA)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := eebusV1ProjectSnapshotForBoundary(source, eebusV1PublicBoundary, keyA)
	if err != nil {
		t.Fatal(err)
	}
	otherProcess, err := eebusV1ProjectSnapshotForBoundary(source, eebusV1PublicBoundary, keyB)
	if err != nil {
		t.Fatal(err)
	}

	firstJSON, err := json.Marshal(first.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	repeatedJSON, err := json.Marshal(repeated.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	otherJSON, err := json.Marshal(otherProcess.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, repeatedJSON) || first.ContentHash != repeated.ContentHash {
		t.Fatal("same pseudonym key did not produce deterministic public output")
	}
	if bytes.Equal(firstJSON, otherJSON) || first.ContentHash == otherProcess.ContentHash {
		t.Fatal("different process keys produced correlatable public output")
	}

	firstDigests := issue743PublicSnapshotDigests(first.Snapshot)
	repeatedDigests := issue743PublicSnapshotDigests(repeated.Snapshot)
	otherDigests := issue743PublicSnapshotDigests(otherProcess.Snapshot)
	if len(firstDigests) == 0 || len(firstDigests) != len(repeatedDigests) || len(firstDigests) != len(otherDigests) {
		t.Fatalf("public digest counts = %d/%d/%d", len(firstDigests), len(repeatedDigests), len(otherDigests))
	}
	for index := range firstDigests {
		if firstDigests[index] != repeatedDigests[index] {
			t.Fatalf("same-key digest %d changed", index)
		}
		if firstDigests[index] == otherDigests[index] {
			t.Fatalf("cross-process digest %d remained stable", index)
		}
		if !msp06HashPattern.MatchString(firstDigests[index]) {
			t.Fatalf("public digest %d is not canonical: %q", index, firstDigests[index])
		}
	}

	stable, err := eebusruntime.BuildRedactedSnapshotV1(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, digest := range issue743StableRedactedDigests(stable) {
		if bytes.Contains(firstJSON, []byte(digest)) || bytes.Contains(otherJSON, []byte(digest)) {
			t.Fatalf("dependency-stable correlator escaped public output: %q", digest)
		}
	}
	for _, encodedKey := range []string{
		hex.EncodeToString(keyA),
		base64.RawURLEncoding.EncodeToString(keyA),
	} {
		if bytes.Contains(firstJSON, []byte(encodedKey)) {
			t.Fatalf("pseudonym key leaked into public output as %q", encodedKey)
		}
	}

	serviceDomain, err := eebusV1KeyRedactedID(keyA, eebusV1PseudonymDomainService, stable.Services[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	deviceDomain, err := eebusV1KeyRedactedID(keyA, eebusV1PseudonymDomainDevice, stable.Services[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if serviceDomain.Digest == deviceDomain.Digest {
		t.Fatal("distinct identity domains produced the same pseudonym")
	}
}

func TestIssue743RawOperatorProjectionIgnoresPublicPseudonymKey(t *testing.T) {
	source := msp06Snapshot(t, "runtime-raw-key-invariant")
	first, err := eebusV1ProjectSnapshotForBoundary(
		source, eebusV1OperatorBoundary, bytes.Repeat([]byte{0x41}, sha256.Size),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eebusV1ProjectSnapshotForBoundary(
		source, eebusV1OperatorBoundary, bytes.Repeat([]byte{0x42}, sha256.Size),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first.capturedSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second.capturedSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) || first.ContentHash != source.Meta.DataHash ||
		second.ContentHash != source.Meta.DataHash {
		t.Fatal("public pseudonym key changed the raw operator projection")
	}
}

func TestIssue743SynchronizedEvidencePublicIDsUseProvidedEphemeralKey(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-keyed-evidence")}
	keyA := bytes.Repeat([]byte{0x51}, sha256.Size)
	keyB := bytes.Repeat([]byte{0x52}, sha256.Size)

	first, _, err := CaptureEEBusV1ServicesEvidence(provider, keyA)
	if err != nil {
		t.Fatal(err)
	}
	repeated, _, err := CaptureEEBusV1ServicesEvidence(provider, keyA)
	if err != nil {
		t.Fatal(err)
	}
	otherProcess, _, err := CaptureEEBusV1ServicesEvidence(provider, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, repeated) {
		t.Fatal("same synchronized-evidence key was not deterministic")
	}
	firstDigest, firstMeta := issue743EvidenceServiceDigest(t, first)
	repeatedDigest, _ := issue743EvidenceServiceDigest(t, repeated)
	otherDigest, otherMeta := issue743EvidenceServiceDigest(t, otherProcess)
	if firstDigest != repeatedDigest || firstDigest == otherDigest {
		t.Fatalf("evidence pseudonyms first/repeated/other = %q/%q/%q", firstDigest, repeatedDigest, otherDigest)
	}
	for _, meta := range []eebusV1MetaV1{firstMeta, otherMeta} {
		if meta.MaskTier != eebusV1PublicBoundary.MaskTier || meta.AuthScope != eebusV1PublicBoundary.AuthScope {
			t.Fatalf("evidence boundary = %s/%s", meta.MaskTier, meta.AuthScope)
		}
	}

	stable, err := eebusruntime.BuildRedactedSnapshotV1(provider.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range stable.Services {
		if bytes.Contains(first, []byte(service.ID.Digest)) ||
			bytes.Contains(otherProcess, []byte(service.ID.Digest)) {
			t.Fatal("synchronized evidence exposed a dependency-stable service correlator")
		}
	}
	for _, encodedKey := range []string{
		hex.EncodeToString(keyA),
		base64.RawURLEncoding.EncodeToString(keyA),
	} {
		if bytes.Contains(first, []byte(encodedKey)) {
			t.Fatalf("synchronized evidence leaked its pseudonym key as %q", encodedKey)
		}
	}
}

func issue743EvidenceServiceDigest(t *testing.T, payload []byte) (string, eebusV1MetaV1) {
	t.Helper()
	var envelope struct {
		Meta eebusV1MetaV1 `json:"meta"`
		Data struct {
			Services []eebusruntime.RedactedServiceV1 `json:"services"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	for _, service := range envelope.Data.Services {
		if service.Kind == eebusruntime.ServiceKindV1Local {
			return service.ID.Digest, envelope.Meta
		}
	}
	t.Fatalf("evidence contains no local service among %d rows", len(envelope.Data.Services))
	return "", envelope.Meta
}

func issue743PublicSnapshotDigests(snapshot eebusV1SnapshotDataV1) []string {
	result := []string{snapshot.Meta.Runtime.Digest}
	for _, service := range snapshot.Services {
		result = append(result, service.ID.Digest)
	}
	for _, session := range snapshot.Sessions {
		result = append(result, session.ID.Digest, session.Remote.Digest)
	}
	for _, device := range snapshot.Topology.Devices {
		result = append(result, device.ID.Digest)
		for _, entity := range device.Entities {
			result = append(result, entity.ID.Digest)
			for _, feature := range entity.Features {
				result = append(result, feature.ID.Digest)
			}
		}
		for _, useCase := range device.UseCaseClaims {
			result = append(result, useCase.ID.Digest)
		}
	}
	return result
}

func issue743StableRedactedDigests(snapshot eebusruntime.RedactedSnapshotV1) []string {
	result := []string{snapshot.Meta.Runtime.Digest}
	for _, service := range snapshot.Services {
		result = append(result, service.ID.Digest)
	}
	for _, session := range snapshot.Sessions {
		result = append(result, session.ID.Digest, session.Remote.Digest)
	}
	for _, device := range snapshot.Devices {
		result = append(result, device.ID.Digest)
		for _, entity := range device.Entities {
			result = append(result, entity.ID.Digest)
			for _, feature := range entity.Features {
				result = append(result, feature.ID.Digest)
			}
		}
		for _, useCase := range device.UseCaseClaims {
			result = append(result, useCase.ID.Digest)
		}
	}
	return result
}
