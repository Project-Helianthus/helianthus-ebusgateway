package eebusadmin

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type issue809RawSnapshotStub struct {
	snapshot eebusruntime.SnapshotV1
	calls    int
}

func (stub *issue809RawSnapshotStub) Snapshot() (eebusruntime.SnapshotV1, error) {
	stub.calls++
	return stub.snapshot.Clone(), nil
}

type issue809SpinePage struct {
	SnapshotID   string `json:"snapshot_id"`
	SnapshotHash string `json:"snapshot_hash"`
	ParentNodeID any    `json:"parent_node_id"`
	Nodes        []struct {
		NodeID       string          `json:"node_id"`
		ParentNodeID *string         `json:"parent_node_id"`
		Kind         string          `json:"kind"`
		SortKey      string          `json:"sort_key"`
		Payload      json.RawMessage `json:"payload"`
	} `json:"nodes"`
	NextCursor string `json:"next_cursor"`
}

func TestIssue809LazySPINETreePreservesVR940CanonicalInventory(t *testing.T) {
	raw := &issue809RawSnapshotStub{snapshot: issue809VR940Snapshot(t)}
	ski := raw.snapshot.Devices[0].SKI
	adminSnapshot := testAdminSnapshot()
	adminSnapshot.StateRevision = 41
	adminSnapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: ski, TrustState: "durably_trusted"}}
	admin := &adminV1Stub{snapshot: adminSnapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Trusted: adminSnapshot}}
	handler := newIssue809SpineServer(t, admin, raw)
	cookie, _ := issue809OwnerSession(t, handler)
	partnerID := issue809TrustedPartnerID(t, handler, cookie)

	root := issue809GetSpinePage(t, handler, cookie, "/admin/eebus/v1/partners/"+partnerID+"/spine?request=root")
	if root.SnapshotID == "" || root.SnapshotHash != raw.snapshot.Meta.DataHash || root.ParentNodeID != nil || len(root.Nodes) != 1 || root.Nodes[0].Kind != "device" {
		t.Fatalf("root page=%#v", root)
	}
	if !json.Valid(root.Nodes[0].Payload) || !containsJSONField(root.Nodes[0].Payload, "metadata") || !containsJSONField(root.Nodes[0].Payload, "opaque") {
		t.Fatalf("device payload lost canonical fields: %s", root.Nodes[0].Payload)
	}

	deviceChildren := issue809AllChildren(t, handler, cookie, partnerID, root.SnapshotID, root.Nodes[0].NodeID)
	entityPages := deviceChildren[:0]
	opaqueCount := 0
	for _, child := range deviceChildren {
		switch child.Kind {
		case "entity":
			entityPages = append(entityPages, child)
		case "opaque":
			opaqueCount++
		default:
			t.Fatalf("device child kind=%q", child.Kind)
		}
	}
	if len(entityPages) != 11 || opaqueCount != len(raw.snapshot.Opaque) {
		t.Fatalf("entities/opaque=%d/%d, want 11/%d", len(entityPages), opaqueCount, len(raw.snapshot.Opaque))
	}
	featureCount := 0
	useCaseCount := 0
	for _, entity := range entityPages {
		features := issue809AllChildren(t, handler, cookie, partnerID, root.SnapshotID, entity.NodeID)
		for _, feature := range features {
			if feature.Kind != "feature" {
				t.Fatalf("entity child kind=%q", feature.Kind)
			}
			featureCount++
			claims := issue809AllChildren(t, handler, cookie, partnerID, root.SnapshotID, feature.NodeID)
			for _, claim := range claims {
				if claim.Kind != "use_case_claim" {
					t.Fatalf("feature child kind=%q", claim.Kind)
				}
				useCaseCount++
			}
		}
	}
	if featureCount != 20 || useCaseCount != len(raw.snapshot.UseCases) {
		t.Fatalf("features/use-cases=%d/%d, want 20/%d", featureCount, useCaseCount, len(raw.snapshot.UseCases))
	}
	if raw.calls != 1 {
		t.Fatalf("raw Snapshot calls=%d, want immutable one-shot capture", raw.calls)
	}
}

func TestIssue809SPINEIsOwnerOnlyAndRejectsUnknownQueryBeforeRawCapture(t *testing.T) {
	raw := &issue809RawSnapshotStub{snapshot: issue809VR940Snapshot(t)}
	adminSnapshot := testAdminSnapshot()
	adminSnapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: raw.snapshot.Devices[0].SKI, TrustState: "durably_trusted"}}
	admin := &adminV1Stub{snapshot: adminSnapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Trusted: adminSnapshot}}
	handler := newIssue809SpineServer(t, admin, raw)

	ha := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners/not-authority/spine?request=root", nil)
	ha.Header.Set("Authorization", "Bearer "+testHASecret)
	haResponse := httptest.NewRecorder()
	handler.ServeHTTP(haResponse, ha)
	if haResponse.Code != http.StatusForbidden || raw.calls != 0 {
		t.Fatalf("HA SPINE status/raw calls=%d/%d body=%s", haResponse.Code, raw.calls, haResponse.Body.String())
	}

	cookie, _ := issue809OwnerSession(t, handler)
	partnerID := issue809TrustedPartnerID(t, handler, cookie)
	bad := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners/"+partnerID+"/spine?request=root&cursor=extra", nil)
	bad.AddCookie(cookie)
	badResponse := httptest.NewRecorder()
	handler.ServeHTTP(badResponse, bad)
	if badResponse.Code != http.StatusBadRequest || raw.calls != 0 {
		t.Fatalf("bad query status/raw calls=%d/%d body=%s", badResponse.Code, raw.calls, badResponse.Body.String())
	}
}

func TestIssue809SPINEFailsClosedOnUnattributedTopLevelOpaqueForMultiplePartners(t *testing.T) {
	snapshot := issue809VR940Snapshot(t)
	other := snapshot.Devices[0]
	other.SKI = "4444444444444444444444444444444444444444"
	other.Address = "other-device"
	snapshot.Devices = append(snapshot.Devices, other)
	snapshot.Meta.DataHash = ""
	var err error
	snapshot, err = eebusruntime.NewSnapshotV1(snapshot)
	if err != nil {
		t.Fatalf("build multi-partner snapshot: %v", err)
	}
	raw := &issue809RawSnapshotStub{snapshot: snapshot}
	adminSnapshot := testAdminSnapshot()
	adminSnapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: snapshot.Devices[0].SKI, TrustState: "durably_trusted"}}
	admin := &adminV1Stub{snapshot: adminSnapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Trusted: adminSnapshot}}
	handler := newIssue809SpineServer(t, admin, raw)
	cookie, _ := issue809OwnerSession(t, handler)
	partnerID := issue809TrustedPartnerID(t, handler, cookie)
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners/"+partnerID+"/spine?request=root", nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ambiguous top-level opaque status=%d body=%s", response.Code, response.Body.String())
	}
}

func newIssue809SpineServer(t *testing.T, admin eebusruntime.AdminV1, raw RawSnapshotProvider) http.Handler {
	t.Helper()
	config := issue809AuthConfig()
	config.Random = rand.Reader
	handler, err := NewServer(Config{Admin: admin, Raw: raw, Auth: config})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func issue809TrustedPartnerID(t *testing.T, handler http.Handler, cookie *http.Cookie) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=trusted", nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var envelope struct {
		Data struct {
			Partners []struct {
				PartnerID string `json:"partner_id"`
			} `json:"partners"`
		} `json:"data"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &envelope) != nil || len(envelope.Data.Partners) != 1 || envelope.Data.Partners[0].PartnerID == "" {
		t.Fatalf("trusted status=%d body=%s", response.Code, response.Body.String())
	}
	return envelope.Data.Partners[0].PartnerID
}

func issue809GetSpinePage(t *testing.T, handler http.Handler, cookie *http.Cookie, target string) issue809SpinePage {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, target, nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var envelope struct {
		Data issue809SpinePage `json:"data"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &envelope) != nil {
		t.Fatalf("SPINE status=%d body=%s", response.Code, response.Body.String())
	}
	return envelope.Data
}

func issue809AllChildren(t *testing.T, handler http.Handler, cookie *http.Cookie, partnerID, snapshotID, parentID string) []struct {
	NodeID       string          `json:"node_id"`
	ParentNodeID *string         `json:"parent_node_id"`
	Kind         string          `json:"kind"`
	SortKey      string          `json:"sort_key"`
	Payload      json.RawMessage `json:"payload"`
} {
	t.Helper()
	target := "/admin/eebus/v1/partners/" + partnerID + "/spine?request=children&snapshot_id=" + snapshotID + "&parent_node_id=" + parentID
	page := issue809GetSpinePage(t, handler, cookie, target)
	result := append([]struct {
		NodeID       string          `json:"node_id"`
		ParentNodeID *string         `json:"parent_node_id"`
		Kind         string          `json:"kind"`
		SortKey      string          `json:"sort_key"`
		Payload      json.RawMessage `json:"payload"`
	}(nil), page.Nodes...)
	for page.NextCursor != "" {
		target = "/admin/eebus/v1/partners/" + partnerID + "/spine?request=continue&snapshot_id=" + snapshotID + "&parent_node_id=" + parentID + "&cursor=" + page.NextCursor
		page = issue809GetSpinePage(t, handler, cookie, target)
		result = append(result, page.Nodes...)
	}
	return result
}

func issue809VR940Snapshot(t *testing.T) eebusruntime.SnapshotV1 {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("..", "..", "mcp", "testdata", "issue743", "vr940-raw-snapshot-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var draft eebusruntime.SnapshotV1
	if err := json.Unmarshal(content, &draft); err != nil {
		t.Fatal(err)
	}
	snapshot, err := eebusruntime.NewSnapshotV1(draft)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func containsJSONField(payload json.RawMessage, field string) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(payload, &object) == nil && object[field] != nil
}
