package eebusadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	spinePageSize        = 8
	maxSpineSnapshots    = 32
	maxSpineCursors      = 256
	spineSnapshotMaxLife = 2 * time.Minute
)

type RawSnapshotProvider interface {
	Snapshot() (eebusruntime.SnapshotV1, error)
}

type spineSnapshot struct {
	id        string
	hash      string
	sessionID string
	partnerID string
	expiresAt time.Time
	nodes     map[string]spineNode
	children  map[string][]string
	cursors   map[string]spineCursor
}

type spineCursor struct {
	parentID string
	offset   int
}

type spineNode struct {
	ID       string
	ParentID string
	Kind     string
	SortKey  string
	Payload  json.RawMessage
}

type spinePageEnvelope struct {
	Contract string    `json:"contract"`
	Data     spinePage `json:"data"`
	Error    any       `json:"error"`
}

type spinePage struct {
	SnapshotID   string          `json:"snapshot_id"`
	SnapshotHash string          `json:"snapshot_hash"`
	ParentNodeID *string         `json:"parent_node_id"`
	Nodes        []spinePageNode `json:"nodes"`
	NextCursor   string          `json:"next_cursor,omitempty"`
}

type spinePageNode struct {
	NodeID       string          `json:"node_id"`
	ParentNodeID *string         `json:"parent_node_id"`
	Kind         string          `json:"kind"`
	SortKey      string          `json:"sort_key"`
	Payload      json.RawMessage `json:"payload"`
}

func (server *server) spinePage(w http.ResponseWriter, request *http.Request, session ownerSession) {
	partnerID, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/partners/", "/spine")
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	shape, ok := parseSpineQuery(request)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	if shape.request == "root" {
		server.spineRoot(w, session, partnerID)
		return
	}
	server.spineExistingPage(w, session, partnerID, shape)
}

type spineQuery struct {
	request    string
	snapshotID string
	parentID   string
	cursor     string
}

func parseSpineQuery(request *http.Request) (spineQuery, bool) {
	values := request.URL.Query()
	for key, items := range values {
		if len(items) != 1 || items[0] == "" || (key != "request" && key != "snapshot_id" && key != "parent_node_id" && key != "cursor") {
			return spineQuery{}, false
		}
	}
	shape := spineQuery{request: values.Get("request"), snapshotID: values.Get("snapshot_id"), parentID: values.Get("parent_node_id"), cursor: values.Get("cursor")}
	switch shape.request {
	case "root":
		return shape, len(values) == 1
	case "children":
		return shape, len(values) == 3 && shape.snapshotID != "" && shape.parentID != "" && shape.cursor == ""
	case "continue":
		return shape, len(values) == 4 && shape.snapshotID != "" && shape.parentID != "" && shape.cursor != ""
	default:
		return spineQuery{}, false
	}
}

func (server *server) spineRoot(w http.ResponseWriter, session ownerSession, partnerID string) {
	partner, ok := server.resolveCurrentCapability(partnerID, session.id, capabilityPartner)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "snapshot_expired")
		return
	}
	if server.raw == nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	raw, err := server.raw.Snapshot()
	if err != nil || raw.Validate() != nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	snapshot, err := server.buildSpineSnapshot(raw.Clone(), session, partnerID, partner.ski)
	if err != nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.spineMu.Lock()
	server.pruneSpineSnapshotsLocked()
	if len(server.spineSnapshots) >= maxSpineSnapshots {
		server.spineMu.Unlock()
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.spineSnapshots[snapshot.id] = snapshot
	page, err := server.spinePageLocked(snapshot, "", 0)
	server.spineMu.Unlock()
	if err != nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.writeJSON(w, http.StatusOK, spinePageEnvelope{Contract: ContractV1, Data: page})
}

func (server *server) spineExistingPage(w http.ResponseWriter, session ownerSession, partnerID string, shape spineQuery) {
	server.spineMu.Lock()
	server.pruneSpineSnapshotsLocked()
	snapshot, ok := server.spineSnapshots[shape.snapshotID]
	if !ok || snapshot.sessionID != session.id || snapshot.partnerID != partnerID || !server.auth.now().Before(snapshot.expiresAt) {
		server.spineMu.Unlock()
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "snapshot_expired")
		return
	}
	if _, ok := snapshot.nodes[shape.parentID]; !ok {
		server.spineMu.Unlock()
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "snapshot_expired")
		return
	}
	offset := 0
	if shape.request == "continue" {
		cursor, exists := snapshot.cursors[shape.cursor]
		if !exists || cursor.parentID != shape.parentID {
			server.spineMu.Unlock()
			server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "snapshot_expired")
			return
		}
		delete(snapshot.cursors, shape.cursor)
		offset = cursor.offset
	}
	page, err := server.spinePageLocked(snapshot, shape.parentID, offset)
	server.spineMu.Unlock()
	if err != nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.writeJSON(w, http.StatusOK, spinePageEnvelope{Contract: ContractV1, Data: page})
}

func (server *server) buildSpineSnapshot(raw eebusruntime.SnapshotV1, session ownerSession, partnerID, ski string) (*spineSnapshot, error) {
	id, err := randomToken(server.auth.random)
	if err != nil {
		return nil, err
	}
	expiresAt := server.auth.now().Add(spineSnapshotMaxLife)
	if session.expiresAt.Before(expiresAt) {
		expiresAt = session.expiresAt
	}
	result := &spineSnapshot{id: id, hash: raw.Meta.DataHash, sessionID: session.id, partnerID: partnerID, expiresAt: expiresAt, nodes: make(map[string]spineNode), children: make(map[string][]string), cursors: make(map[string]spineCursor)}
	deviceIDs := make(map[string]string)
	entityIDs := make(map[string]string)
	featureIDs := make(map[string]string)
	for _, device := range raw.Devices {
		if device.SKI != ski {
			continue
		}
		node, nodeErr := server.newSpineNode("", "device", "device|"+device.Address, device)
		if nodeErr != nil {
			return nil, nodeErr
		}
		result.add(node)
		deviceIDs[device.Address] = node.ID
	}
	if len(deviceIDs) == 0 {
		return nil, errors.New("partner has no raw device inventory")
	}
	for _, entity := range raw.Entities {
		parentID, exists := deviceIDs[entity.DeviceAddress]
		if !exists {
			continue
		}
		node, nodeErr := server.newSpineNode(parentID, "entity", "entity|"+entity.EntityAddress, entity)
		if nodeErr != nil {
			return nil, nodeErr
		}
		result.add(node)
		entityIDs[entity.EntityAddress] = node.ID
	}
	for _, feature := range raw.Features {
		parentID, exists := entityIDs[feature.EntityAddress]
		if !exists {
			continue
		}
		node, nodeErr := server.newSpineNode(parentID, "feature", "feature|"+feature.FeatureAddress, feature)
		if nodeErr != nil {
			return nil, nodeErr
		}
		result.add(node)
		featureIDs[feature.FeatureAddress] = node.ID
	}
	for index, useCase := range raw.UseCases {
		parentID, exists := featureIDs[useCase.ContextAddress]
		if !exists {
			if belongsToSpinePartner(useCase.ContextAddress, deviceIDs) {
				return nil, errors.New("use-case claim has no partner feature")
			}
			continue
		}
		node, nodeErr := server.newSpineNode(parentID, "use_case_claim", "use_case|"+useCase.ContextAddress+"|"+useCase.Name+"|"+useCase.Actor+"|"+decimalIndex(index), useCase)
		if nodeErr != nil {
			return nil, nodeErr
		}
		result.add(node)
	}
	includeTopLevelOpaque, unambiguous := topLevelOpaqueBelongsToPartner(raw, ski)
	if !unambiguous && len(raw.Opaque) != 0 {
		return nil, errors.New("top-level opaque ownership is ambiguous")
	}
	if includeTopLevelOpaque {
		rootParent := firstSortedMapValue(deviceIDs)
		for index, opaque := range raw.Opaque {
			node, nodeErr := server.newSpineNode(rootParent, "opaque", "opaque|"+opaque.Path+"|"+opaque.Source+"|"+decimalIndex(index), opaque)
			if nodeErr != nil {
				return nil, nodeErr
			}
			result.add(node)
		}
	}
	for parentID := range result.children {
		sort.Slice(result.children[parentID], func(left, right int) bool {
			leftNode := result.nodes[result.children[parentID][left]]
			rightNode := result.nodes[result.children[parentID][right]]
			if leftNode.SortKey == rightNode.SortKey {
				return leftNode.ID < rightNode.ID
			}
			return leftNode.SortKey < rightNode.SortKey
		})
	}
	return result, nil
}

func topLevelOpaqueBelongsToPartner(raw eebusruntime.SnapshotV1, ski string) (bool, bool) {
	remoteSKIs := make(map[string]struct{})
	for _, device := range raw.Devices {
		if device.SKI != "" && device.SKI != raw.Meta.LocalSKI {
			remoteSKIs[device.SKI] = struct{}{}
		}
	}
	_, selected := remoteSKIs[ski]
	return selected && len(remoteSKIs) == 1, len(remoteSKIs) <= 1
}

func belongsToSpinePartner(contextAddress string, deviceIDs map[string]string) bool {
	for address := range deviceIDs {
		if contextAddress == address || strings.HasPrefix(contextAddress, address+":") {
			return true
		}
	}
	return false
}

func (server *server) newSpineNode(parentID, kind, sortKey string, payload any) (spineNode, error) {
	id, err := randomToken(server.auth.random)
	if err != nil {
		return spineNode{}, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil || !json.Valid(encoded) {
		return spineNode{}, errors.New("canonical payload is not JSON representable")
	}
	return spineNode{ID: id, ParentID: parentID, Kind: kind, SortKey: sortKey, Payload: append(json.RawMessage(nil), encoded...)}, nil
}

func (snapshot *spineSnapshot) add(node spineNode) {
	snapshot.nodes[node.ID] = node
	snapshot.children[node.ParentID] = append(snapshot.children[node.ParentID], node.ID)
}

func (server *server) spinePageLocked(snapshot *spineSnapshot, parentID string, offset int) (spinePage, error) {
	children := snapshot.children[parentID]
	if offset < 0 || offset > len(children) {
		return spinePage{}, errors.New("invalid page offset")
	}
	end := offset + spinePageSize
	if end > len(children) {
		end = len(children)
	}
	page := spinePage{SnapshotID: snapshot.id, SnapshotHash: snapshot.hash, Nodes: make([]spinePageNode, 0, end-offset)}
	if parentID != "" {
		value := parentID
		page.ParentNodeID = &value
	}
	for _, nodeID := range children[offset:end] {
		node := snapshot.nodes[nodeID]
		var parent *string
		if node.ParentID != "" {
			value := node.ParentID
			parent = &value
		}
		page.Nodes = append(page.Nodes, spinePageNode{NodeID: node.ID, ParentNodeID: parent, Kind: node.Kind, SortKey: node.SortKey, Payload: append(json.RawMessage(nil), node.Payload...)})
	}
	if end < len(children) {
		if len(snapshot.cursors) >= maxSpineCursors {
			return spinePage{}, errors.New("cursor capacity exhausted")
		}
		cursor, err := randomToken(server.auth.random)
		if err != nil {
			return spinePage{}, err
		}
		snapshot.cursors[cursor] = spineCursor{parentID: parentID, offset: end}
		page.NextCursor = cursor
	}
	return page, nil
}

func (server *server) pruneSpineSnapshotsLocked() {
	now := server.auth.now()
	for id, snapshot := range server.spineSnapshots {
		if !now.Before(snapshot.expiresAt) {
			delete(server.spineSnapshots, id)
		}
	}
}

func firstSortedMapValue(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return values[keys[0]]
}

func decimalIndex(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var reversed [20]byte
	position := len(reversed)
	for value > 0 {
		position--
		reversed[position] = digits[value%10]
		value /= 10
	}
	return string(reversed[position:])
}
