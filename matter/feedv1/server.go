package feedv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// maxJSONBody bounds whole HTTP documents; individual invocation arguments are
// separately capped at 64 KiB by Intent.Validate.
const maxJSONBody = 1024 * 1024

type principalKey struct{}

// WithPrincipal records the already-authenticated M2M principal. A listener
// adapter must add it only after its mTLS authentication succeeds.
func WithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// Principal returns the verified M2M principal carried by a listener adapter.
func Principal(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(principalKey{}).(string)
	return value, ok && publicID(value)
}

type Options struct {
	// InstanceID must be unique for every process lifetime. Reusing it makes a
	// lost replay window indistinguishable from a valid cursor.
	InstanceID string
	// ReplayLimit is the maximum number of changed full snapshots retained.
	ReplayLimit int
}

type Server struct {
	provider Provider
	instance string
	limit    int

	mu       sync.Mutex
	current  Snapshot
	currentB []byte
	sequence uint64
	history  []change
	hasValue bool
}

type change struct {
	cursor Cursor
	value  Snapshot
}

func New(provider Provider, options Options) (*Server, error) {
	if provider == nil || !publicID(options.InstanceID) || options.ReplayLimit < 1 || options.ReplayLimit > 1024 {
		return nil, errors.New("matter binding feed server options are invalid")
	}
	return &Server{provider: provider, instance: options.InstanceID, limit: options.ReplayLimit}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if s == nil || s.provider == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	switch request.URL.Path {
	case "/v1/snapshot":
		if request.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		snapshot, cursor, err := s.capture(request.Context())
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "capture_unavailable")
			return
		}
		writeJSON(w, http.StatusOK, SnapshotResponse{Contract: Contract, Cursor: cursor, Snapshot: snapshot})
	case "/v1/changes":
		if request.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		cursor, err := parseCursor(request.URL.Query().Get("cursor"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_cursor")
			return
		}
		changes, current, resync, err := s.changes(request.Context(), cursor)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "capture_unavailable")
			return
		}
		if resync {
			writeJSON(w, http.StatusConflict, ChangesResponse{Contract: Contract, Cursor: current, ResyncRequired: true, Changes: []SnapshotResponse{}})
			return
		}
		writeJSON(w, http.StatusOK, ChangesResponse{Contract: Contract, Cursor: current, Changes: changes})
	case "/v1/invoke":
		if request.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		if !isJSON(request.Header.Get("Content-Type")) {
			writeError(w, http.StatusUnsupportedMediaType, "content_type_required")
			return
		}
		if _, ok := Principal(request.Context()); !ok {
			writeError(w, http.StatusUnauthorized, "principal_required")
			return
		}
		var intent Intent
		if err := decodeStrict(request.Body, &intent); err != nil || intent.Validate() != nil || !time.Now().UTC().Before(intent.Deadline) {
			writeError(w, http.StatusBadRequest, "invalid_intent")
			return
		}
		copy, err := detach(intent)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_intent")
			return
		}
		snapshot, _, err := s.capture(request.Context())
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "capture_unavailable")
			return
		}
		operation, ok := findOperation(snapshot, copy)
		if !ok {
			writeError(w, http.StatusForbidden, "admission_rejected")
			return
		}
		execution, err := s.provider.Invoke(request.Context(), Invocation{Operation: operation, Intent: copy})
		if err != nil {
			writeError(w, http.StatusForbidden, "admission_rejected")
			return
		}
		if err := execution.Validate(); err != nil {
			writeError(w, http.StatusBadGateway, "invalid_execution")
			return
		}
		writeJSON(w, http.StatusOK, execution)
	default:
		writeError(w, http.StatusNotFound, "not_found")
	}
}

func findOperation(snapshot Snapshot, intent Intent) (AdmittedOperation, bool) {
	for _, operation := range snapshot.Operations {
		if operation.AssetID == intent.AssetID && operation.Claim == intent.Claim {
			return operation, true
		}
	}
	return AdmittedOperation{}, false
}

func (s *Server) capture(ctx context.Context) (Snapshot, Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	provided, err := s.provider.Capture(ctx)
	if err != nil {
		return Snapshot{}, Cursor{}, err
	}
	canonical, err := CanonicalJSON(provided)
	if err != nil {
		return Snapshot{}, Cursor{}, err
	}
	value, err := detach(provided)
	if err != nil {
		return Snapshot{}, Cursor{}, err
	}
	sortSnapshot(&value)
	if !s.hasValue || string(canonical) != string(s.currentB) {
		s.sequence++
		s.current, s.currentB, s.hasValue = value, append([]byte(nil), canonical...), true
		s.history = append(s.history, change{cursor: Cursor{Instance: s.instance, Sequence: s.sequence}, value: value})
		if len(s.history) > s.limit {
			s.history = append([]change(nil), s.history[len(s.history)-s.limit:]...)
		}
	}
	copy, err := detach(s.current)
	return copy, Cursor{Instance: s.instance, Sequence: s.sequence}, err
}

func (s *Server) changes(ctx context.Context, cursor Cursor) ([]SnapshotResponse, Cursor, bool, error) {
	_, current, err := s.capture(ctx)
	if err != nil {
		return nil, Cursor{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor.Instance != s.instance || cursor.Sequence > s.sequence || (len(s.history) != 0 && cursor.Sequence+1 < s.history[0].cursor.Sequence) {
		return nil, current, true, nil
	}
	changes := make([]SnapshotResponse, 0)
	for _, item := range s.history {
		if item.cursor.Sequence <= cursor.Sequence {
			continue
		}
		copy, err := detach(item.value)
		if err != nil {
			return nil, Cursor{}, false, err
		}
		changes = append(changes, SnapshotResponse{Contract: Contract, Cursor: item.cursor, Snapshot: copy})
	}
	return changes, current, false, nil
}

type SnapshotResponse struct {
	Contract string   `json:"contract"`
	Cursor   Cursor   `json:"cursor"`
	Snapshot Snapshot `json:"snapshot"`
}
type ChangesResponse struct {
	Contract       string             `json:"contract"`
	Cursor         Cursor             `json:"cursor"`
	ResyncRequired bool               `json:"resync_required,omitempty"`
	Changes        []SnapshotResponse `json:"changes"`
}
type errorResponse struct {
	Contract string `json:"contract"`
	Error    string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}
func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, errorResponse{Contract: Contract, Error: code})
}
func isJSON(value string) bool {
	return strings.EqualFold(strings.TrimSpace(strings.Split(value, ";")[0]), "application/json")
}

func decodeStrict(body io.ReadCloser, destination any) error {
	defer func() { _ = body.Close() }()
	reader := io.LimitReader(body, maxJSONBody+1)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("multiple JSON values")
	}
	return nil
}

func parseCursor(raw string) (Cursor, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 || !publicID(parts[0]) {
		return Cursor{}, errors.New("invalid cursor")
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || sequence == 0 {
		return Cursor{}, errors.New("invalid cursor")
	}
	return Cursor{Instance: parts[0], Sequence: sequence}, nil
}

func FormatCursor(cursor Cursor) string {
	return cursor.Instance + ":" + strconv.FormatUint(cursor.Sequence, 10)
}

// Digest returns a stable diagnostic hash of a successfully canonical snapshot.
func Digest(snapshot Snapshot) (string, error) {
	raw, err := CanonicalJSON(snapshot)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
