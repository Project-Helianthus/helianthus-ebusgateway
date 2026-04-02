package ebusgateway

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

const (
	wireTimingReferenceArtifactSchema = "observe_first_wire_timing_reference_v1"
	proxyLogTimestampLayout           = "2006/01/02 15:04:05"
	proxyLogSyntheticSymbolSpacing    = 4 * time.Millisecond
)

var (
	proxyLogWireRXLineRE       = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) wire_rx symbol=0x([0-9A-Fa-f]{2})$`)
	proxyLogSessionStartLineRE = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) session=(\d+) start initiator=0x([0-9A-Fa-f]{2})$`)
	proxyLogSessionSendLineRE  = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) session=(\d+) send symbol=0x([0-9A-Fa-f]{2})$`)
)

type wireTimingReferenceArtifact struct {
	Schema      string                           `json:"schema"`
	CapturedAt  string                           `json:"captured_at"`
	Source      string                           `json:"source"`
	ClaimScope  string                           `json:"claim_scope"`
	OK          bool                             `json:"ok"`
	Evidence    wireTimingReferenceEvidence      `json:"evidence"`
	Summary     wireTimingReferenceSummary       `json:"summary"`
	Periodicity []wireTimingReferencePeriodicity `json:"periodicity"`
}

type wireTimingReferenceEvidence struct {
	ProxyLogPath             string  `json:"proxy_log_path"`
	ProxyLogLineCount        int     `json:"proxy_log_line_count"`
	SessionStartCount        int     `json:"session_start_count"`
	SendSymbolCount          int     `json:"send_symbol_count"`
	WireSymbolCount          int     `json:"wire_symbol_count"`
	SyntheticSymbolSpacingMS float64 `json:"synthetic_symbol_spacing_ms"`
	TimestampResolution      string  `json:"timestamp_resolution"`
}

type wireTimingReferenceSummary struct {
	ClassifiedEventCount  int     `json:"classified_event_count"`
	TransactionCount      int     `json:"transaction_count"`
	MasterFrameCount      int     `json:"master_frame_count"`
	AbandonedCount        int     `json:"abandoned_count"`
	BusySecondsTotal      float64 `json:"busy_seconds_total"`
	FamiliesWithIntervals int     `json:"families_with_intervals"`
}

type wireTimingReferencePeriodicity struct {
	SourceBucket    string  `json:"source_bucket"`
	TargetBucket    string  `json:"target_bucket"`
	Primary         int     `json:"primary"`
	Secondary       int     `json:"secondary"`
	Family          string  `json:"family"`
	SampleCount     int     `json:"sample_count"`
	LastSeen        string  `json:"last_seen"`
	LastIntervalSec float64 `json:"last_interval_sec,omitempty"`
	MeanIntervalSec float64 `json:"mean_interval_sec,omitempty"`
	MinIntervalSec  float64 `json:"min_interval_sec,omitempty"`
	MaxIntervalSec  float64 `json:"max_interval_sec,omitempty"`
}

type proxyLogTransaction struct {
	lineNo          int
	startObservedAt time.Time
	initiator       byte
	sendSymbols     []byte
	firstWireAt     time.Time
	lastWireAt      time.Time
	wireSymbolCount int
}

type wireTimingPeriodicityKey struct {
	Source    byte
	Target    byte
	Primary   byte
	Secondary byte
	Family    string
}

type periodicityAccumulator struct {
	starts   []time.Time
	lastSeen time.Time
}

func TestWireTimingReferenceArtifact(t *testing.T) {
	outPath := strings.TrimSpace(os.Getenv("WIRE_TIMING_REFERENCE_ARTIFACT_PATH"))
	if outPath == "" {
		t.Skip("WIRE_TIMING_REFERENCE_ARTIFACT_PATH not set")
	}
	proxyLogPath := strings.TrimSpace(os.Getenv("WIRE_TIMING_REFERENCE_PROXY_LOG_PATH"))
	if proxyLogPath == "" {
		t.Fatalf("WIRE_TIMING_REFERENCE_PROXY_LOG_PATH not set")
	}

	artifact, err := buildWireTimingReferenceArtifactFromProxyLog(proxyLogPath)
	if err != nil {
		t.Fatalf("build timing reference artifact error = %v", err)
	}
	payload, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent timing reference artifact error = %v", err)
	}
	payload = append(payload, '\n')
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(outPath), err)
	}
	if err := os.WriteFile(outPath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", outPath, err)
	}
}

func buildWireTimingReferenceArtifactFromProxyLog(proxyLogPath string) (wireTimingReferenceArtifact, error) {
	transactions, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, err := loadProxyLogTransactions(proxyLogPath)
	if err != nil {
		return wireTimingReferenceArtifact{}, err
	}

	busySecondsTotal := 0.0
	transactionCount := 0
	periodicity := make(map[wireTimingPeriodicityKey]*periodicityAccumulator)

	for _, transaction := range transactions {
		request, usable, err := transaction.requestFrame(proxyLogPath)
		if err != nil {
			return wireTimingReferenceArtifact{}, err
		}
		if !usable {
			continue
		}
		transactionCount++
		if transaction.wireSymbolCount > 0 {
			duration := transaction.lastWireAt.Sub(transaction.startObservedAt).Seconds()
			if duration < 0 || !isFiniteFloat(duration) {
				return wireTimingReferenceArtifact{}, fmt.Errorf("%s:%d: invalid busy duration %v", proxyLogPath, transaction.lineNo, duration)
			}
			if duration == 0 {
				duration = float64(transaction.wireSymbolCount) * proxyLogSyntheticSymbolSpacing.Seconds()
			}
			busySecondsTotal += duration
		}

		family := classifyFamily(request)
		if family == "" {
			continue
		}
		key := wireTimingPeriodicityKey{
			Source:    request.Source,
			Target:    request.Target,
			Primary:   request.Primary,
			Secondary: request.Secondary,
			Family:    family,
		}
		acc := periodicity[key]
		if acc == nil {
			acc = &periodicityAccumulator{}
			periodicity[key] = acc
		}
		acc.starts = append(acc.starts, transaction.startObservedAt.UTC())
		lastSeen := transaction.lastWireAt
		if lastSeen.IsZero() {
			lastSeen = transaction.startObservedAt
		}
		if acc.lastSeen.IsZero() || lastSeen.After(acc.lastSeen) {
			acc.lastSeen = lastSeen.UTC()
		}
	}

	if transactionCount == 0 {
		return wireTimingReferenceArtifact{}, fmt.Errorf("%s: proxy log did not yield any session transactions", proxyLogPath)
	}
	if busySecondsTotal <= 0 {
		return wireTimingReferenceArtifact{}, fmt.Errorf("%s: proxy log did not yield positive busy timing evidence", proxyLogPath)
	}

	items := make([]wireTimingReferencePeriodicity, 0, len(periodicity))
	familiesWithIntervals := 0
	for key, acc := range periodicity {
		sort.Slice(acc.starts, func(i, j int) bool {
			return acc.starts[i].Before(acc.starts[j])
		})
		item := wireTimingReferencePeriodicity{
			SourceBucket: fmt.Sprintf("0x%02X", key.Source),
			TargetBucket: fmt.Sprintf("0x%02X", key.Target),
			Primary:      int(key.Primary),
			Secondary:    int(key.Secondary),
			Family:       key.Family,
			SampleCount:  len(acc.starts),
			LastSeen:     acc.lastSeen.Format(time.RFC3339Nano),
		}
		if len(acc.starts) >= 2 {
			intervals := make([]float64, 0, len(acc.starts)-1)
			for index := 1; index < len(acc.starts); index++ {
				interval := acc.starts[index].Sub(acc.starts[index-1]).Seconds()
				if interval < 0 || !isFiniteFloat(interval) {
					return wireTimingReferenceArtifact{}, fmt.Errorf("%s: proxy log produced invalid interval %v for %s", proxyLogPath, interval, key.Family)
				}
				intervals = append(intervals, interval)
			}
			item.LastIntervalSec = intervals[len(intervals)-1]
			item.MinIntervalSec = intervals[0]
			item.MaxIntervalSec = intervals[0]
			total := 0.0
			for _, interval := range intervals {
				total += interval
				if interval < item.MinIntervalSec {
					item.MinIntervalSec = interval
				}
				if interval > item.MaxIntervalSec {
					item.MaxIntervalSec = interval
				}
			}
			item.MeanIntervalSec = total / float64(len(intervals))
			familiesWithIntervals++
		}
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Family != items[j].Family {
			return items[i].Family < items[j].Family
		}
		if items[i].SourceBucket != items[j].SourceBucket {
			return items[i].SourceBucket < items[j].SourceBucket
		}
		if items[i].TargetBucket != items[j].TargetBucket {
			return items[i].TargetBucket < items[j].TargetBucket
		}
		if items[i].Primary != items[j].Primary {
			return items[i].Primary < items[j].Primary
		}
		return items[i].Secondary < items[j].Secondary
	})

	if familiesWithIntervals == 0 {
		return wireTimingReferenceArtifact{}, fmt.Errorf("%s: proxy log did not yield any periodicity intervals", proxyLogPath)
	}

	return wireTimingReferenceArtifact{
		Schema:     wireTimingReferenceArtifactSchema,
		CapturedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Source:     "proxy_log_session_send_plus_wire_rx",
		ClaimScope: "bounded_proof_window_timing_reference_source",
		OK:         true,
		Evidence: wireTimingReferenceEvidence{
			ProxyLogPath:             proxyLogPath,
			ProxyLogLineCount:        lineCount,
			SessionStartCount:        sessionStartCount,
			SendSymbolCount:          sendSymbolCount,
			WireSymbolCount:          wireSymbolCount,
			SyntheticSymbolSpacingMS: float64(proxyLogSyntheticSymbolSpacing) / float64(time.Millisecond),
			TimestampResolution:      "proxy_log_seconds_plus_symbol_spacing",
		},
		Summary: wireTimingReferenceSummary{
			ClassifiedEventCount:  transactionCount,
			TransactionCount:      transactionCount,
			MasterFrameCount:      0,
			AbandonedCount:        0,
			BusySecondsTotal:      busySecondsTotal,
			FamiliesWithIntervals: familiesWithIntervals,
		},
		Periodicity: items,
	}, nil
}

func loadProxyLogTransactions(path string) ([]proxyLogTransaction, int, int, int, int, error) {
	if strings.TrimSpace(path) == "" {
		return nil, 0, 0, 0, 0, fmt.Errorf("proxy log path is empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("open proxy log %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	sessionStartCount := 0
	sendSymbolCount := 0
	wireSymbolCount := 0
	var lastObservedAt time.Time
	transactions := make([]proxyLogTransaction, 0, 64)
	var current *proxyLogTransaction

	finalizeCurrent := func() error {
		if current == nil {
			return nil
		}
		if len(current.sendSymbols) == 0 {
			current = nil
			return nil
		}
		transactions = append(transactions, *current)
		current = nil
		return nil
	}

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()

		switch {
		case proxyLogSessionStartLineRE.MatchString(line):
			match := proxyLogSessionStartLineRE.FindStringSubmatch(line)
			observedAt, err := nextProxyLogObservedAt(match[1], lastObservedAt)
			if err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: parse start timestamp: %w", path, lineCount, err)
			}
			lastObservedAt = observedAt
			if err := finalizeCurrent(); err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, err
			}
			initiator, err := decodeProxyLogHexByte(match[3])
			if err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: parse initiator %q: %w", path, lineCount, match[3], err)
			}
			sessionStartCount++
			current = &proxyLogTransaction{
				lineNo:          lineCount,
				startObservedAt: observedAt,
				initiator:       initiator,
				sendSymbols:     make([]byte, 0, 16),
			}
		case proxyLogSessionSendLineRE.MatchString(line):
			match := proxyLogSessionSendLineRE.FindStringSubmatch(line)
			observedAt, err := nextProxyLogObservedAt(match[1], lastObservedAt)
			if err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: parse send timestamp: %w", path, lineCount, err)
			}
			lastObservedAt = observedAt
			if current == nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: send symbol without preceding session start", path, lineCount)
			}
			symbol, err := decodeProxyLogHexByte(match[3])
			if err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: parse send symbol %q: %w", path, lineCount, match[3], err)
			}
			current.sendSymbols = append(current.sendSymbols, symbol)
			sendSymbolCount++
		case proxyLogWireRXLineRE.MatchString(line):
			match := proxyLogWireRXLineRE.FindStringSubmatch(line)
			observedAt, err := nextProxyLogObservedAt(match[1], lastObservedAt)
			if err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: parse wire_rx timestamp: %w", path, lineCount, err)
			}
			lastObservedAt = observedAt
			if _, err := decodeProxyLogHexByte(match[2]); err != nil {
				return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: parse wire_rx symbol %q: %w", path, lineCount, match[2], err)
			}
			wireSymbolCount++
			if current != nil {
				current.wireSymbolCount++
				if current.firstWireAt.IsZero() {
					current.firstWireAt = observedAt
				}
				current.lastWireAt = observedAt
			}
		case strings.Contains(line, "wire_rx"):
			return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: malformed wire_rx line", path, lineCount)
		case strings.Contains(line, " start initiator="):
			return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: malformed session start line", path, lineCount)
		case strings.Contains(line, " send symbol="):
			return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s:%d: malformed session send line", path, lineCount)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("scan proxy log %s: %w", path, err)
	}
	if err := finalizeCurrent(); err != nil {
		return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, err
	}
	if len(transactions) == 0 {
		return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s: missing session start timing evidence", path)
	}
	if wireSymbolCount == 0 {
		return nil, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, fmt.Errorf("%s: missing wire_rx timing evidence", path)
	}
	return transactions, lineCount, sessionStartCount, sendSymbolCount, wireSymbolCount, nil
}

func (transaction proxyLogTransaction) requestFrame(path string) (protocol.Frame, bool, error) {
	if len(transaction.sendSymbols) == 0 {
		return protocol.Frame{}, false, nil
	}
	if transaction.sendSymbols[len(transaction.sendSymbols)-1] != protocol.SymbolSyn {
		return protocol.Frame{}, false, nil
	}
	requestSymbols := transaction.sendSymbols[:len(transaction.sendSymbols)-1]
	if len(requestSymbols) > 0 && requestSymbols[len(requestSymbols)-1] == protocol.SymbolAck {
		requestSymbols = requestSymbols[:len(requestSymbols)-1]
	}
	raw := make([]byte, 0, 1+len(requestSymbols))
	raw = append(raw, transaction.initiator)
	raw = append(raw, requestSymbols...)
	frame, ok := parseFrame(raw)
	if !ok {
		return protocol.Frame{}, false, fmt.Errorf("%s:%d: session send sequence did not decode to a valid initiator frame", path, transaction.lineNo)
	}
	return frame, true, nil
}

func nextProxyLogObservedAt(raw string, previous time.Time) (time.Time, error) {
	baseTime, err := time.ParseInLocation(proxyLogTimestampLayout, raw, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	if previous.IsZero() || baseTime.After(previous) {
		return baseTime, nil
	}
	return previous.Add(proxyLogSyntheticSymbolSpacing), nil
}

func decodeProxyLogHexByte(value string) (byte, error) {
	symbolBytes, err := hex.DecodeString(value)
	if err != nil || len(symbolBytes) != 1 {
		return 0, fmt.Errorf("invalid byte %q", value)
	}
	return symbolBytes[0], nil
}

func TestBuildWireTimingReferenceArtifactFromProxyLog(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.March, 28, 21, 0, 0, 0, time.UTC)
	requestB509 := protocol.Frame{
		Source:    0x31,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x24},
	}
	requestB524 := protocol.Frame{
		Source:    0x31,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x08, 0x10},
	}

	withTempFile(t, func(path string) {
		lines := make([]string, 0, 128)
		lines = append(lines, proxyLogLinesForTransaction(base, requestB509, []byte{0x11, 0x22})...)
		lines = append(lines, proxyLogLinesForTransaction(base.Add(10*time.Second), requestB509, []byte{0x33, 0x44})...)
		lines = append(lines, proxyLogLinesForTransaction(base.Add(20*time.Second), requestB524, []byte{0x55, 0x66})...)
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}

		artifact, err := buildWireTimingReferenceArtifactFromProxyLog(path)
		if err != nil {
			t.Fatalf("buildWireTimingReferenceArtifactFromProxyLog error = %v", err)
		}
		if !artifact.OK {
			t.Fatal("artifact.OK = false; want true")
		}
		if artifact.Schema != wireTimingReferenceArtifactSchema {
			t.Fatalf("schema = %q; want %q", artifact.Schema, wireTimingReferenceArtifactSchema)
		}
		if artifact.Source != "proxy_log_session_send_plus_wire_rx" {
			t.Fatalf("source = %q; want proxy_log_session_send_plus_wire_rx", artifact.Source)
		}
		if artifact.Evidence.SessionStartCount != 3 {
			t.Fatalf("session_start_count = %d; want 3", artifact.Evidence.SessionStartCount)
		}
		if artifact.Summary.TransactionCount != 3 {
			t.Fatalf("transaction_count = %d; want 3", artifact.Summary.TransactionCount)
		}
		if artifact.Summary.FamiliesWithIntervals != 1 {
			t.Fatalf("families_with_intervals = %d; want 1", artifact.Summary.FamiliesWithIntervals)
		}
		if artifact.Summary.BusySecondsTotal <= 0 {
			t.Fatalf("busy_seconds_total = %v; want > 0", artifact.Summary.BusySecondsTotal)
		}
		var b509 *wireTimingReferencePeriodicity
		for index := range artifact.Periodicity {
			if artifact.Periodicity[index].Family == "B509" {
				b509 = &artifact.Periodicity[index]
				break
			}
		}
		if b509 == nil {
			t.Fatalf("periodicity = %+v; want B509 item", artifact.Periodicity)
		}
		if b509.SourceBucket != "0x31" || b509.TargetBucket != "0x15" {
			t.Fatalf("B509 buckets = %s -> %s; want 0x31 -> 0x15", b509.SourceBucket, b509.TargetBucket)
		}
		if b509.SampleCount != 2 {
			t.Fatalf("B509 sample_count = %d; want 2", b509.SampleCount)
		}
		if diff := math.Abs(b509.LastIntervalSec - 10.0); diff > 0.5 {
			t.Fatalf("B509 last_interval_sec = %v; want ~10s", b509.LastIntervalSec)
		}
	})
}

func TestBuildWireTimingReferenceArtifactFailsOnMissingProxyLog(t *testing.T) {
	t.Parallel()

	_, err := buildWireTimingReferenceArtifactFromProxyLog("/definitely/missing/proxy.log")
	if err == nil {
		t.Fatal("missing proxy log error = nil; want error")
	}
}

func TestBuildWireTimingReferenceArtifactFailsOnMalformedWireRXLine(t *testing.T) {
	t.Parallel()

	withTempFile(t, func(path string) {
		lines := []string{
			"2026/03/28 21:00:00 session=1 start initiator=0x31",
			"2026/03/28 21:00:00 session=1 send symbol=0x15",
			"2026/03/28 21:00:00 session=1 send symbol=0xB5",
			"2026/03/28 21:00:00 session=1 send symbol=0x09",
			"2026/03/28 21:00:00 session=1 send symbol=0x01",
			"2026/03/28 21:00:00 session=1 send symbol=0x24",
			"2026/03/28 21:00:00 session=1 send symbol=0xD3",
			"2026/03/28 21:00:00 session=1 send symbol=0x00",
			"2026/03/28 21:00:00 session=1 send symbol=0xAA",
			"2026/03/28 21:00:00 wire_rx nope",
		}
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}
		_, err := buildWireTimingReferenceArtifactFromProxyLog(path)
		if err == nil {
			t.Fatal("malformed proxy log error = nil; want error")
		}
		if !strings.Contains(err.Error(), "malformed wire_rx line") {
			t.Fatalf("malformed proxy log error = %v; want malformed wire_rx line", err)
		}
	})
}

func TestBuildWireTimingReferenceArtifactFailsOnMalformedSessionSendLine(t *testing.T) {
	t.Parallel()

	withTempFile(t, func(path string) {
		lines := []string{
			"2026/03/28 21:00:00 session=1 start initiator=0x31",
			"2026/03/28 21:00:00 session=1 send symbol=nope",
			"2026/03/28 21:00:00 wire_rx symbol=0xAA",
		}
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}
		_, err := buildWireTimingReferenceArtifactFromProxyLog(path)
		if err == nil {
			t.Fatal("malformed session send error = nil; want error")
		}
		if !strings.Contains(err.Error(), "malformed session send line") {
			t.Fatalf("malformed session send error = %v; want malformed session send line", err)
		}
	})
}

func transactionPayload(request protocol.Frame, responseData []byte) []byte {
	payload := append([]byte{}, frameBytes(request)...)
	payload = append(payload, protocol.SymbolAck)
	payload = append(payload, responseSegmentBytes(responseData)...)
	payload = append(payload, protocol.SymbolAck, protocol.SymbolSyn)
	return payload
}

func proxyLogLinesForTransaction(at time.Time, request protocol.Frame, responseData []byte) []string {
	lines := make([]string, 0, 32)
	prefix := at.UTC().Format(proxyLogTimestampLayout)
	lines = append(lines, fmt.Sprintf("%s session=1 start initiator=0x%02X", prefix, request.Source))
	sendSymbols := frameBytes(request)
	sendSymbols = append(sendSymbols[1:len(sendSymbols)-1], protocol.SymbolAck, protocol.SymbolSyn)
	for _, symbol := range sendSymbols {
		lines = append(lines, fmt.Sprintf("%s session=1 send symbol=0x%02X", prefix, symbol))
	}
	for _, symbol := range transactionPayload(request, responseData) {
		lines = append(lines, fmt.Sprintf("%s wire_rx symbol=0x%02X", prefix, symbol))
	}
	return lines
}

func withTempFile(t *testing.T, fn func(path string)) {
	t.Helper()
	dir := t.TempDir()
	fn(filepath.Join(dir, "proxy.log"))
}

func isFiniteFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
