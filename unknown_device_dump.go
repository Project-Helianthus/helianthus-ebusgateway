package ebusgateway

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
)

type DumpBus interface {
	Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error)
}

type ExtRegisterRequest struct {
	Opcode   byte
	Group    byte
	Instance byte
	Addr     uint16
}

type UnknownDeviceDumpOptions struct {
	OutputDir      string
	UploadURL      string
	IncludePII     bool
	IncludeTraffic bool
	TrafficWindow  time.Duration
	SourceAddress  byte
	B509Addresses  []uint16
	B524Requests   []ExtRegisterRequest
	Logger         *log.Logger
	Now            func() time.Time
}

type UnknownDeviceDumpResult struct {
	Address        byte
	BundlePath     string
	ManifestPath   string
	Uploaded       bool
	UploadURL      string
	UploadStatus   string
	UploadHTTPCode int
	Error          string
}

func (g *Gateway) DumpUnknownDevices(ctx context.Context, entries []registry.DeviceEntry, opts UnknownDeviceDumpOptions) ([]UnknownDeviceDumpResult, error) {
	if g == nil || g.Bus == nil {
		return nil, fmt.Errorf("unknown dump missing gateway bus: %w", ebuserrors.ErrInvalidPayload)
	}
	return DumpUnknownDevices(ctx, g.Bus, entries, opts)
}

func DumpUnknownDevices(ctx context.Context, bus DumpBus, entries []registry.DeviceEntry, opts UnknownDeviceDumpOptions) ([]UnknownDeviceDumpResult, error) {
	if bus == nil {
		return nil, fmt.Errorf("unknown dump missing bus: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeUnknownDumpOptions(opts)

	unknown := filterUnknownEntries(entries)
	if len(unknown) == 0 {
		return nil, nil
	}

	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return nil, err
	}

	results := make([]UnknownDeviceDumpResult, 0, len(unknown))
	for _, entry := range unknown {
		if ctx.Err() != nil {
			return results, ctx.Err()
		}
		result := dumpUnknownDevice(ctx, bus, entry, opts)
		results = append(results, result)
	}
	return results, nil
}

func normalizeUnknownDumpOptions(opts UnknownDeviceDumpOptions) UnknownDeviceDumpOptions {
	if opts.OutputDir == "" {
		opts.OutputDir = DefaultConfig().DumpOutputDir
	}
	if opts.SourceAddress == 0 {
		opts.SourceAddress = defaultSmokeSource
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.TrafficWindow <= 0 {
		opts.TrafficWindow = 10 * time.Second
	}
	if len(opts.B509Addresses) == 0 {
		opts.B509Addresses = defaultB509Addresses()
	}
	if len(opts.B524Requests) == 0 {
		opts.B524Requests = defaultB524Requests()
	}
	return opts
}

func filterUnknownEntries(entries []registry.DeviceEntry) []registry.DeviceEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]registry.DeviceEntry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if len(entry.Planes()) == 0 {
			out = append(out, entry)
		}
	}
	return out
}

func dumpUnknownDevice(ctx context.Context, bus DumpBus, entry registry.DeviceEntry, opts UnknownDeviceDumpOptions) UnknownDeviceDumpResult {
	result := UnknownDeviceDumpResult{
		Address: entry.Address(),
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "dump: ", log.LstdFlags)
	}

	bundleID := newBundleID()
	createdAt := opts.Now().UTC()
	deviceLabel := fmt.Sprintf("0x%02x", entry.Address())
	tempDir, err := os.MkdirTemp(opts.OutputDir, "bundle-"+strings.TrimPrefix(deviceLabel, "0x")+"-")
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer os.RemoveAll(tempDir)

	var traffic []trafficFrame
	recordTraffic := func(record trafficFrame) {
		if !opts.IncludeTraffic {
			return
		}
		traffic = append(traffic, record)
	}

	identify := runIdentifyDump(ctx, bus, entry.Address(), opts.SourceAddress, opts, recordTraffic)
	b509 := runB509Dump(ctx, bus, entry.Address(), opts.SourceAddress, opts, recordTraffic)
	b524 := runB524Dump(ctx, bus, entry.Address(), opts.SourceAddress, opts, recordTraffic)

	files := make([]dumpFileInfo, 0, 4)

	identifyPath := filepath.Join(tempDir, "identify.json")
	if err := writeJSONFile(identifyPath, identify); err == nil {
		if info, err := statDumpFile(identifyPath, "identify", "identify response and parsed fields"); err == nil {
			files = append(files, info)
		}
	}

	b509Path := filepath.Join(tempDir, "b509_registers.json")
	if err := writeJSONFile(b509Path, b509); err == nil {
		if info, err := statDumpFile(b509Path, "b509", "B5 09 raw register reads"); err == nil {
			files = append(files, info)
		}
	}

	b524Path := filepath.Join(tempDir, "b524_registers.json")
	if err := writeJSONFile(b524Path, b524); err == nil {
		if info, err := statDumpFile(b524Path, "b524", "B5 24 raw extended register reads"); err == nil {
			files = append(files, info)
		}
	}

	var trafficPath string
	if opts.IncludeTraffic {
		trafficDump := trafficCapture{
			WindowSeconds: int(opts.TrafficWindow.Seconds()),
			Frames:        redactTrafficFrames(traffic, opts.IncludePII),
		}
		trafficPath = filepath.Join(tempDir, "traffic.json")
		if err := writeJSONFile(trafficPath, trafficDump); err == nil {
			if info, err := statDumpFile(trafficPath, "traffic", "frame-level traffic capture with timestamps"); err == nil {
				files = append(files, info)
			}
		}
	}

	manifest := buildManifest(entry, bundleID, createdAt, opts, identify, b509, b524, traffic, files)
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := writeJSONFile(manifestPath, manifest); err != nil {
		result.Error = err.Error()
		return result
	}
	if info, err := statDumpFile(manifestPath, "manifest", "bundle manifest and privacy notes"); err == nil {
		manifest.Files = append([]dumpFileInfo{info}, files...)
		if err := writeJSONFile(manifestPath, manifest); err != nil {
			result.Error = err.Error()
			return result
		}
	}

	bundleName := fmt.Sprintf("unknown_device_%s_%s.zip", strings.TrimPrefix(deviceLabel, "0x"), bundleID[:8])
	bundlePath := filepath.Join(opts.OutputDir, bundleName)
	if err := writeBundleZip(bundlePath, manifestPath, files); err != nil {
		result.Error = err.Error()
		return result
	}

	manifestCopyName := fmt.Sprintf("unknown_device_%s_%s_manifest.json", strings.TrimPrefix(deviceLabel, "0x"), bundleID[:8])
	manifestCopyPath := filepath.Join(opts.OutputDir, manifestCopyName)
	if err := copyFile(manifestPath, manifestCopyPath); err != nil {
		result.Error = err.Error()
		return result
	}

	result.BundlePath = bundlePath
	result.ManifestPath = manifestCopyPath

	if strings.TrimSpace(opts.UploadURL) == "" {
		logger.Printf("unknown device bundle written: %s", bundlePath)
		logger.Printf("manifest available at: %s (review before uploading)", manifestCopyPath)
		logger.Printf("set DumpUploadURL to upload this bundle for analysis")
		return result
	}

	uploadStatus, httpCode, err := uploadDumpBundle(ctx, opts.UploadURL, bundlePath, manifest)
	result.UploadURL = opts.UploadURL
	if err != nil {
		result.Error = err.Error()
		result.UploadStatus = uploadStatus
		result.UploadHTTPCode = httpCode
		logger.Printf("unknown device bundle upload failed: %v", err)
		return result
	}

	result.Uploaded = true
	result.UploadStatus = uploadStatus
	result.UploadHTTPCode = httpCode
	logger.Printf("unknown device bundle uploaded: %s (status %s)", bundlePath, uploadStatus)
	return result
}

func defaultB509Addresses() []uint16 {
	return []uint16{0x2800, 0x2820, 0x2840, 0x2860, 0x2880, 0x28A0, 0x28C0, 0x28E0}
}

func defaultB524Requests() []ExtRegisterRequest {
	reqs := make([]ExtRegisterRequest, 0, 8)
	for addr := uint16(0x0000); addr < 0x0008; addr++ {
		reqs = append(reqs, ExtRegisterRequest{
			Opcode:   0x02,
			Group:    0x00,
			Instance: 0x00,
			Addr:     addr,
		})
	}
	return reqs
}

type identifyDump struct {
	Request   frameSnapshot     `json:"request"`
	Response  frameSnapshot     `json:"response,omitempty"`
	Parsed    map[string]string `json:"parsed,omitempty"`
	Error     string            `json:"error,omitempty"`
	Redacted  bool              `json:"redacted"`
	Timestamp string            `json:"timestamp"`
	Notes     []string          `json:"notes,omitempty"`
}

type registerDump struct {
	Requests []registerRead `json:"requests"`
}

type registerRead struct {
	Timestamp string        `json:"timestamp"`
	Request   frameSnapshot `json:"request"`
	Response  frameSnapshot `json:"response,omitempty"`
	Error     string        `json:"error,omitempty"`
}

type trafficCapture struct {
	WindowSeconds int            `json:"window_seconds"`
	Frames        []trafficFrame `json:"frames"`
}

type trafficFrame struct {
	Timestamp string        `json:"timestamp"`
	Dir       string        `json:"dir"`
	Frame     frameSnapshot `json:"frame"`
	Error     string        `json:"error,omitempty"`
}

type frameSnapshot struct {
	Source    byte   `json:"source"`
	Target    byte   `json:"target"`
	Primary   byte   `json:"primary"`
	Secondary byte   `json:"secondary"`
	Data      string `json:"data"`
}

func runIdentifyDump(ctx context.Context, bus DumpBus, target, source byte, opts UnknownDeviceDumpOptions, recordTraffic func(trafficFrame)) identifyDump {
	request := protocol.Frame{
		Source:    source,
		Target:    target,
		Primary:   0x07,
		Secondary: 0x04,
	}
	timestamp := opts.Now().UTC().Format(time.RFC3339Nano)
	recordTraffic(trafficFrame{
		Timestamp: timestamp,
		Dir:       "tx",
		Frame:     snapshotFrame(request, false),
	})

	response, err := bus.Send(ctx, request)
	if err != nil {
		recordTraffic(trafficFrame{
			Timestamp: opts.Now().UTC().Format(time.RFC3339Nano),
			Dir:       "rx",
			Frame:     frameSnapshot{Source: target, Target: source, Primary: 0x07, Secondary: 0x04, Data: ""},
			Error:     err.Error(),
		})
		return identifyDump{
			Request:   snapshotFrame(request, opts.IncludePII),
			Error:     err.Error(),
			Redacted:  !opts.IncludePII,
			Timestamp: timestamp,
		}
	}
	if response == nil {
		return identifyDump{
			Request:   snapshotFrame(request, opts.IncludePII),
			Error:     "empty response",
			Redacted:  !opts.IncludePII,
			Timestamp: timestamp,
		}
	}

	payload := response.Data
	parsed, parsedErr := decodeDeviceInfoPayload(payload)
	if parsedErr != nil {
		if parsed == nil {
			parsed = map[string]string{}
		}
		parsed["decode_error"] = parsedErr.Error()
	}

	redactedPayload := payload
	if !opts.IncludePII {
		redactedPayload = redactIdentifyPayload(payload)
	}

	responseFrame := *response
	responseFrame.Data = redactedPayload
	recordTraffic(trafficFrame{
		Timestamp: opts.Now().UTC().Format(time.RFC3339Nano),
		Dir:       "rx",
		Frame:     snapshotFrame(responseFrame, opts.IncludePII),
	})

	return identifyDump{
		Request:   snapshotFrame(request, opts.IncludePII),
		Response:  snapshotFrame(responseFrame, opts.IncludePII),
		Parsed:    redactParsedDeviceInfo(parsed, opts.IncludePII),
		Redacted:  !opts.IncludePII,
		Timestamp: timestamp,
	}
}

func runB509Dump(ctx context.Context, bus DumpBus, target, source byte, opts UnknownDeviceDumpOptions, recordTraffic func(trafficFrame)) registerDump {
	dump := registerDump{Requests: make([]registerRead, 0, len(opts.B509Addresses))}
	for _, addr := range opts.B509Addresses {
		request := protocol.Frame{
			Source:    source,
			Target:    target,
			Primary:   0xB5,
			Secondary: 0x09,
			Data:      []byte{0x0D, byte(addr >> 8), byte(addr)},
		}
		ts := opts.Now().UTC().Format(time.RFC3339Nano)
		recordTraffic(trafficFrame{Timestamp: ts, Dir: "tx", Frame: snapshotFrame(request, opts.IncludePII)})

		response, err := bus.Send(ctx, request)
		read := registerRead{
			Timestamp: ts,
			Request:   snapshotFrame(request, opts.IncludePII),
		}
		if err != nil {
			read.Error = err.Error()
			recordTraffic(trafficFrame{
				Timestamp: opts.Now().UTC().Format(time.RFC3339Nano),
				Dir:       "rx",
				Frame:     frameSnapshot{Source: target, Target: source, Primary: 0xB5, Secondary: 0x09, Data: ""},
				Error:     err.Error(),
			})
		} else if response != nil {
			read.Response = snapshotFrame(*response, opts.IncludePII)
			recordTraffic(trafficFrame{Timestamp: opts.Now().UTC().Format(time.RFC3339Nano), Dir: "rx", Frame: snapshotFrame(*response, opts.IncludePII)})
		} else {
			read.Error = "empty response"
		}
		dump.Requests = append(dump.Requests, read)
	}
	return dump
}

func runB524Dump(ctx context.Context, bus DumpBus, target, source byte, opts UnknownDeviceDumpOptions, recordTraffic func(trafficFrame)) registerDump {
	dump := registerDump{Requests: make([]registerRead, 0, len(opts.B524Requests))}
	for _, req := range opts.B524Requests {
		opcode := req.Opcode
		if opcode == 0x00 {
			opcode = 0x02
		}
		request := protocol.Frame{
			Source:    source,
			Target:    target,
			Primary:   0xB5,
			Secondary: 0x24,
			Data:      []byte{opcode, 0x00, req.Group, req.Instance, byte(req.Addr), byte(req.Addr >> 8)},
		}
		if opcode == 0x01 {
			dump.Requests = append(dump.Requests, registerRead{
				Timestamp: opts.Now().UTC().Format(time.RFC3339Nano),
				Request:   snapshotFrame(request, opts.IncludePII),
				Error:     "opcode 0x01 is static-only and must not be queried on runtime bus",
			})
			continue
		}
		ts := opts.Now().UTC().Format(time.RFC3339Nano)
		recordTraffic(trafficFrame{Timestamp: ts, Dir: "tx", Frame: snapshotFrame(request, opts.IncludePII)})

		response, err := bus.Send(ctx, request)
		read := registerRead{
			Timestamp: ts,
			Request:   snapshotFrame(request, opts.IncludePII),
		}
		if err != nil {
			read.Error = err.Error()
			recordTraffic(trafficFrame{
				Timestamp: opts.Now().UTC().Format(time.RFC3339Nano),
				Dir:       "rx",
				Frame:     frameSnapshot{Source: target, Target: source, Primary: 0xB5, Secondary: 0x24, Data: ""},
				Error:     err.Error(),
			})
		} else if response != nil {
			read.Response = snapshotFrame(*response, opts.IncludePII)
			recordTraffic(trafficFrame{Timestamp: opts.Now().UTC().Format(time.RFC3339Nano), Dir: "rx", Frame: snapshotFrame(*response, opts.IncludePII)})
		} else {
			read.Error = "empty response"
		}
		dump.Requests = append(dump.Requests, read)
	}
	return dump
}

func snapshotFrame(frame protocol.Frame, includePII bool) frameSnapshot {
	data := frame.Data
	if !includePII && frame.Primary == 0x07 && frame.Secondary == 0x04 {
		data = redactIdentifyPayload(frame.Data)
	}
	return frameSnapshot{
		Source:    frame.Source,
		Target:    frame.Target,
		Primary:   frame.Primary,
		Secondary: frame.Secondary,
		Data:      hex.EncodeToString(data),
	}
}

func redactIdentifyPayload(payload []byte) []byte {
	redacted := append([]byte(nil), payload...)
	if len(redacted) < 6 {
		return redacted
	}
	for idx := 1; idx <= 5 && idx < len(redacted); idx++ {
		redacted[idx] = 0x00
	}
	return redacted
}

func redactParsedDeviceInfo(parsed map[string]string, includePII bool) map[string]string {
	if includePII || parsed == nil {
		return parsed
	}
	out := make(map[string]string, len(parsed))
	for key, value := range parsed {
		out[key] = value
	}
	if _, ok := out["device_id"]; ok {
		out["device_id"] = ""
	}
	return out
}

func redactTrafficFrames(frames []trafficFrame, includePII bool) []trafficFrame {
	if includePII || len(frames) == 0 {
		return frames
	}
	out := make([]trafficFrame, len(frames))
	for i, frame := range frames {
		out[i] = frame
		if frame.Frame.Primary == 0x07 && frame.Frame.Secondary == 0x04 {
			data, _ := hex.DecodeString(frame.Frame.Data)
			out[i].Frame.Data = hex.EncodeToString(redactIdentifyPayload(data))
		}
	}
	return out
}

type dumpFileInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Sha256      string `json:"sha256"`
	Size        int64  `json:"size_bytes"`
	Description string `json:"description"`
}

type dumpManifest struct {
	SchemaVersion int             `json:"schema_version"`
	BundleID      string          `json:"bundle_id"`
	CreatedAt     string          `json:"created_at"`
	UnknownReason string          `json:"unknown_reason"`
	Device        manifestDevice  `json:"device"`
	Privacy       privacyManifest `json:"privacy"`
	Captures      captureSummary  `json:"captures"`
	Files         []dumpFileInfo  `json:"files"`
}

type manifestDevice struct {
	Address         string `json:"address"`
	Manufacturer    string `json:"manufacturer"`
	DeviceID        string `json:"device_id,omitempty"`
	DeviceIDHash    string `json:"device_id_hash,omitempty"`
	SerialNumber    string `json:"serial_number,omitempty"`
	SerialHash      string `json:"serial_number_hash,omitempty"`
	MacAddress      string `json:"mac_address,omitempty"`
	MacHash         string `json:"mac_address_hash,omitempty"`
	SoftwareVersion string `json:"software_version"`
	HardwareVersion string `json:"hardware_version"`
}

type privacyManifest struct {
	IncludePII   bool     `json:"include_pii"`
	Redactions   []string `json:"redactions"`
	Minimization string   `json:"minimization"`
	Review       string   `json:"review"`
	Retention    string   `json:"retention"`
	Regulations  []string `json:"regulations"`
	OptInHint    string   `json:"opt_in_hint"`
}

type captureSummary struct {
	Identify      bool `json:"identify"`
	B509Reads     int  `json:"b509_reads"`
	B524Reads     int  `json:"b524_reads"`
	TrafficFrames int  `json:"traffic_frames"`
}

func buildManifest(entry registry.DeviceEntry, bundleID string, createdAt time.Time, opts UnknownDeviceDumpOptions, identify identifyDump, b509 registerDump, b524 registerDump, traffic []trafficFrame, files []dumpFileInfo) dumpManifest {
	device := manifestDevice{
		Address:         fmt.Sprintf("0x%02x", entry.Address()),
		Manufacturer:    entry.Manufacturer(),
		DeviceID:        entry.DeviceID(),
		SerialNumber:    entry.SerialNumber(),
		MacAddress:      entry.MacAddress(),
		SoftwareVersion: entry.SoftwareVersion(),
		HardwareVersion: entry.HardwareVersion(),
	}
	redactions := make([]string, 0)
	if !opts.IncludePII {
		if device.DeviceID != "" {
			device.DeviceIDHash = hashString(device.DeviceID)
			device.DeviceID = ""
			redactions = append(redactions, "device_id")
		}
		if device.SerialNumber != "" {
			device.SerialHash = hashString(device.SerialNumber)
			device.SerialNumber = ""
			redactions = append(redactions, "serial_number")
		}
		if device.MacAddress != "" {
			device.MacHash = hashString(device.MacAddress)
			device.MacAddress = ""
			redactions = append(redactions, "mac_address")
		}
	}

	return dumpManifest{
		SchemaVersion: 1,
		BundleID:      bundleID,
		CreatedAt:     createdAt.Format(time.RFC3339Nano),
		UnknownReason: "no plane providers matched device info",
		Device:        device,
		Privacy: privacyManifest{
			IncludePII:   opts.IncludePII,
			Redactions:   redactions,
			Minimization: "bundle contains identify response, sampled registers, and optional traffic window",
			Review:       "review manifest.json before sharing; upload is opt-in",
			Retention:    "retain for the shortest time needed; recommended <= 7 days",
			Regulations:  []string{"GDPR", "CCPA"},
			OptInHint:    "set DumpIncludePII=true to include identifiers",
		},
		Captures: captureSummary{
			Identify:      identify.Error == "",
			B509Reads:     len(b509.Requests),
			B524Reads:     len(b524.Requests),
			TrafficFrames: len(traffic),
		},
		Files: files,
	}
}

func newBundleID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

func writeJSONFile(path string, payload any) error {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func statDumpFile(path, fileType, description string) (dumpFileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		return dumpFileInfo{}, err
	}
	hash, err := hashFile(path)
	if err != nil {
		return dumpFileInfo{}, err
	}
	return dumpFileInfo{
		Name:        filepath.Base(path),
		Type:        fileType,
		Sha256:      hash,
		Size:        info.Size(),
		Description: description,
	}, nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func writeBundleZip(bundlePath, manifestPath string, files []dumpFileInfo) error {
	zipFile, err := os.Create(bundlePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	archive := zip.NewWriter(zipFile)
	if err := addFileToZip(archive, manifestPath); err != nil {
		_ = archive.Close()
		return err
	}
	for _, file := range files {
		if file.Name == filepath.Base(manifestPath) {
			continue
		}
		path := filepath.Join(filepath.Dir(manifestPath), file.Name)
		if err := addFileToZip(archive, path); err != nil {
			_ = archive.Close()
			return err
		}
	}
	return archive.Close()
}

func addFileToZip(archive *zip.Writer, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	header.Name = filepath.Base(path)
	writer, err := archive.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, file)
	return err
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func uploadDumpBundle(ctx context.Context, uploadURL string, bundlePath string, manifest dumpManifest) (string, int, error) {
	metadata := map[string]any{
		"bundle_id":   manifest.BundleID,
		"address":     manifest.Device.Address,
		"created_at":  manifest.CreatedAt,
		"include_pii": manifest.Privacy.IncludePII,
	}

	bodyReader, contentType, err := buildMultipart(bundlePath, manifest, metadata)
	if err != nil {
		return "multipart_error", 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bodyReader)
	if err != nil {
		return "request_error", 0, err
	}
	req.Header.Set("Content-Type", contentType)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "network_error", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.Status, resp.StatusCode, fmt.Errorf("upload status %s", resp.Status)
	}
	return resp.Status, resp.StatusCode, nil
}

func buildMultipart(bundlePath string, manifest dumpManifest, metadata map[string]any) (io.Reader, string, error) {
	reader, writer := io.Pipe()
	multipartWriter := newMultipartWriter(writer)

	go func() {
		defer writer.Close()
		defer multipartWriter.Close()

		if err := multipartWriter.WriteJSONPart("manifest", manifest); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := multipartWriter.WriteJSONPart("metadata", metadata); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := multipartWriter.WriteFilePart("bundle", bundlePath, "application/zip"); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
	}()

	return reader, multipartWriter.ContentType(), nil
}

type multipartWriter struct {
	writer *multipart.Writer
}

func newMultipartWriter(w io.Writer) *multipartWriter {
	return &multipartWriter{writer: multipart.NewWriter(w)}
}

func (m *multipartWriter) Close() error {
	return m.writer.Close()
}

func (m *multipartWriter) ContentType() string {
	return m.writer.FormDataContentType()
}

func (m *multipartWriter) WriteJSONPart(name string, payload any) error {
	part, err := m.writer.CreateFormFile(name, name+".json")
	if err != nil {
		return err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = part.Write(data)
	return err
}

func (m *multipartWriter) WriteFilePart(name, path, contentType string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	part, err := m.writer.CreateFormFile(name, filepath.Base(path))
	if err != nil {
		return err
	}
	_, err = io.Copy(part, file)
	return err
}
