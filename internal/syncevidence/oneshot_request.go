package syncevidence

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	OneShotRequestContractV1 = "helianthus.platform.synchronized-evidence.one-shot-request.v1"
	OneShotRequestFileV1     = "one-shot-request-v1.json"

	oneShotRequestSchemaDigest = "0a17427656c4a5b81b9f7aceb3d6c1f3445a626c7ce2846953e5ccd29a774227"
)

type OneShotCloudActionV1 struct {
	EvidenceRef        EvidenceRefV1   `json:"evidence_ref"`
	NormalizedEvidence json.RawMessage `json:"normalized_evidence"`
}

type OneShotRequestV1 struct {
	Contract          string               `json:"contract"`
	SchemaVersion     uint64               `json:"schema_version"`
	ActionEvidenceRef EvidenceRefV1        `json:"action_evidence_ref"`
	CloudAppAction    OneShotCloudActionV1 `json:"cloud_app_action"`
}

type oneShotFileIdentity struct {
	device    uint64
	inode     uint64
	mode      uint32
	uid       uint32
	links     uint64
	size      int64
	mtimeSec  int64
	mtimeNSec int64
	ctimeSec  int64
	ctimeNSec int64
}

func loadOneShotRequestAt(root string, afterFirstRead func()) (OneShotRequestV1, error) {
	return loadOneShotRequestAtWithHooks(root, nil, afterFirstRead)
}

func loadOneShotRequestAtWithHooks(
	root string,
	afterInitialStat func(),
	afterFirstRead func(),
) (OneShotRequestV1, error) {
	if digestHex(mustReadContract("synchronized-evidence-one-shot-control-v1.schema.json")) != oneShotRequestSchemaDigest {
		panic("syncevidence: pinned one-shot request schema digest mismatch")
	}
	parent, err := openOneShotRequestDirectory(root)
	if err != nil {
		return OneShotRequestV1{}, err
	}
	defer func() {
		_ = parent.Close()
	}()
	return loadOneShotRequestFromDirectoryWithHooks(parent, afterInitialStat, afterFirstRead)
}

func loadOneShotRequestFromDirectory(parent *os.File, afterFirstRead func()) (OneShotRequestV1, error) {
	return loadOneShotRequestFromDirectoryWithHooks(parent, nil, afterFirstRead)
}

func loadOneShotRequestFromDirectoryWithHooks(
	parent *os.File,
	afterInitialStat func(),
	afterFirstRead func(),
) (OneShotRequestV1, error) {
	if parent == nil || verifyDirectoryFD(int(parent.Fd())) != nil {
		return OneShotRequestV1{}, ErrUnsafeStore
	}
	fd, err := unix.Openat(
		int(parent.Fd()),
		OneShotRequestFileV1,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	file := os.NewFile(uintptr(fd), OneShotRequestFileV1)
	defer func() {
		_ = file.Close()
	}()

	before, err := verifiedOneShotRequestIdentity(fd)
	if err != nil {
		return OneShotRequestV1{}, err
	}
	if afterInitialStat != nil {
		afterInitialStat()
	}
	first, err := readOneShotRequest(file)
	if err != nil {
		return OneShotRequestV1{}, err
	}
	if afterFirstRead != nil {
		afterFirstRead()
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	second, err := readOneShotRequest(file)
	if err != nil {
		return OneShotRequestV1{}, err
	}
	after, err := verifiedOneShotRequestIdentity(fd)
	if err != nil {
		return OneShotRequestV1{}, err
	}
	var linked unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), OneShotRequestFileV1, &linked, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	linkedIdentity, err := oneShotRequestIdentity(linked)
	if err != nil || before != after || before != linkedIdentity || !bytes.Equal(first, second) {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	return parseOneShotRequest(second)
}

func openOneShotRequestDirectory(root string) (*os.File, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, ErrInvalidArgument
	}
	components := strings.Split(strings.TrimPrefix(root, string(filepath.Separator)), string(filepath.Separator))
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrInvalidArgument
	}
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(fd)
			return nil, ErrInvalidArgument
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, ErrInvalidArgument
		}
		fd = next
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil ||
		stat.Mode&unix.S_IFMT != unix.S_IFDIR ||
		uint32(stat.Mode)&0o777 != 0o700 ||
		stat.Uid != uint32(os.Geteuid()) {
		_ = unix.Close(fd)
		return nil, ErrUnsafeStore
	}
	return os.NewFile(uintptr(fd), root), nil
}

func verifiedOneShotRequestIdentity(fd int) (oneShotFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return oneShotFileIdentity{}, ErrInvalidArgument
	}
	return oneShotRequestIdentity(stat)
}

func oneShotRequestIdentity(stat unix.Stat_t) (oneShotFileIdentity, error) {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		uint32(stat.Mode)&0o777 != 0o600 ||
		stat.Uid != uint32(os.Geteuid()) ||
		uint64(stat.Nlink) != 1 ||
		stat.Size < 0 {
		return oneShotFileIdentity{}, ErrUnsafeStore
	}
	mtimeSec, mtimeNSec, ctimeSec, ctimeNSec := oneShotRequestChangeTimes(stat)
	return oneShotFileIdentity{
		device:    uint64(stat.Dev),
		inode:     uint64(stat.Ino),
		mode:      uint32(stat.Mode),
		uid:       stat.Uid,
		links:     uint64(stat.Nlink),
		size:      stat.Size,
		mtimeSec:  mtimeSec,
		mtimeNSec: mtimeNSec,
		ctimeSec:  ctimeSec,
		ctimeNSec: ctimeNSec,
	}, nil
}

func readOneShotRequest(file *os.File) ([]byte, error) {
	maximum := int64(DefaultLimitsV1().MaxArtifactBytes)
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > maximum {
		return nil, ErrInvalidArgument
	}
	return raw, nil
}

func parseOneShotRequest(raw []byte) (OneShotRequestV1, error) {
	value, _, err := parseJSON(raw, DefaultLimitsV1(), false)
	if err != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	root, ok := value.(map[string]any)
	if !ok || !exactKeys(root, "contract", "schema_version", "action_evidence_ref", "cloud_app_action") ||
		root["contract"] != OneShotRequestContractV1 ||
		root["schema_version"] != json.Number("1") {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	actionMap, actionOK := root["action_evidence_ref"].(map[string]any)
	cloudMap, cloudOK := root["cloud_app_action"].(map[string]any)
	if !actionOK || !cloudOK || !exactKeys(cloudMap, "evidence_ref", "normalized_evidence") {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	cloudRefMap, refOK := cloudMap["evidence_ref"].(map[string]any)
	normalized, evidenceOK := cloudMap["normalized_evidence"].(map[string]any)
	if !refOK || !evidenceOK {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	actionRef, actionCanonical, err := parseOneShotEvidenceRef(actionMap)
	if err != nil {
		return OneShotRequestV1{}, err
	}
	cloudRef, cloudCanonical, err := parseOneShotEvidenceRef(cloudRefMap)
	if err != nil || !bytes.Equal(actionCanonical, cloudCanonical) || !reflect.DeepEqual(actionRef, cloudRef) {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	normalizedCanonical, err := canonicalJSONValue(normalized)
	if err != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	contentDigest := HashContentBytes(normalizedCanonical)
	if actionRef.Digest != contentDigest || cloudRef.Digest != contentDigest {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	observedAtText, ok := normalized["source_observed_at"].(string)
	if !ok || !canonicalTimestamp(observedAtText) {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	observedAt, err := time.Parse(time.RFC3339Nano, observedAtText)
	if err != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	authority, ok := boundSourceAuthority(
		SourceCloudApp,
		"helianthus.cloud-app.precaptured.evidence.v1",
		1,
	)
	if !ok || validateSourcePayload(normalized, sourceCapture{
		sourceKind:          SourceCloudApp,
		sourceContract:      authority.contract,
		sourceSchemaVersion: authority.version,
		sourceObservedAt:    observedAt,
	}) != nil {
		return OneShotRequestV1{}, ErrInvalidArgument
	}
	return OneShotRequestV1{
		Contract:          OneShotRequestContractV1,
		SchemaVersion:     1,
		ActionEvidenceRef: actionRef,
		CloudAppAction: OneShotCloudActionV1{
			EvidenceRef:        cloudRef,
			NormalizedEvidence: normalizedCanonical,
		},
	}, nil
}

func parseOneShotEvidenceRef(value map[string]any) (EvidenceRefV1, []byte, error) {
	if !exactKeys(value, "kind", "digest_algorithm", "digest", "repository", "commit", "path") {
		return EvidenceRefV1{}, nil, ErrInvalidArgument
	}
	canonical, err := canonicalJSONValue(value)
	if err != nil {
		return EvidenceRefV1{}, nil, ErrInvalidArgument
	}
	var ref EvidenceRefV1
	if err := json.Unmarshal(canonical, &ref); err != nil ||
		validateEvidenceRef(ref) != nil ||
		ref.Kind != EvidenceKindContent ||
		ref.DigestAlgorithm != DigestAlgorithmContentBytes ||
		ref.Repository != nil ||
		ref.Commit != nil ||
		ref.Path != nil {
		return EvidenceRefV1{}, nil, ErrInvalidArgument
	}
	return ref, canonical, nil
}
