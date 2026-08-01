package syncevidence

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type M625EEBusRuntime interface {
	Snapshot() (eebusruntime.SnapshotV1, error)
	FeaturesGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeaturesGetRequestV1) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1)
	FeaturesDataGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1)
}

type m625EEBusReader struct {
	runtime M625EEBusRuntime
}

type m625Target struct {
	target        eebusraw.FeatureTargetV1
	unit          string
	fallbackTime  time.Time
	nativeService []string
}

type m625ProjectedObservation struct {
	target        eebusraw.FeatureTargetV1
	nativeService []string
	fieldPath     []string
	observedAt    time.Time
	terminal      string
	valueType     any
	value         any
	unit          any
	quality       any
}

func NewM625EEBusReader(runtime M625EEBusRuntime) (EEBusM625Reader, error) {
	if runtime == nil || reflect.ValueOf(runtime).Kind() == reflect.Ptr && reflect.ValueOf(runtime).IsNil() {
		return nil, ErrInvalidArgument
	}
	return &m625EEBusReader{runtime: runtime}, nil
}

func (reader *m625EEBusReader) ReadFeatureData(ctx context.Context, request SourceRequest) (AcquiredEvidence, error) {
	if reader == nil || reader.runtime == nil || ctx == nil ||
		request.Phase != PhasePre ||
		request.OperationID != string(eebusraw.ToolV1FeaturesDataGet) ||
		request.OperationScope != "feature-data" ||
		request.MaskTier != "redacted" ||
		request.AuthScope.Authority != "effective" ||
		!permissionsContain(request.AuthScope.Permissions, []string{"eebus.raw.read"}) ||
		ValidateLimitsV1(request.Limits) != nil {
		return AcquiredEvidence{}, ErrInvalidArgument
	}
	snapshot, err := reader.runtime.Snapshot()
	if err != nil || snapshot.Validate() != nil ||
		snapshot.Meta.MaskTier != eebusraw.MaskTierRaw ||
		snapshot.Status.State != eebusruntime.ObservedRuntimeStateV1Ready {
		return AcquiredEvidence{}, ErrBackendUnavailable
	}
	locators, err := m625SnapshotLocators(snapshot)
	if err != nil {
		return AcquiredEvidence{}, err
	}
	if len(locators) == 0 {
		return AcquiredEvidence{}, ErrContractViolation
	}
	if uint64(len(locators)) > request.Limits.MaxItemsPerSource {
		return AcquiredEvidence{}, ErrLimitsExceeded
	}

	featureAuth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          eebusraw.AuthScopeV1RawRead,
		Tool:           eebusraw.ToolV1FeaturesGet,
		MaskTier:       eebusraw.MaskTierRaw,
	}
	targets := make([]m625Target, 0, len(locators))
	var binding eebusraw.RuntimeBindingV1
	for _, locator := range locators {
		if err := ctx.Err(); err != nil {
			return AcquiredEvidence{}, err
		}
		featuresRequest := eebusraw.FeaturesGetRequestV1{Target: locator.Clone()}
		if terminal := eebusraw.ValidateFeaturesGetRequestV1(featuresRequest); terminal != nil {
			return AcquiredEvidence{}, ErrContractViolation
		}
		data, terminal := reader.runtime.FeaturesGet(ctx, featureAuth, featuresRequest)
		if terminal != nil || eebusraw.ValidateFeaturesGetDataV1(featuresRequest, data) != nil {
			return AcquiredEvidence{}, ErrContractViolation
		}
		if binding == (eebusraw.RuntimeBindingV1{}) {
			binding = data.Runtime
		} else if data.Runtime != binding {
			return AcquiredEvidence{}, ErrContractViolation
		}
		function := m625FunctionForType(locator.FeatureType)
		var descriptor *eebusraw.FunctionDescriptorV1
		for index := range data.Functions {
			if data.Functions[index].Function == function {
				descriptor = &data.Functions[index]
				break
			}
		}
		if descriptor == nil || !descriptor.PossibleOperations.Read {
			continue
		}
		unit := descriptor.Constraints.Unit
		if unit == "" || !m625Units[unit] {
			unit = "unknown"
		}
		targets = append(targets, m625Target{
			target: eebusraw.FeatureTargetV1{
				RemoteSKI: locator.RemoteSKI, SHIPID: locator.SHIPID,
				DeviceAddress: locator.DeviceAddress, EntityAddress: append([]uint64(nil), locator.EntityAddress...),
				FeatureAddress: locator.FeatureAddress, FeatureType: locator.FeatureType,
				FeatureRole: locator.FeatureRole, Function: function, Operation: eebusraw.OperationV1Read,
			},
			unit: unit, fallbackTime: data.DataTimestamp,
			nativeService: []string{locator.RemoteSKI, locator.SHIPID},
		})
	}
	if len(targets) == 0 {
		return AcquiredEvidence{}, ErrContractViolation
	}

	dataAuth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          eebusraw.AuthScopeV1RawRead,
		Tool:           eebusraw.ToolV1FeaturesDataGet,
		MaskTier:       eebusraw.MaskTierRaw,
	}
	projected := make([]m625ProjectedObservation, 0, len(targets))
	for begin := 0; begin < len(targets); begin += eebusraw.MaximumReadTargetsV1 {
		end := begin + eebusraw.MaximumReadTargetsV1
		if end > len(targets) {
			end = len(targets)
		}
		batch := targets[begin:end]
		rawTargets := make([]eebusraw.FeatureTargetV1, len(batch))
		for index := range batch {
			rawTargets[index] = batch[index].target.Clone()
		}
		readRequest := eebusraw.FeatureDataGetRequestV1{Targets: rawTargets}
		if terminal := eebusraw.ValidateFeatureDataGetRequestV1(readRequest); terminal != nil {
			return AcquiredEvidence{}, ErrContractViolation
		}
		data, terminal := reader.runtime.FeaturesDataGet(ctx, dataAuth, readRequest)
		if terminal != nil && len(data.Results) == 0 && len(data.Failures) == 0 {
			for _, target := range batch {
				projected = append(projected, m625TerminalObservation(target, terminal.Code))
			}
			continue
		}
		if eebusraw.ValidateFeatureDataGetDataV1(readRequest, data, terminal) != nil {
			return AcquiredEvidence{}, ErrContractViolation
		}
		resultIndex := 0
		failureByIndex := make(map[uint64]eebusraw.ReadFailureV1, len(data.Failures))
		for _, failure := range data.Failures {
			failureByIndex[failure.TargetIndex] = failure
		}
		for targetIndex, target := range batch {
			if failure, failed := failureByIndex[uint64(targetIndex)]; failed {
				projected = append(projected, m625TerminalObservation(target, failure.Error.Code))
				continue
			}
			if resultIndex >= len(data.Results) || data.Results[resultIndex].Runtime != binding {
				return AcquiredEvidence{}, ErrContractViolation
			}
			normalized := m625NormalizeRead(target, data.Results[resultIndex])
			if len(normalized) == 0 {
				normalized = []m625ProjectedObservation{m625TerminalObservation(target, eebusraw.ErrorCodeV1DecodeError)}
			}
			projected = append(projected, normalized...)
			resultIndex++
		}
	}
	if len(projected) == 0 || uint64(len(projected)) > request.Limits.MaxItemsPerSource {
		return AcquiredEvidence{}, ErrLimitsExceeded
	}
	return m625BuildEvidence(projected, request.Limits)
}

func m625SnapshotLocators(snapshot eebusruntime.SnapshotV1) ([]eebusraw.FeatureLocatorV1, error) {
	devices := make(map[string]eebusruntime.DeviceV1, len(snapshot.Devices))
	for _, device := range snapshot.Devices {
		if _, duplicate := devices[device.Address]; duplicate || device.SHIPID == nil {
			return nil, ErrContractViolation
		}
		devices[device.Address] = device
	}
	locators := make([]eebusraw.FeatureLocatorV1, 0)
	for _, feature := range snapshot.Features {
		if feature.Role != string(eebusraw.FeatureRoleV1Server) || m625FunctionForType(feature.Type) == "" {
			continue
		}
		device, ok := devices[feature.DeviceAddress]
		if !ok || device.SHIPID == nil || *device.SHIPID == "" {
			return nil, ErrContractViolation
		}
		entity, featureAddress, err := m625ParseSnapshotAddress(
			feature.DeviceAddress,
			feature.EntityAddress,
			feature.FeatureAddress,
		)
		if err != nil {
			return nil, err
		}
		locator := eebusraw.FeatureLocatorV1{
			RemoteSKI: device.SKI, SHIPID: *device.SHIPID,
			DeviceAddress: feature.DeviceAddress, EntityAddress: entity,
			FeatureAddress: featureAddress, FeatureType: feature.Type,
			FeatureRole: eebusraw.FeatureRoleV1Server,
		}
		if terminal := eebusraw.ValidateFeaturesGetRequestV1(eebusraw.FeaturesGetRequestV1{Target: locator}); terminal != nil {
			return nil, ErrContractViolation
		}
		locators = append(locators, locator)
	}
	sort.Slice(locators, func(left, right int) bool {
		return m625CompareLocator(locators[left], locators[right]) < 0
	})
	for index := 1; index < len(locators); index++ {
		if m625CompareLocator(locators[index-1], locators[index]) == 0 {
			return nil, ErrContractViolation
		}
	}
	return locators, nil
}

func m625ParseSnapshotAddress(device, entityText, featureText string) ([]uint64, uint64, error) {
	prefix := device + ":["
	if device == "" || !strings.HasPrefix(entityText, prefix) || !strings.HasSuffix(entityText, "]:") {
		return nil, 0, ErrContractViolation
	}
	entityBody := strings.TrimSuffix(strings.TrimPrefix(entityText, prefix), "]:")
	if entityBody == "" {
		return nil, 0, ErrContractViolation
	}
	parts := strings.Split(entityBody, ",")
	entity := make([]uint64, len(parts))
	for index, part := range parts {
		value, ok := m625ParseAddressInteger(part)
		if !ok {
			return nil, 0, ErrContractViolation
		}
		entity[index] = value
	}
	if !strings.HasPrefix(featureText, entityText) {
		return nil, 0, ErrContractViolation
	}
	feature, ok := m625ParseAddressInteger(strings.TrimPrefix(featureText, entityText))
	if !ok {
		return nil, 0, ErrContractViolation
	}
	return entity, feature, nil
}

func m625ParseAddressInteger(value string) (uint64, bool) {
	if value == "" || len(value) > 16 || len(value) > 1 && value[0] == '0' {
		return 0, false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil && parsed <= MaxSafeIntegerV1
}

func m625CompareLocator(left, right eebusraw.FeatureLocatorV1) int {
	for _, pair := range [][2]string{
		{left.RemoteSKI, right.RemoteSKI},
		{left.SHIPID, right.SHIPID},
		{left.DeviceAddress, right.DeviceAddress},
	} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	for index := 0; index < len(left.EntityAddress) && index < len(right.EntityAddress); index++ {
		if left.EntityAddress[index] < right.EntityAddress[index] {
			return -1
		}
		if left.EntityAddress[index] > right.EntityAddress[index] {
			return 1
		}
	}
	if len(left.EntityAddress) < len(right.EntityAddress) {
		return -1
	}
	if len(left.EntityAddress) > len(right.EntityAddress) {
		return 1
	}
	if left.FeatureAddress < right.FeatureAddress {
		return -1
	}
	if left.FeatureAddress > right.FeatureAddress {
		return 1
	}
	if left.FeatureType < right.FeatureType {
		return -1
	}
	if left.FeatureType > right.FeatureType {
		return 1
	}
	return strings.Compare(string(left.FeatureRole), string(right.FeatureRole))
}

func m625FunctionForType(featureType string) string {
	switch featureType {
	case "Measurement":
		return "measurementListData"
	case "Setpoint":
		return "setpointListData"
	case "HVAC":
		return "hvacSystemFunctionListData"
	default:
		return ""
	}
}

func m625TerminalObservation(target m625Target, code eebusraw.ErrorCodeV1) m625ProjectedObservation {
	terminal := strings.ToUpper(string(code))
	if !m625Terminals[terminal] || terminal == "SUCCESS" {
		terminal = "INTERNAL"
	}
	return m625ProjectedObservation{
		target: target.target, nativeService: append([]string(nil), target.nativeService...),
		observedAt: target.fallbackTime, terminal: terminal,
	}
}

func m625NormalizeRead(target m625Target, observation eebusraw.ReadObservationV1) []m625ProjectedObservation {
	root, ok := observation.Value.Value().(map[string]any)
	if !ok {
		return nil
	}
	switch target.target.Function {
	case "measurementListData":
		return m625NormalizeMeasurement(target, observation.DataTimestamp, root)
	case "setpointListData":
		return m625NormalizeSetpoint(target, observation.DataTimestamp, root)
	case "hvacSystemFunctionListData":
		return m625NormalizeHVAC(target, observation.DataTimestamp, root)
	default:
		return nil
	}
}

func m625NormalizeMeasurement(target m625Target, observedAt time.Time, root map[string]any) []m625ProjectedObservation {
	items, ok := root["measurementData"].([]any)
	if !ok || len(items) == 0 {
		return nil
	}
	result := make([]m625ProjectedObservation, 0, len(items))
	for index, raw := range items {
		item, ok := raw.(map[string]any)
		path := m625ItemPath("measurementData", index, item, "measurementId")
		if !ok {
			result = append(result, m625DecodeFailure(target, observedAt, path))
			continue
		}
		decimal, ok := m625ScaledDecimal(item["value"])
		if !ok {
			result = append(result, m625DecodeFailure(target, observedAt, append(path, "value")))
			continue
		}
		result = append(result, m625Success(target, observedAt, append(path, "value"), "DECIMAL", decimal, target.unit))
	}
	return result
}

func m625NormalizeSetpoint(target m625Target, observedAt time.Time, root map[string]any) []m625ProjectedObservation {
	items, ok := root["setpointData"].([]any)
	if !ok || len(items) == 0 {
		return nil
	}
	result := make([]m625ProjectedObservation, 0)
	decimalFields := []string{"value", "valueMin", "valueMax", "valueToleranceAbsolute", "valueTolerancePercentage"}
	booleanFields := []string{"isSetpointChangeable", "isSetpointActive"}
	for index, raw := range items {
		item, ok := raw.(map[string]any)
		path := m625ItemPath("setpointData", index, item, "setpointId")
		if !ok {
			result = append(result, m625DecodeFailure(target, observedAt, path))
			continue
		}
		for _, field := range decimalFields {
			if _, exists := item[field]; !exists {
				continue
			}
			decimal, valid := m625ScaledDecimal(item[field])
			if !valid {
				result = append(result, m625DecodeFailure(target, observedAt, append(path, field)))
				continue
			}
			result = append(result, m625Success(target, observedAt, append(path, field), "DECIMAL", decimal, target.unit))
		}
		for _, field := range booleanFields {
			if _, exists := item[field]; !exists {
				continue
			}
			value, valid := item[field].(bool)
			if !valid {
				result = append(result, m625DecodeFailure(target, observedAt, append(path, field)))
				continue
			}
			result = append(result, m625Success(target, observedAt, append(path, field), "BOOLEAN", strconv.FormatBool(value), nil))
		}
	}
	return result
}

func m625NormalizeHVAC(target m625Target, observedAt time.Time, root map[string]any) []m625ProjectedObservation {
	items, ok := root["hvacSystemFunctionData"].([]any)
	if !ok || len(items) == 0 {
		return nil
	}
	result := make([]m625ProjectedObservation, 0)
	enumFields := []string{"currentOperationModeId", "currentSetpointId"}
	booleanFields := []string{"isOperationModeIdChangeable", "isSetpointIdChangeable", "isOverrunActive"}
	for index, raw := range items {
		item, ok := raw.(map[string]any)
		path := m625ItemPath("hvacSystemFunctionData", index, item, "systemFunctionId")
		if !ok {
			result = append(result, m625DecodeFailure(target, observedAt, path))
			continue
		}
		for _, field := range enumFields {
			if _, exists := item[field]; !exists {
				continue
			}
			value, valid := m625UnsignedInteger(item[field])
			if !valid || value > 9_999_999_999 {
				result = append(result, m625DecodeFailure(target, observedAt, append(path, field)))
				continue
			}
			result = append(result, m625Success(target, observedAt, append(path, field), "ENUM", "ID_"+strconv.FormatUint(value, 10), nil))
		}
		for _, field := range booleanFields {
			if _, exists := item[field]; !exists {
				continue
			}
			value, valid := item[field].(bool)
			if !valid {
				result = append(result, m625DecodeFailure(target, observedAt, append(path, field)))
				continue
			}
			result = append(result, m625Success(target, observedAt, append(path, field), "BOOLEAN", strconv.FormatBool(value), nil))
		}
	}
	return result
}

func m625ItemPath(root string, index int, item map[string]any, identityField string) []string {
	path := []string{root, fmt.Sprintf("%020d", index)}
	if item != nil {
		if identity, ok := m625UnsignedInteger(item[identityField]); ok {
			path = append(path, identityField+"="+strconv.FormatUint(identity, 10))
		}
	}
	return path
}

func m625Success(target m625Target, observedAt time.Time, path []string, valueType, value string, unit any) m625ProjectedObservation {
	return m625ProjectedObservation{
		target: target.target, nativeService: append([]string(nil), target.nativeService...),
		fieldPath: append([]string(nil), path...), observedAt: observedAt, terminal: "SUCCESS",
		valueType: valueType, value: value, unit: unit, quality: "OBSERVED",
	}
}

func m625DecodeFailure(target m625Target, observedAt time.Time, path []string) m625ProjectedObservation {
	return m625ProjectedObservation{
		target: target.target, nativeService: append([]string(nil), target.nativeService...),
		fieldPath: append([]string(nil), path...), observedAt: observedAt, terminal: "DECODE_ERROR",
	}
}

func m625ScaledDecimal(value any) (string, bool) {
	scaled, ok := value.(map[string]any)
	if !ok {
		return "", false
	}
	number, numberOK := m625SignedInteger(scaled["number"])
	scale, scaleOK := m625SignedInteger(scaled["scale"])
	if !numberOK || !scaleOK || scale < -128 || scale > 127 {
		return "", false
	}
	if number == 0 {
		if scale >= 0 {
			return "0", true
		}
		result := "0." + strings.Repeat("0", int(-scale))
		return result, len(result) <= 64
	}
	negative := number < 0
	digits := strconv.FormatInt(number, 10)
	if negative {
		digits = digits[1:]
	}
	var result string
	switch {
	case scale >= 0:
		result = digits + strings.Repeat("0", int(scale))
	case int64(len(digits))+scale > 0:
		point := int64(len(digits)) + scale
		result = digits[:point] + "." + digits[point:]
	default:
		result = "0." + strings.Repeat("0", int(-scale)-len(digits)) + digits
	}
	if negative {
		result = "-" + result
	}
	return result, len(result) <= 64 && m625DecimalPattern.MatchString(result)
}

func m625SignedInteger(value any) (int64, bool) {
	switch current := value.(type) {
	case int:
		return int64(current), true
	case int8:
		return int64(current), true
	case int16:
		return int64(current), true
	case int32:
		return int64(current), true
	case int64:
		return current, true
	case uint:
		if uint64(current) <= uint64(^uint64(0)>>1) {
			return int64(current), true
		}
	case uint8:
		return int64(current), true
	case uint16:
		return int64(current), true
	case uint32:
		return int64(current), true
	case uint64:
		if current <= uint64(^uint64(0)>>1) {
			return int64(current), true
		}
	case json.Number:
		parsed, err := strconv.ParseInt(string(current), 10, 64)
		return parsed, err == nil
	}
	return 0, false
}

func m625UnsignedInteger(value any) (uint64, bool) {
	signed, ok := m625SignedInteger(value)
	return uint64(signed), ok && signed >= 0
}

func m625BuildEvidence(observations []m625ProjectedObservation, limits CaptureLimitsV1) (AcquiredEvidence, error) {
	sort.Slice(observations, func(left, right int) bool {
		return m625ObservationNativeKey(observations[left]) < m625ObservationNativeKey(observations[right])
	})
	services := make(map[string]string)
	serviceRows := make([]any, 0)
	paths := make([]any, 0, len(observations))
	rows := make([]any, 0, len(observations))
	pathKeys := make(map[string]bool, len(observations))
	var latest time.Time
	for index, observation := range observations {
		serviceIdentity := strings.Join(observation.nativeService, "\x00")
		service := services[serviceIdentity]
		if service == "" {
			service = m625OpaquePseudonym("service\x00" + serviceIdentity)
			services[serviceIdentity] = service
			serviceRows = append(serviceRows, service)
		}
		entityIdentity := serviceIdentity + "\x00" + observation.target.DeviceAddress + "\x00" + m625UintPath(observation.target.EntityAddress)
		entity := m625OpaquePseudonym("entity\x00" + entityIdentity)
		featureIdentity := entityIdentity + "\x00" + strconv.FormatUint(observation.target.FeatureAddress, 10)
		feature := m625OpaquePseudonym("feature\x00" + featureIdentity)
		segments := []any{
			map[string]any{"kind": "SERVICE", "selector": service},
			map[string]any{"kind": "ENTITY", "selector": entity},
			map[string]any{"kind": "FEATURE", "selector": feature},
		}
		fieldIdentity := featureIdentity
		for _, component := range observation.fieldPath {
			fieldIdentity += "\x00" + component
			segments = append(segments, map[string]any{
				"kind": "FIELD", "selector": m625OpaquePseudonym("field\x00" + fieldIdentity),
			})
		}
		path := map[string]any{
			"service": service, "entity": entity, "feature": feature, "feature_path": segments,
		}
		pathBytes, err := canonicalJSONValue(path)
		if err != nil || pathKeys[string(pathBytes)] {
			return AcquiredEvidence{}, ErrContractViolation
		}
		pathKeys[string(pathBytes)] = true
		paths = append(paths, path)
		observationKey := m625ObservationNativeKey(observation)
		rows = append(rows, map[string]any{
			"observation_ref":         "obs-" + m625OpaquePseudonym("observation\x00"+observationKey),
			"path_index":              json.Number(strconv.Itoa(index)),
			"feature_type":            observation.target.FeatureType,
			"feature_role":            string(observation.target.FeatureRole),
			"function":                observation.target.Function,
			"source_observed_at":      observation.observedAt.UTC().Format(time.RFC3339Nano),
			"terminal_classification": observation.terminal,
			"value_type":              observation.valueType,
			"value":                   observation.value,
			"unit":                    observation.unit,
			"quality":                 observation.quality,
		})
		if latest.IsZero() || observation.observedAt.After(latest) {
			latest = observation.observedAt
		}
	}
	payload := map[string]any{
		"contract":           M625EEBusContractV1,
		"schema_version":     json.Number("1"),
		"source_observed_at": latest.UTC().Format(time.RFC3339Nano),
		"services":           serviceRows,
		"feature_paths":      paths,
		"observations":       rows,
	}
	if err := validateM625Payload(payload, sourceCapture{
		sourceKind: SourceEEBus, sourceContract: M625EEBusContractV1, sourceSchemaVersion: 1,
		sourceObservedAt: latest,
	}); err != nil {
		return AcquiredEvidence{}, err
	}
	encoded, err := canonicalJSONValue(payload)
	if err != nil || uint64(len(encoded)) > limits.MaxArtifactBytes {
		return AcquiredEvidence{}, ErrLimitsExceeded
	}
	return AcquiredEvidence{SourceObservedAt: latest, NormalizedEvidence: encoded}, nil
}

func m625ObservationNativeKey(observation m625ProjectedObservation) string {
	parts := append([]string(nil), observation.nativeService...)
	parts = append(parts, observation.target.DeviceAddress)
	for _, entity := range observation.target.EntityAddress {
		parts = append(parts, fmt.Sprintf("%020d", entity))
	}
	parts = append(parts, fmt.Sprintf("%020d", observation.target.FeatureAddress))
	parts = append(parts, observation.fieldPath...)
	parts = append(parts, observation.target.FeatureType, observation.target.Function)
	return strings.Join(parts, "\x00")
}

func m625UintPath(values []uint64) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, ",")
}

func m625OpaquePseudonym(identity string) string {
	digest := sha256.Sum256([]byte("HELIANTHUS:M625:EPHEMERAL:V1\x00" + identity))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

var _ EEBusM625Reader = (*m625EEBusReader)(nil)
