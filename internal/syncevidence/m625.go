package syncevidence

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"time"
)

var (
	m625ObservationRefPattern = regexp.MustCompile(`^obs-[A-Za-z0-9_-]{43}$`)
	m625IdentifierPattern     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,127}$`)
	m625DecimalPattern        = regexp.MustCompile(`^(0(\.0+)?|0\.[0-9]*[1-9][0-9]*|[1-9][0-9]*(\.[0-9]+)?|-(0\.[0-9]*[1-9][0-9]*|[1-9][0-9]*(\.[0-9]+)?))$`)
	m625EnumPattern           = regexp.MustCompile(`^ID_(0|[1-9][0-9]{0,9})$`)
	m625Units                 = mustM625SchemaEnum("UnitV1")
	m625Terminals             = mustM625SchemaEnum("TerminalClassificationV1")
)

type m625RemaskingRequirement struct {
	pseudonym string
	identity  string
}

func mustM625SchemaEnum(definition string) map[string]bool {
	var document struct {
		Definitions map[string]struct {
			Enum []string `json:"enum"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(mustReadContract("helianthus.eebus.m625.public-redacted-evidence.v1.schema.json"), &document); err != nil {
		panic("syncevidence: invalid M6.25 source schema")
	}
	values := document.Definitions[definition].Enum
	if len(values) == 0 {
		panic("syncevidence: incomplete M6.25 source schema")
	}
	result := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || result[value] {
			panic("syncevidence: invalid M6.25 source enum")
		}
		result[value] = true
	}
	return result
}

func validateM625Payload(object map[string]any, capture sourceCapture) error {
	if !exactKeys(object, "contract", "schema_version", "source_observed_at", "services", "feature_paths", "observations") ||
		object["contract"] != M625EEBusContractV1 ||
		object["schema_version"] != json.Number("1") ||
		capture.sourceContract != M625EEBusContractV1 ||
		capture.sourceSchemaVersion != 1 {
		return contractError("schema.bundle")
	}
	rootTimestamp, ok := m625Timestamp(object["source_observed_at"])
	if !ok || !rootTimestamp.Equal(capture.sourceObservedAt) {
		return contractError("schema.bundle")
	}

	services, ok := object["services"].([]any)
	if !ok || len(services) < 1 || len(services) > 64 {
		return contractError("schema.bundle")
	}
	serviceSet := make(map[string]bool, len(services))
	lastService := ""
	for _, raw := range services {
		service, ok := raw.(string)
		if !ok || !remaskedValuePattern.MatchString(service) || serviceSet[service] ||
			lastService != "" && service < lastService {
			return contractError("schema.bundle")
		}
		serviceSet[service] = true
		lastService = service
	}

	paths, ok := object["feature_paths"].([]any)
	if !ok || len(paths) < 1 || len(paths) > 4096 {
		return contractError("schema.bundle")
	}
	referencedServices := make(map[string]bool)
	pathEncodings := make(map[string]bool, len(paths))
	lastPath := ""
	for _, raw := range paths {
		path, ok := raw.(map[string]any)
		if !ok || validateM625Path(path, serviceSet) != nil {
			return contractError("schema.bundle")
		}
		service := path["service"].(string)
		referencedServices[service] = true
		encoded, err := canonicalJSONValue(path)
		if err != nil {
			return contractError("schema.bundle")
		}
		key := string(encoded)
		if pathEncodings[key] || lastPath != "" && key < lastPath {
			return contractError("ordering.invalid")
		}
		pathEncodings[key] = true
		lastPath = key
	}
	if len(referencedServices) != len(serviceSet) {
		return contractError("schema.bundle")
	}
	for service := range serviceSet {
		if !referencedServices[service] {
			return contractError("schema.bundle")
		}
	}

	observations, ok := object["observations"].([]any)
	if !ok || len(observations) < 1 || len(observations) > 4096 {
		return contractError("schema.bundle")
	}
	refs := make(map[string]bool, len(observations))
	pathIndexes := make(map[uint64]bool, len(observations))
	lastRef := ""
	var latest time.Time
	for _, raw := range observations {
		observation, ok := raw.(map[string]any)
		if !ok || !exactKeys(
			observation,
			"observation_ref",
			"path_index",
			"feature_type",
			"feature_role",
			"function",
			"source_observed_at",
			"terminal_classification",
			"value_type",
			"value",
			"unit",
			"quality",
		) {
			return contractError("schema.bundle")
		}
		ref, ok := observation["observation_ref"].(string)
		if !ok || !m625ObservationRefPattern.MatchString(ref) || refs[ref] ||
			lastRef != "" && ref < lastRef {
			return contractError("ordering.invalid")
		}
		refs[ref] = true
		lastRef = ref
		pathIndex, ok := m625SafeInteger(observation["path_index"])
		if !ok || pathIndex >= uint64(len(paths)) || pathIndexes[pathIndex] {
			return contractError("schema.bundle")
		}
		pathIndexes[pathIndex] = true
		observedAt, ok := m625Timestamp(observation["source_observed_at"])
		if !ok || validateM625Observation(observation) != nil {
			return contractError("schema.bundle")
		}
		if latest.IsZero() || observedAt.After(latest) {
			latest = observedAt
		}
	}
	if !rootTimestamp.Equal(latest) {
		return contractError("schema.bundle")
	}
	return nil
}

func validateM625Path(path map[string]any, services map[string]bool) error {
	if !exactKeys(path, "service", "entity", "feature", "feature_path") {
		return contractError("schema.bundle")
	}
	service, serviceOK := path["service"].(string)
	entity, entityOK := path["entity"].(string)
	feature, featureOK := path["feature"].(string)
	if !serviceOK || !entityOK || !featureOK ||
		!remaskedValuePattern.MatchString(service) ||
		!remaskedValuePattern.MatchString(entity) ||
		!remaskedValuePattern.MatchString(feature) ||
		!services[service] {
		return contractError("schema.bundle")
	}
	segments, ok := path["feature_path"].([]any)
	if !ok || len(segments) < 3 || len(segments) > 32 {
		return contractError("schema.bundle")
	}
	selectors := make(map[string]bool, len(segments))
	baseKinds := []string{"SERVICE", "ENTITY", "FEATURE"}
	baseSelectors := []string{service, entity, feature}
	for index, raw := range segments {
		segment, ok := raw.(map[string]any)
		if !ok || !exactKeys(segment, "kind", "selector") {
			return contractError("schema.bundle")
		}
		kind, kindOK := segment["kind"].(string)
		selector, selectorOK := segment["selector"].(string)
		if !kindOK || !selectorOK || !remaskedValuePattern.MatchString(selector) || selectors[selector] {
			return contractError("schema.bundle")
		}
		selectors[selector] = true
		if index < 3 {
			if kind != baseKinds[index] || selector != baseSelectors[index] {
				return contractError("schema.bundle")
			}
		} else if kind != "FIELD" {
			return contractError("schema.bundle")
		}
	}
	return nil
}

func validateM625Observation(observation map[string]any) error {
	featureType, typeOK := observation["feature_type"].(string)
	role, roleOK := observation["feature_role"].(string)
	function, functionOK := observation["function"].(string)
	terminal, terminalOK := observation["terminal_classification"].(string)
	if !typeOK || !roleOK || !functionOK || !terminalOK ||
		!m625IdentifierPattern.MatchString(featureType) ||
		!m625IdentifierPattern.MatchString(function) ||
		role != "server" ||
		!m625Terminals[terminal] {
		return contractError("schema.bundle")
	}
	allowed := map[string]bool{}
	switch {
	case featureType == "Measurement" && function == "measurementListData":
		allowed["DECIMAL"] = true
	case featureType == "Setpoint" && function == "setpointListData":
		allowed["DECIMAL"], allowed["BOOLEAN"] = true, true
	case featureType == "HVAC" && function == "hvacSystemFunctionListData":
		allowed["BOOLEAN"], allowed["ENUM"] = true, true
	default:
		return contractError("schema.bundle")
	}
	valueType := observation["value_type"]
	value := observation["value"]
	unit := observation["unit"]
	quality := observation["quality"]
	if terminal != "SUCCESS" {
		if valueType != nil || value != nil || unit != nil || quality != nil {
			return contractError("schema.bundle")
		}
		return nil
	}
	valueTypeText, typeOK := valueType.(string)
	valueText, valueOK := value.(string)
	qualityText, qualityOK := quality.(string)
	if !typeOK || !valueOK || !qualityOK || !allowed[valueTypeText] ||
		(qualityText != "OBSERVED" && qualityText != "STALE") {
		return contractError("schema.bundle")
	}
	switch valueTypeText {
	case "DECIMAL":
		if len(valueText) > 64 || !m625DecimalPattern.MatchString(valueText) {
			return contractError("schema.bundle")
		}
		if unit != nil {
			unitText, ok := unit.(string)
			if !ok || !m625Units[unitText] {
				return contractError("schema.bundle")
			}
		}
	case "BOOLEAN":
		if (valueText != "false" && valueText != "true") || unit != nil {
			return contractError("schema.bundle")
		}
	case "ENUM":
		if !m625EnumPattern.MatchString(valueText) || unit != nil {
			return contractError("schema.bundle")
		}
	default:
		return contractError("schema.bundle")
	}
	return nil
}

func m625Timestamp(value any) (time.Time, bool) {
	text, ok := value.(string)
	if !ok || !canonicalTimestamp(text) {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	return parsed, err == nil
}

func m625SafeInteger(value any) (uint64, bool) {
	number, ok := value.(json.Number)
	if !ok || !isSafeInteger(string(number)) {
		return 0, false
	}
	parsed, err := strconv.ParseUint(string(number), 10, 64)
	return parsed, err == nil
}

func sortM625Payload(object map[string]any) error {
	paths := object["feature_paths"].([]any)
	type indexedPath struct {
		oldIndex int
		encoded  string
		value    any
	}
	indexed := make([]indexedPath, len(paths))
	for index, path := range paths {
		encoded, err := canonicalJSONValue(path)
		if err != nil {
			return err
		}
		indexed[index] = indexedPath{oldIndex: index, encoded: string(encoded), value: path}
	}
	sort.Slice(indexed, func(left, right int) bool { return indexed[left].encoded < indexed[right].encoded })
	oldToNew := make(map[uint64]uint64, len(indexed))
	for newIndex, path := range indexed {
		paths[newIndex] = path.value
		oldToNew[uint64(path.oldIndex)] = uint64(newIndex)
	}
	observations := object["observations"].([]any)
	for _, raw := range observations {
		observation := raw.(map[string]any)
		oldIndex, ok := m625SafeInteger(observation["path_index"])
		if !ok {
			return contractError("schema.bundle")
		}
		observation["path_index"] = json.Number(strconv.FormatUint(oldToNew[oldIndex], 10))
	}
	sort.Slice(observations, func(left, right int) bool {
		return observations[left].(map[string]any)["observation_ref"].(string) <
			observations[right].(map[string]any)["observation_ref"].(string)
	})
	services := object["services"].([]any)
	sort.Slice(services, func(left, right int) bool {
		return services[left].(string) < services[right].(string)
	})
	return nil
}

func m625RemaskingRequirements(object map[string]any) (map[string]m625RemaskingRequirement, error) {
	result := make(map[string]m625RemaskingRequirement)
	services, ok := object["services"].([]any)
	if !ok {
		return nil, contractError("privacy.remask")
	}
	for index, raw := range services {
		service, ok := raw.(string)
		if !ok {
			return nil, contractError("privacy.remask")
		}
		result["/services/"+strconv.Itoa(index)] = m625RemaskingRequirement{
			pseudonym: service,
			identity:  "SERVICE\x00" + service,
		}
	}
	paths, ok := object["feature_paths"].([]any)
	if !ok {
		return nil, contractError("privacy.remask")
	}
	for pathIndex, raw := range paths {
		path, ok := raw.(map[string]any)
		if !ok {
			return nil, contractError("privacy.remask")
		}
		service := path["service"].(string)
		entity := path["entity"].(string)
		feature := path["feature"].(string)
		prefix := "/feature_paths/" + strconv.Itoa(pathIndex)
		serviceIdentity := "SERVICE\x00" + service
		entityIdentity := serviceIdentity + "\x00ENTITY\x00" + entity
		featureIdentity := entityIdentity + "\x00FEATURE\x00" + feature
		result[prefix+"/service"] = m625RemaskingRequirement{pseudonym: service, identity: serviceIdentity}
		result[prefix+"/entity"] = m625RemaskingRequirement{pseudonym: entity, identity: entityIdentity}
		result[prefix+"/feature"] = m625RemaskingRequirement{pseudonym: feature, identity: featureIdentity}
		segments := path["feature_path"].([]any)
		identity := ""
		for segmentIndex, rawSegment := range segments {
			segment := rawSegment.(map[string]any)
			kind := segment["kind"].(string)
			selector := segment["selector"].(string)
			switch segmentIndex {
			case 0:
				identity = serviceIdentity
			case 1:
				identity = entityIdentity
			case 2:
				identity = featureIdentity
			default:
				identity += "\x00" + kind + "\x00" + selector
			}
			result[prefix+"/feature_path/"+strconv.Itoa(segmentIndex)+"/selector"] = m625RemaskingRequirement{
				pseudonym: selector,
				identity:  identity,
			}
		}
	}
	observations, ok := object["observations"].([]any)
	if !ok {
		return nil, contractError("privacy.remask")
	}
	for index, raw := range observations {
		observation := raw.(map[string]any)
		ref := observation["observation_ref"].(string)
		pathIndex, ok := m625SafeInteger(observation["path_index"])
		if !ok || pathIndex >= uint64(len(paths)) {
			return nil, contractError("privacy.remask")
		}
		pathBytes, err := canonicalJSONValue(paths[pathIndex])
		if err != nil {
			return nil, err
		}
		result["/observations/"+strconv.Itoa(index)+"/observation_ref"] = m625RemaskingRequirement{
			pseudonym: ref[4:],
			identity:  "OBSERVATION\x00" + string(pathBytes),
		}
	}
	return result, nil
}
