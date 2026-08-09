package coexistence

import (
	"encoding/base64"
	"net"
	"reflect"
	"regexp"
	"strings"
	"unicode"
)

var (
	privateKeyPatternV1  = regexp.MustCompile(`(?i)-----BEGIN (?:[A-Z0-9]+(?: [A-Z0-9]+)* )?PRIVATE KEY-----`)
	namespacedHashV1     = regexp.MustCompile(`^[a-z0-9.-]+:sha256:[0-9a-f]{64}$`)
	macPatternV1         = regexp.MustCompile(`(?i)(?:^|[^0-9a-f])(?:(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}|[0-9a-f]{4}\.[0-9a-f]{4}\.[0-9a-f]{4}|[0-9a-f]{12})(?:$|[^0-9a-f])`)
	skiPatternV1         = regexp.MustCompile(`(?i)(?:^|[^0-9a-f])[0-9a-f]{40}(?:$|[^0-9a-f])`)
	redactedIDPatternV1  = regexp.MustCompile(`^redacted:sha256:[0-9a-f]{12}$`)
	toolNamePatternV1    = regexp.MustCompile(`^[a-z][a-z0-9]*(?:\.[a-z0-9]+)+$`)
	dottedRunPatternV1   = regexp.MustCompile(`(?:^|[^a-zA-Z0-9_])([0-9][0-9.]*)(?:$|[^a-zA-Z0-9_])`)
	ipv6PatternV1        = regexp.MustCompile(`(?i)(?:^|[^0-9a-f:])([0-9a-f:]{2,}(?:%[a-z0-9_.-]+)?)(?:$|[^0-9a-f:])`)
	credentialPatternV1  = regexp.MustCompile(`(?i)\b(?:authorization\s*:\s*(?:bearer|basic)|bearer|set-cookie\s*:|cookie\s*:|(?:access[_-]?key|api[_-]?key|credential|password|secret|session[_-]?cookie|token|private[_-]?key)\s*(?::|=|\bis\b))\s*\S+`)
	basicPatternV1       = regexp.MustCompile(`(?i)\bbasic\s+([a-z0-9+/]+={0,2})(?:\s|$)`)
	m7CandidatePatternV1 = regexp.MustCompile(`(?i)(?:^|[^a-z0-9_])m7-candidate-[a-z0-9-]+(?:$|[^a-z0-9_-])`)
)

var candidateCompactKeysV1 = map[string]bool{
	"bindingsourcekind": true, "candidate": true, "candidatefact": true, "candidatefacts": true,
	"candidateid": true, "candidateids": true, "candidatecount": true, "candidatecounts": true,
	"candidates": true, "candidateref": true, "candidaterefs": true, "candidatestatus": true,
	"candidatestatuses": true, "conflict": true, "conflicted": true, "conflicts": true,
	"conflictstatus": true, "comparatoroutcome": true, "debugonly": true, "draftunit": true,
	"draftvalue": true, "errorcategory": true, "evidencedigests": true, "evidencerefs": true,
	"facthash": true, "facthashes": true, "identityfamily": true, "nativeevidencedigests": true,
	"nativeevidencerefs": true, "proposedpath": true, "rawonly": true, "rawonlycount": true,
	"rawonlycounts": true, "sourcebundleid": true, "sourcecontract": true, "sourceid": true,
	"sourceschemaversion": true, "sourceterminal": true, "sourceterminals": true,
	"terminalnegativestate": true, "terminalnegativestates": true, "retesttrigger": true,
	"visibilitychannel": true, "withheld": true, "withheldcount": true, "withheldcounts": true,
}

var sensitiveCompactKeysV1 = map[string]bool{
	"accesskey": true, "accesskeyid": true, "accesskeys": true, "apikey": true, "apikeys": true,
	"authheader": true, "authorization": true, "credential": true, "credentials": true,
	"encryptionkey": true, "keymaterial": true, "password": true, "passwords": true,
	"passphrase": true, "passphrases": true, "presharedkey": true, "privatekey": true,
	"psk": true, "secret": true, "secrets": true, "sessioncookie": true, "signingkey": true,
	"tlskey": true, "token": true, "tokens": true, "truststore": true,
}

var identityPrefixesV1 = map[string]bool{
	"auth": true, "client": true, "device": true, "eebus": true, "endpoint": true,
	"entity": true, "feature": true, "ip": true, "mac": true, "peer": true, "remote": true,
	"serial": true, "service": true, "session": true, "ship": true, "source": true,
	"spine": true, "target": true, "unique": true,
}

var identitySuffixesV1 = map[string]bool{
	"address": true, "addresses": true, "device": true, "devices": true, "entities": true,
	"entity": true, "feature": true, "features": true, "id": true, "ids": true,
	"identifier": true, "identifiers": true, "identities": true, "identity": true,
	"kind": true, "kinds": true, "node": true, "nodes": true, "number": true, "numbers": true,
	"path": true, "paths": true, "peer": true, "peers": true, "selector": true,
	"selectors": true, "serial": true, "serials": true, "service": true, "services": true,
	"ski": true, "skis": true, "source": true, "sources": true, "subject": true,
	"subjects": true, "target": true, "targets": true, "uid": true, "uids": true,
}

func checkAntiLeak(evidence map[string]any, graphs ...map[string]any) error {
	candidateIDs := make(map[string]bool)
	terminalValues := make(map[string]bool)
	for _, graph := range graphs {
		collectCandidateVocabularyV1(graph, candidateIDs, terminalValues)
	}
	for _, rawRun := range evidence["runs"].([]any) {
		state := rawRun.(map[string]any)["state_evidence"].(map[string]any)
		for _, rawFact := range state["facts"].([]any) {
			fact := rawFact.(map[string]any)
			candidateIDs[stringOrEmpty(fact["candidate_id"])] = true
			if terminal := stringOrEmpty(fact["terminal_negative_state"]); terminal != "" {
				terminalValues[terminal] = true
			}
		}
	}
	for _, rawRun := range evidence["runs"].([]any) {
		for _, rawView := range rawRun.(map[string]any)["protected_views"].([]any) {
			if containsCandidateLeakV1(rawView.(map[string]any)["payload"], candidateIDs, terminalValues) {
				return fail("anti_leak.candidate")
			}
		}
	}
	return nil
}

func collectCandidateVocabularyV1(graph map[string]any, ids, terminal map[string]bool) {
	facts, _ := arrayValueV1(graph["facts"])
	for _, rawFact := range facts {
		fact, ok := objectValueV1(rawFact)
		if !ok {
			continue
		}
		if id := stringOrEmpty(fact["candidate_id"]); id != "" {
			ids[id] = true
		}
		if value := stringOrEmpty(fact["terminal_negative_state"]); value != "" {
			terminal[value] = true
		}
		provenance, _ := objectValueV1(fact["provenance"])
		source, _ := objectValueV1(provenance["source_terminal"])
		for _, key := range []string{"binding_source_kind", "error_category", "source_contract", "source_id", "state"} {
			if value := stringOrEmpty(source[key]); value != "" {
				terminal[value] = true
			}
		}
	}
}

func containsCandidateLeakV1(value any, ids, terminal map[string]bool) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if candidateLeakKeyV1(key) || containsCandidateLeakV1(key, ids, terminal) || containsCandidateLeakV1(item, ids, terminal) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsCandidateLeakV1(item, ids, terminal) {
				return true
			}
		}
	case string:
		compact := compactKeyV1(current)
		if candidateCompactKeysV1[compact] || containsText([]string{"RAW_ONLY", "CANDIDATE", "CONFLICTED", "WITHHELD", "WITHHELD/CONFLICT", "CANDIDATE_DEBUG_REPLAY"}, current) ||
			m7CandidatePatternV1.MatchString(current) {
			return true
		}
		for id := range ids {
			if id != "" && strings.Contains(current, id) {
				return true
			}
		}
		lower := strings.ToLower(current)
		for terminalValue := range terminal {
			if tokenContainsV1(lower, strings.ToLower(terminalValue)) {
				return true
			}
		}
	}
	return false
}

func candidateLeakKeyV1(key string) bool {
	if candidateCompactKeysV1[compactKeyV1(key)] {
		return true
	}
	tokens := keyTokensV1(key)
	for index, token := range tokens {
		switch token {
		case "candidate", "candidates", "conflict", "conflicted", "conflicts", "withheld", "rawonly":
			return true
		case "raw":
			if index+1 < len(tokens) && tokens[index+1] == "only" {
				return true
			}
		}
	}
	return false
}

func checkPublicRedaction(evidence map[string]any) error {
	if evidence["export_tier"] != "PUBLIC_REDACTED" || containsPublicSecretV1(evidence, "") {
		return fail("redaction.public")
	}
	return nil
}

func containsPublicSecretV1(value any, key string) bool {
	if key != "" {
		compact := compactKeyV1(key)
		tokens := keyTokensV1(key)
		if (compact == "source" || compact == "target") && numericAddressV1(value) {
			return true
		}
		if sensitiveKeyV1(key, value) {
			return true
		}
		if strings.HasSuffix(compact, "commit") {
			return value == nil && compact != "sourceparentcommit" || value != nil && !validHashLikeV1(value)
		}
		if strings.HasSuffix(compact, "hash") || strings.HasSuffix(compact, "digest") {
			return !validHashLikeV1(value)
		}
		if identityKeyV1(tokens) {
			return !validRedactedIdentityV1(value)
		}
		if strings.HasSuffix(compact, "spinepath") || strings.HasSuffix(compact, "spinekind") {
			return true
		}
	}
	switch current := value.(type) {
	case map[string]any:
		for itemKey, item := range current {
			if containsPublicSecretV1(itemKey, "") || containsPublicSecretV1(item, itemKey) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsPublicSecretV1(item, key) {
				return true
			}
		}
	case string:
		if redactedIDPatternV1.MatchString(current) || digestPatternV1.MatchString(current) || namespacedHashV1.MatchString(current) {
			return false
		}
		return privateKeyPatternV1.MatchString(current) || credentialValueV1(current) ||
			containsNonPublicIPv4V1(current) || containsPrivateIPv6V1(current) || macPatternV1.MatchString(current) || skiPatternV1.MatchString(current)
	}
	return false
}

func sensitiveKeyV1(key string, value any) bool {
	compact := compactKeyV1(key)
	if sensitiveCompactKeysV1[compact] {
		return true
	}
	tokens := keyTokensV1(key)
	for index, token := range tokens {
		if sensitiveCompactKeysV1[token] || token == "cookie" || token == "cookies" ||
			index+1 < len(tokens) && sensitiveCompactKeysV1[token+tokens[index+1]] {
			return true
		}
	}
	if len(tokens) > 0 && (tokens[len(tokens)-1] == "count" || tokens[len(tokens)-1] == "counts" || tokens[len(tokens)-1] == "total" || tokens[len(tokens)-1] == "totals") {
		_, numeric := integerValue(value)
		return !numeric
	}
	return false
}

func identityKeyV1(tokens []string) bool {
	if len(tokens) >= 2 && tokens[0] == "via" && tokens[len(tokens)-1] == "device" {
		return true
	}
	if len(tokens) == 1 {
		switch tokens[0] {
		case "address", "addresses", "device", "endpoint", "host", "hostname", "id", "identifier", "identifiers", "identities", "identity", "ip", "ipv4", "ipv6", "selector", "selectors", "serial", "serials", "ski", "skis", "uid", "uids":
			return true
		}
	}
	for index := 0; index+1 < len(tokens); index++ {
		if identityPrefixesV1[tokens[index]] && identitySuffixesV1[tokens[index+1]] {
			return true
		}
	}
	return false
}

func validRedactedIdentityV1(value any) bool {
	switch current := value.(type) {
	case []any:
		for _, item := range current {
			if !validRedactedIdentityV1(item) {
				return false
			}
		}
		return true
	case string:
		return redactedIDPatternV1.MatchString(current)
	default:
		return false
	}
}

func validHashLikeV1(value any) bool {
	text, ok := stringValueV1(value)
	return ok && (digestPatternV1.MatchString(text) || shaPatternV1.MatchString(text) || regexp.MustCompile(`^[a-z0-9.-]+:sha256:[0-9a-f]{64}$`).MatchString(text))
}

func credentialValueV1(value string) bool {
	if credentialPatternV1.MatchString(value) {
		return true
	}
	for _, match := range basicPatternV1.FindAllStringSubmatch(value, -1) {
		decoded, err := base64.StdEncoding.DecodeString(match[1])
		if err == nil && strings.Contains(string(decoded), ":") {
			return true
		}
	}
	return false
}

func containsNonPublicIPv4V1(value string) bool {
	for _, match := range dottedRunPatternV1.FindAllStringSubmatch(value, -1) {
		candidate := match[1]
		if strings.Count(candidate, ".") < 3 {
			continue
		}
		if strings.HasSuffix(candidate, ".") {
			return true
		}
		address := net.ParseIP(candidate)
		if address == nil || !address.IsGlobalUnicast() || address.IsPrivate() {
			return true
		}
	}
	return false
}

func containsPrivateIPv6V1(value string) bool {
	for _, match := range ipv6PatternV1.FindAllStringSubmatch(value, -1) {
		candidate := strings.Split(match[1], "%")[0]
		address := net.ParseIP(candidate)
		if address != nil && address.To4() == nil && (!address.IsGlobalUnicast() || address.IsPrivate()) {
			return true
		}
	}
	return false
}

func numericAddressV1(value any) bool {
	if _, ok := integerValue(value); ok {
		return true
	}
	text, ok := stringValueV1(value)
	if !ok {
		return false
	}
	if strings.HasPrefix(strings.ToLower(text), "0x") {
		text = text[2:]
		return text != "" && allHexV1(text)
	}
	for _, current := range text {
		if current < '0' || current > '9' {
			return false
		}
	}
	return text != ""
}

func allHexV1(value string) bool {
	for _, current := range strings.ToLower(value) {
		if !strings.ContainsRune("0123456789abcdef", current) {
			return false
		}
	}
	return true
}

func checkAuthority(evidence map[string]any) error {
	runs := evidence["runs"].([]any)
	for _, rawRun := range runs[:len(runs)-1] {
		run := rawRun.(map[string]any)
		registryView, registryOK := findViewV1(run, "semantic.registry")
		routesView, routesOK := findViewV1(run, "command.routing")
		registryData, registryDataOK := payloadDataV1(registryView)
		routesData, routesDataOK := payloadDataV1(routesView)
		if !registryOK || !routesOK || !registryDataOK || !routesDataOK || registryData["authority"] != "ebus.promoted" ||
			containsEEBusAuthorityV1(registryData) || containsEEBusAuthorityV1(routesData) {
			return fail("authority.ebus")
		}
		leaves, leavesOK := arrayValueV1(registryData["leaves"])
		for _, rawLeaf := range leaves {
			leaf, ok := objectValueV1(rawLeaf)
			if !ok || leaf["source"] != "ebus" || leaf["promotion_state"] != "PROMOTED" {
				return fail("authority.ebus")
			}
		}
		routes, routesOK := arrayValueV1(routesData["routes"])
		for _, rawRoute := range routes {
			route, ok := objectValueV1(rawRoute)
			if !ok || route["source"] != "ebus" {
				return fail("authority.ebus")
			}
		}
		if !leavesOK || !routesOK {
			return fail("authority.ebus")
		}
	}
	return nil
}

func containsEEBusAuthorityV1(value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			compact := compactKeyV1(key)
			if strings.Contains(compact, "eebus") && (strings.Contains(compact, "authority") || strings.Contains(compact, "source") || strings.Contains(compact, "runtime") || strings.Contains(compact, "provider")) {
				return true
			}
			if authorityKeyV1(key) && identifiesEEBusV1(item) || containsEEBusAuthorityV1(item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsEEBusAuthorityV1(item) {
				return true
			}
		}
	}
	return false
}

func authorityKeyV1(key string) bool {
	for _, token := range keyTokensV1(key) {
		switch token {
		case "adapter", "adapters", "authority", "authorities", "backend", "backends", "driver", "drivers", "origin", "origins", "provider", "providers", "protocol", "protocols", "runtime", "runtimes", "source", "sources", "transport", "transports":
			return true
		}
	}
	return false
}

func identifiesEEBusV1(value any) bool {
	switch current := value.(type) {
	case string:
		return strings.Contains(compactKeyV1(current), "eebus")
	case []any:
		for _, item := range current {
			if identifiesEEBusV1(item) {
				return true
			}
		}
	case map[string]any:
		for key, item := range current {
			if identifiesEEBusV1(key) || identifiesEEBusV1(item) {
				return true
			}
		}
	}
	return false
}

func checkScope(evidence, registry map[string]any) error {
	live := evidence["evidence_class"] == "CAPTURED_RUNTIME_EVIDENCE"
	scope := evidence["scope"].(map[string]any)
	if scope["gate"] != registry["gate"] || !reflect.DeepEqual(scope["claims"], []any{"EEBUS-G18"}) ||
		!reflect.DeepEqual(scope["excluded_gates"], registry["excluded_gates"]) || scope["live_vr940_claim"] != live ||
		scope["public_version_policy"] != "V1_ONLY_NO_PUBLIC_V2" {
		return fail("gate.scope")
	}
	approvedEEBus := []string{"eebus.v1.runtime.status.get", "eebus.v1.services.list"}
	approvedTools := []string{"ebus.v1.devices.list", "ebus.v1.zones.list", "eebus.v1.runtime.status.get", "eebus.v1.services.list"}
	runs := evidence["runs"].([]any)
	for runIndex, rawRun := range runs {
		if runIndex == len(runs)-1 {
			continue
		}
		run := rawRun.(map[string]any)
		inventory, inventoryOK := findViewV1(run, "mcp.tool.inventory")
		contractView, contractOK := findViewV1(run, "mcp.eebus.v1.contract")
		inventoryData, inventoryDataOK := payloadDataV1(inventory)
		contractData, contractDataOK := payloadDataV1(contractView)
		tools, toolsOK := stringsFromArray(inventoryData["tools"])
		if !inventoryOK || !contractOK || !inventoryDataOK || !contractDataOK || !toolsOK || !reflect.DeepEqual(tools, approvedTools) ||
			!exactKeys(contractData, "namespace", "public_v2", "schema_digest", "version") || contractData["namespace"] != "eebus.v1" ||
			contractData["version"] != number(1) || contractData["public_v2"] != false || !digestPatternV1.MatchString(stringOrEmpty(contractData["schema_digest"])) {
			return fail("gate.scope")
		}
		var eebusTools []string
		for _, tool := range tools {
			if !toolNamePatternV1.MatchString(tool) || nonV1EEBusSurfaceV1(tool) || eebusWriteSurfaceV1(tool) {
				return fail("gate.scope")
			}
			if strings.HasPrefix(tool, "eebus.v1.") {
				eebusTools = append(eebusTools, tool)
			}
		}
		if !reflect.DeepEqual(eebusTools, approvedEEBus) {
			return fail("gate.scope")
		}
		for _, rawView := range run["protected_views"].([]any) {
			view := rawView.(map[string]any)
			payload := view["payload"]
			if containsLaterMilestoneV1(payload) || containsNonV1OrWriteV1(payload) ||
				containsEEBusPublicIdentifierOutsideV1Context(stringOrEmpty(view["view_id"]), payload) {
				return fail("gate.scope")
			}
		}
	}
	return nil
}

func containsNonV1OrWriteV1(value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if nonV1EEBusSurfaceV1(key) || eebusWriteSurfaceV1(key) || containsNonV1OrWriteV1(item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsNonV1OrWriteV1(item) {
				return true
			}
		}
	case string:
		return nonV1EEBusSurfaceV1(current) || eebusWriteSurfaceV1(current)
	}
	return false
}

func nonV1EEBusSurfaceV1(value string) bool {
	compact := compactKeyV1(value)
	return strings.Contains(compact, "eebusv2") || strings.Contains(compact, "eebus2") || strings.Contains(compact, "eebusexperimental") || strings.Contains(compact, "eebuslegacy")
}

func containsEEBusPublicIdentifierOutsideV1Context(viewID string, value any) bool {
	return containsEEBusPublicIdentifierOutsideV1Path(viewID, nil, value)
}

func containsEEBusPublicIdentifierOutsideV1Path(viewID string, path []string, value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if isEEBusPublicIdentifierV1(key) || containsEEBusPublicIdentifierOutsideV1Path(viewID, append(path, key), item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsEEBusPublicIdentifierOutsideV1Path(viewID, append(path, "[]"), item) {
				return true
			}
		}
	case string:
		return isEEBusPublicIdentifierV1(current) && !approvedEEBusPublicIdentifierV1(viewID, path, current)
	}
	return false
}

func approvedEEBusPublicIdentifierV1(viewID string, path []string, value string) bool {
	switch viewID {
	case "mcp.tool.inventory":
		if !reflect.DeepEqual(path, []string{"data", "tools", "[]"}) {
			return false
		}
		return value == "eebus.v1.runtime.status.get" || value == "eebus.v1.services.list"
	case "mcp.eebus.v1.contract":
		return reflect.DeepEqual(path, []string{"data", "namespace"}) && value == "eebus.v1"
	default:
		return false
	}
}

func isEEBusPublicIdentifierV1(value string) bool {
	compact := compactKeyV1(value)
	return strings.Contains(compact, "eebus") && !strings.ContainsAny(value, " \t\n\r")
}

func eebusWriteSurfaceV1(value string) bool {
	compact := compactKeyV1(value)
	if !strings.Contains(compact, "eebus") {
		return false
	}
	for _, verb := range []string{"set", "write", "mutate", "invoke", "command"} {
		if strings.Contains(compact, verb) {
			return true
		}
	}
	return false
}

func containsLaterMilestoneV1(value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if containsLaterMilestoneV1(key) || containsLaterMilestoneV1(item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsLaterMilestoneV1(item) {
				return true
			}
		}
	case string:
		compact := compactKeyV1(current)
		return strings.Contains(compact, "m85") || strings.Contains(compact, "m9") || strings.Contains(compact, "semanticpromotion") || strings.Contains(compact, "consumerrollout")
	}
	return false
}

func payloadDataV1(view map[string]any) (map[string]any, bool) {
	if view == nil {
		return nil, false
	}
	payload, ok := objectValueV1(view["payload"])
	if !ok {
		return nil, false
	}
	return objectValueV1(payload["data"])
}

func compactKeyV1(value string) string {
	var output strings.Builder
	for _, current := range strings.ToLower(value) {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' {
			output.WriteRune(current)
		}
	}
	return output.String()
}

func keyTokensV1(value string) []string {
	var separated strings.Builder
	var previous rune
	for index, current := range value {
		if index > 0 && unicode.IsUpper(current) && (unicode.IsLower(previous) || unicode.IsDigit(previous)) {
			separated.WriteByte('_')
		}
		if unicode.IsLetter(current) || unicode.IsDigit(current) {
			separated.WriteRune(unicode.ToLower(current))
		} else {
			separated.WriteByte('_')
		}
		previous = current
	}
	return strings.FieldsFunc(separated.String(), func(current rune) bool { return current == '_' })
}

func tokenContainsV1(value, token string) bool {
	if token == "" {
		return false
	}
	for start := 0; ; {
		index := strings.Index(value[start:], token)
		if index < 0 {
			return false
		}
		index += start
		leftOK := index == 0 || !isTokenRuneV1(rune(value[index-1]))
		right := index + len(token)
		rightOK := right == len(value) || !isTokenRuneV1(rune(value[right]))
		if leftOK && rightOK {
			return true
		}
		start = index + 1
	}
}

func isTokenRuneV1(value rune) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '_'
}
