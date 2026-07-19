package candidatefacts

import (
	"math/big"
	"strconv"
	"strings"
)

func evaluateNumericWindow(
	parameters map[string]any,
	samples []map[string]any,
	artifacts map[string]map[string]any,
	allowedRefs map[string]bool,
) (string, string, error) {
	if err := validateComparatorParameters(parameters); err != nil {
		return "", "", err
	}
	if len(samples) == 0 {
		return "NOT_EVALUATED", "", nil
	}
	seenSamples := make(map[string]bool, len(samples))
	for _, sample := range samples {
		key, err := canonicalKey(sample)
		if err != nil {
			return "", "", fail("comparator.invalid")
		}
		if seenSamples[key] {
			return "", "", fail("ordering.invalid")
		}
		seenSamples[key] = true
	}

	window, _ := objectValue(parameters["window"])
	conversion, _ := objectValue(parameters["unit_conversion"])
	rounding, _ := objectValue(parameters["rounding"])
	tolerance, _ := objectValue(parameters["tolerance"])
	threshold, _ := objectValue(parameters["conflict_threshold"])
	start, _ := integer(window["start_offset_ns"])
	end, _ := integer(window["end_offset_ns"])
	staleCutoff, _ := integer(parameters["stale_cutoff_ns"])
	minimumSamples, _ := integer(parameters["minimum_samples"])
	maximumMissing, _ := integer(parameters["maximum_missing_samples"])
	consecutive, _ := integer(threshold["consecutive_samples"])
	relativePPM, _ := integer(tolerance["relative_ppm"])

	absoluteTolerance, _ := parseExactDecimal(asString(tolerance["absolute_decimal"]), true)
	scale, _ := parseExactDecimal(asString(conversion["scale_decimal"]), false)
	offsetDecimal, _ := parseExactDecimal(asString(conversion["offset_decimal"]), false)
	conflictThreshold, _ := parseExactDecimal(asString(threshold["absolute_decimal"]), true)

	var unavailable, present, conflictRun int64
	var mismatch, conflict bool
	var lastRight string
	for _, sample := range samples {
		sampleOffset, ok := integer(sample["offset_ns"])
		if !ok || sampleOffset < start || sampleOffset > end {
			return "", "", fail("comparator.invalid")
		}
		left, err := bindObservationSide(sample["left"], "EBUS", artifacts, allowedRefs)
		if err != nil {
			return "", "", err
		}
		right, err := bindObservationSide(sample["right"], "EEBUS", artifacts, allowedRefs)
		if err != nil {
			return "", "", err
		}
		if sampleOffset < left.offsetNS || sampleOffset < right.offsetNS {
			return "", "", fail("comparator.invalid")
		}

		computedState := "PRESENT"
		if left.value == nil || right.value == nil || left.unit == nil || right.unit == nil {
			computedState = "MISSING"
		} else if sampleOffset-left.offsetNS > staleCutoff || sampleOffset-right.offsetNS > staleCutoff {
			computedState = "STALE"
		}
		if sample["state"] != computedState {
			return "", "", fail("comparator.invalid")
		}
		if computedState != "PRESENT" {
			unavailable++
			conflictRun = 0
			continue
		}
		if *left.unit != conversion["source_unit"] || *right.unit != conversion["target_unit"] {
			return "", "", fail("comparator.invalid")
		}

		convertedLeft := new(big.Rat).Add(
			new(big.Rat).Mul(left.value, scale),
			offsetDecimal,
		)
		roundedLeft, _, err := roundExactDecimal(convertedLeft, left.text, rounding)
		if err != nil {
			return "", "", err
		}
		roundedRight, rightText, err := roundExactDecimal(right.value, right.text, rounding)
		if err != nil {
			return "", "", err
		}
		lastRight = rightText
		delta := new(big.Rat).Sub(roundedLeft, roundedRight)
		if delta.Sign() < 0 {
			delta.Neg(delta)
		}
		relative := new(big.Rat).Mul(absRat(roundedRight), big.NewRat(relativePPM, 1_000_000))
		allowed := new(big.Rat).Add(absoluteTolerance, relative)
		present++
		if delta.Cmp(allowed) > 0 {
			mismatch = true
		}
		if delta.Cmp(conflictThreshold) >= 0 {
			conflictRun++
			if conflictRun >= consecutive {
				conflict = true
			}
		} else {
			conflictRun = 0
		}
	}
	if conflict {
		return "CONFLICT", lastRight, nil
	}
	if unavailable > maximumMissing || present < minimumSamples {
		return "INDETERMINATE", lastRight, nil
	}
	if mismatch {
		return "MISMATCH", lastRight, nil
	}
	return "MATCH", lastRight, nil
}

type boundObservation struct {
	value    *big.Rat
	text     string
	unit     *string
	offsetNS int64
}

func bindObservationSide(
	raw any,
	expectedKind string,
	artifacts map[string]map[string]any,
	allowedRefs map[string]bool,
) (boundObservation, error) {
	side, ok := objectValue(raw)
	if !ok || !exactKeys(side,
		"source_kind", "source_id", "artifact_id", "evidence_ref",
		"observed_offset_ns", "value_pointer", "unit_pointer", "native_decimal", "native_unit",
	) || side["source_kind"] != expectedKind {
		return boundObservation{}, fail("comparator.invalid")
	}
	key, pairOK := pairKey(side["source_id"], side["artifact_id"])
	artifact, artifactOK := artifacts[key]
	if !pairOK || !artifactOK || artifact["source_kind"] != expectedKind {
		return boundObservation{}, fail("comparator.invalid")
	}
	refKey, err := canonicalKey(side["evidence_ref"])
	if err != nil || !artifactHasEvidenceRef(artifact, refKey) ||
		(allowedRefs != nil && !allowedRefs[refKey]) {
		return boundObservation{}, fail("comparator.invalid")
	}
	observedOffset, offsetOK := integer(side["observed_offset_ns"])
	artifactOffset, artifactOffsetOK := integer(artifact["recorder_ingested_offset_ns"])
	if !offsetOK || !artifactOffsetOK || observedOffset != artifactOffset {
		return boundObservation{}, fail("comparator.invalid")
	}
	normalized, ok := objectValue(artifact["normalized_evidence"])
	if !ok {
		return boundObservation{}, fail("comparator.invalid")
	}
	valuePointer, valuePointerOK := stringValue(side["value_pointer"])
	unitPointer, unitPointerOK := stringValue(side["unit_pointer"])
	if !valuePointerOK || !unitPointerOK {
		return boundObservation{}, fail("comparator.invalid")
	}
	selectedValue, err := pointerGet(normalized, valuePointer)
	if err != nil {
		return boundObservation{}, err
	}
	selectedUnit, err := pointerGet(normalized, unitPointer)
	if err != nil {
		return boundObservation{}, err
	}
	if !reflectJSONEqual(selectedValue, side["native_decimal"]) ||
		!reflectJSONEqual(selectedUnit, side["native_unit"]) {
		return boundObservation{}, fail("comparator.invalid")
	}
	var parsed *big.Rat
	var text string
	if selectedValue != nil {
		text, ok = stringValue(selectedValue)
		if !ok {
			return boundObservation{}, fail("comparator.invalid")
		}
		parsed, err = parseExactDecimal(text, false)
		if err != nil {
			return boundObservation{}, err
		}
	}
	var unit *string
	if selectedUnit != nil {
		unitText, ok := stringValue(selectedUnit)
		if !ok || !token(unitText) {
			return boundObservation{}, fail("comparator.invalid")
		}
		unit = &unitText
	}
	return boundObservation{value: parsed, text: text, unit: unit, offsetNS: observedOffset}, nil
}

func pointerGet(value any, pointer string) (any, error) {
	if len(pointer) == 0 || len(pointer) > 512 || pointer[0] != '/' {
		return nil, fail("comparator.invalid")
	}
	current := value
	for _, rawSegment := range strings.Split(pointer[1:], "/") {
		if strings.Contains(strings.ReplaceAll(strings.ReplaceAll(rawSegment, "~0", ""), "~1", ""), "~") {
			return nil, fail("comparator.invalid")
		}
		segment := strings.ReplaceAll(strings.ReplaceAll(rawSegment, "~1", "/"), "~0", "~")
		switch node := current.(type) {
		case map[string]any:
			next, exists := node[segment]
			if !exists {
				return nil, fail("comparator.invalid")
			}
			current = next
		case []any:
			if segment == "" || len(segment) > 1 && segment[0] == '0' {
				return nil, fail("comparator.invalid")
			}
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				return nil, fail("comparator.invalid")
			}
			current = node[index]
		default:
			return nil, fail("comparator.invalid")
		}
	}
	return current, nil
}

func validateComparatorParameters(parameters map[string]any) error {
	if !exactKeys(parameters,
		"window", "tolerance", "unit_conversion", "rounding",
		"minimum_samples", "maximum_missing_samples", "stale_cutoff_ns", "conflict_threshold",
	) {
		return fail("comparator.invalid")
	}
	window, windowOK := exactObject(parameters["window"], "start_offset_ns", "end_offset_ns")
	tolerance, toleranceOK := exactObject(parameters["tolerance"], "absolute_decimal", "relative_ppm")
	conversion, conversionOK := exactObject(parameters["unit_conversion"], "mode", "source_unit", "target_unit", "scale_decimal", "offset_decimal")
	rounding, roundingOK := exactObject(parameters["rounding"], "mode", "decimal_places")
	threshold, thresholdOK := exactObject(parameters["conflict_threshold"], "absolute_decimal", "consecutive_samples")
	start, startOK := integer(window["start_offset_ns"])
	end, endOK := integer(window["end_offset_ns"])
	relative, relativeOK := integer(tolerance["relative_ppm"])
	minimum, minimumOK := integer(parameters["minimum_samples"])
	maximumMissing, maximumMissingOK := integer(parameters["maximum_missing_samples"])
	stale, staleOK := integer(parameters["stale_cutoff_ns"])
	consecutive, consecutiveOK := integer(threshold["consecutive_samples"])
	if !windowOK || !toleranceOK || !conversionOK || !roundingOK || !thresholdOK ||
		!startOK || !endOK || start < 0 || start >= end ||
		!relativeOK || relative < 0 || relative > 1_000_000 ||
		!minimumOK || minimum < 1 || minimum > 1024 ||
		!maximumMissingOK || maximumMissing < 0 || maximumMissing > 1024 ||
		!staleOK || stale < 1 ||
		!consecutiveOK || consecutive < 1 || consecutive > 1024 ||
		!member(conversion["mode"], "IDENTITY", "AFFINE") ||
		!token(conversion["source_unit"]) || !token(conversion["target_unit"]) ||
		!member(rounding["mode"], "NONE", "HALF_EVEN") {
		return fail("comparator.invalid")
	}
	if rounding["mode"] == "NONE" && rounding["decimal_places"] != nil {
		return fail("comparator.invalid")
	}
	if rounding["mode"] == "HALF_EVEN" && !boundedInteger(rounding["decimal_places"], 0, 9) {
		return fail("comparator.invalid")
	}
	if conversion["mode"] == "IDENTITY" &&
		(conversion["source_unit"] != conversion["target_unit"] ||
			conversion["scale_decimal"] != "1" || conversion["offset_decimal"] != "0") {
		return fail("comparator.invalid")
	}
	if _, err := parseExactDecimal(asString(tolerance["absolute_decimal"]), true); err != nil {
		return err
	}
	if _, err := parseExactDecimal(asString(conversion["scale_decimal"]), false); err != nil {
		return err
	}
	if _, err := parseExactDecimal(asString(conversion["offset_decimal"]), false); err != nil {
		return err
	}
	if _, err := parseExactDecimal(asString(threshold["absolute_decimal"]), true); err != nil {
		return err
	}
	return nil
}

func parseExactDecimal(value string, nonnegative bool) (*big.Rat, error) {
	if !decimalString(value, nonnegative) {
		return nil, fail("comparator.invalid")
	}
	sign := int64(1)
	unsignedValue := value
	if strings.HasPrefix(unsignedValue, "-") {
		sign = -1
		unsignedValue = unsignedValue[1:]
	}
	parts := strings.Split(unsignedValue, ".")
	digits := parts[0]
	scale := 0
	if len(parts) == 2 {
		digits += parts[1]
		scale = len(parts[1])
	}
	numerator := new(big.Int)
	if _, ok := numerator.SetString(digits, 10); !ok {
		return nil, fail("comparator.invalid")
	}
	if sign < 0 {
		numerator.Neg(numerator)
	}
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(numerator, denominator), nil
}

func roundExactDecimal(value *big.Rat, original string, rounding map[string]any) (*big.Rat, string, error) {
	if rounding["mode"] == "NONE" {
		return new(big.Rat).Set(value), original, nil
	}
	places, ok := integer(rounding["decimal_places"])
	if !ok || places < 0 || places > 9 {
		return nil, "", fail("comparator.invalid")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(places), nil)
	numerator := new(big.Int).Mul(value.Num(), scale)
	sign := numerator.Sign()
	if sign < 0 {
		numerator.Abs(numerator)
	}
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, value.Denom(), remainder)
	twiceRemainder := new(big.Int).Lsh(new(big.Int).Set(remainder), 1)
	comparison := twiceRemainder.Cmp(value.Denom())
	if comparison > 0 || comparison == 0 && quotient.Bit(0) == 1 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if sign < 0 {
		quotient.Neg(quotient)
	}
	rounded := new(big.Rat).SetFrac(new(big.Int).Set(quotient), scale)
	return rounded, formatScaledInteger(quotient, int(places), sign < 0), nil
}

func formatScaledInteger(value *big.Int, places int, negativeZero bool) string {
	sign := ""
	absolute := new(big.Int).Set(value)
	if absolute.Sign() < 0 {
		sign = "-"
		absolute.Abs(absolute)
	} else if absolute.Sign() == 0 && negativeZero {
		sign = "-"
	}
	digits := absolute.String()
	if places == 0 {
		return sign + digits
	}
	if len(digits) <= places {
		digits = strings.Repeat("0", places-len(digits)+1) + digits
	}
	split := len(digits) - places
	return sign + digits[:split] + "." + digits[split:]
}

func absRat(value *big.Rat) *big.Rat {
	result := new(big.Rat).Set(value)
	if result.Sign() < 0 {
		result.Neg(result)
	}
	return result
}

func reflectJSONEqual(left, right any) bool {
	leftKey, leftErr := canonicalKey(left)
	rightKey, rightErr := canonicalKey(right)
	return leftErr == nil && rightErr == nil && leftKey == rightKey
}
