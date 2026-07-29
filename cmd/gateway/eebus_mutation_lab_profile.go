package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const eebusMutationLabProfileBasename = "mutation-lab-profile-v1.json"

var errEEBusMutationLabProfileLoad = errors.New("eeBUS mutation lab profile load failed")

type eebusMutationLabJSONObjectSchema map[string]eebusMutationLabJSONObjectSchema

var eebusMutationLabProfileJSONSchema = eebusMutationLabJSONObjectSchema{
	"contract":                  nil,
	"profile_id":                nil,
	"target":                    eebusMutationLabTargetJSONSchema,
	"allowed_value_hashes":      nil,
	"rollback_value_hash":       nil,
	"maximum_probe_ttl_seconds": nil,
	"safety_predicates":         nil,
	"evidence_hashes":           nil,
	"expires_at":                nil,
}

var eebusMutationLabTargetJSONSchema = eebusMutationLabJSONObjectSchema{
	"remote_ski":      nil,
	"ship_id":         nil,
	"device_address":  nil,
	"entity_address":  nil,
	"feature_address": nil,
	"feature_type":    nil,
	"feature_role":    nil,
	"function":        nil,
	"operation":       nil,
}

func loadEEBusMutationLabProfile(
	stateRoot string,
) (*eebusraw.MutationLabProfileV1, error) {
	raw, present, err := readEEBusMutationLabProfileFile(stateRoot)
	if err != nil {
		return nil, errEEBusMutationLabProfileLoad
	}
	if !present {
		return nil, nil
	}
	defer clear(raw)

	profile, err := decodeEEBusMutationLabProfile(raw)
	if err != nil {
		return nil, errEEBusMutationLabProfileLoad
	}
	cloned := profile.Clone()
	return &cloned, nil
}

func decodeEEBusMutationLabProfile(
	raw []byte,
) (eebusraw.MutationLabProfileV1, error) {
	if err := validateSingleEEBusMutationLabJSONObject(raw); err != nil {
		return eebusraw.MutationLabProfileV1{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var profile eebusraw.MutationLabProfileV1
	if err := decoder.Decode(&profile); err != nil {
		return eebusraw.MutationLabProfileV1{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return eebusraw.MutationLabProfileV1{}, errors.New("profile JSON has trailing content")
	}
	if terminal := eebusraw.ValidateMutationLabProfileV1(profile); terminal != nil {
		return eebusraw.MutationLabProfileV1{}, errors.New("profile validation failed")
	}
	return profile, nil
}

func validateSingleEEBusMutationLabJSONObject(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return errors.New("profile JSON root is not an object")
	}
	if err := walkEEBusMutationLabJSONObject(
		decoder,
		eebusMutationLabProfileJSONSchema,
	); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("profile JSON has trailing content")
	}
	return nil
}

func walkEEBusMutationLabJSONValue(
	decoder *json.Decoder,
	schema eebusMutationLabJSONObjectSchema,
) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		return walkEEBusMutationLabJSONObject(decoder, schema)
	case '[':
		for decoder.More() {
			if err := walkEEBusMutationLabJSONValue(decoder, nil); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("profile JSON array is unterminated")
		}
		return nil
	default:
		return errors.New("profile JSON has an unexpected delimiter")
	}
}

func walkEEBusMutationLabJSONObject(
	decoder *json.Decoder,
	schema eebusMutationLabJSONObjectSchema,
) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		rawKey, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := rawKey.(string)
		if !ok {
			return errors.New("profile JSON object key is invalid")
		}
		if _, duplicate := seen[key]; duplicate {
			return errors.New("profile JSON object key is duplicated")
		}
		seen[key] = struct{}{}
		childSchema, allowed := schema[key]
		if schema != nil && !allowed {
			return errors.New("profile JSON object key is unknown")
		}
		if err := walkEEBusMutationLabJSONValue(decoder, childSchema); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return errors.New("profile JSON object is unterminated")
	}
	return nil
}
