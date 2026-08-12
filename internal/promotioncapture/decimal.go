package promotioncapture

import (
	"fmt"
	"math/big"
)

const (
	minimumDecimalScale = -12
	maximumDecimalScale = 12
	maximumSafeInteger  = int64(9_007_199_254_740_991)
)

var maximumSafeBigInt = big.NewInt(maximumSafeInteger)

type exactDecimal struct {
	coefficient *big.Int
	scale       int
}

func (value Decimal) Validate() error {
	if value.Number < -maximumSafeInteger || value.Number > maximumSafeInteger ||
		value.Scale < minimumDecimalScale || value.Scale > maximumDecimalScale {
		return fmt.Errorf("%w: number=%d scale=%d", ErrInvalidDecimal, value.Number, value.Scale)
	}
	return nil
}

func (value Decimal) Compare(other Decimal) (int, error) {
	left, err := exactFromDecimal(value)
	if err != nil {
		return 0, err
	}
	right, err := exactFromDecimal(other)
	if err != nil {
		return 0, err
	}
	return compareExact(left, right), nil
}

func (value TypedValue) Validate() error {
	populated := 0
	if value.Decimal != nil {
		populated++
	}
	if value.Enum != nil {
		populated++
	}
	if value.Boolean != nil {
		populated++
	}
	if value.String != nil {
		populated++
	}
	if populated != 1 {
		return fmt.Errorf("%w: typed value must contain exactly one value", ErrInvalidEvidence)
	}
	switch value.Kind {
	case ValueNumeric:
		if value.Decimal == nil || value.Enum != nil || value.Boolean != nil || value.String != nil {
			return fmt.Errorf("%w: malformed numeric value", ErrInvalidEvidence)
		}
		return value.Decimal.Validate()
	case ValueEnum:
		if value.Enum == nil || value.Decimal != nil || value.Boolean != nil || value.String != nil || *value.Enum == "" {
			return fmt.Errorf("%w: malformed enum value", ErrInvalidEvidence)
		}
	case ValueBoolean:
		if value.Boolean == nil || value.Decimal != nil || value.Enum != nil || value.String != nil {
			return fmt.Errorf("%w: malformed boolean value", ErrInvalidEvidence)
		}
	case ValueString:
		if value.String == nil || value.Decimal != nil || value.Enum != nil || value.Boolean != nil ||
			*value.String == "" || len(*value.String) > 256 {
			return fmt.Errorf("%w: malformed string value", ErrInvalidEvidence)
		}
	default:
		return fmt.Errorf("%w: unknown value kind %q", ErrInvalidEvidence, value.Kind)
	}
	return nil
}

func CompareNumeric(ebus, eebus Decimal, constraints DeclaredConstraints, conversion Conversion) (NumericComparison, error) {
	minimum, err := exactFromDecimal(constraints.Minimum)
	if err != nil {
		return NumericComparison{}, err
	}
	maximum, err := exactFromDecimal(constraints.Maximum)
	if err != nil {
		return NumericComparison{}, err
	}
	if compareExact(minimum, maximum) > 0 {
		return NumericComparison{}, fmt.Errorf("%w: minimum exceeds maximum", ErrInvalidEvidence)
	}
	step, err := exactFromDecimal(constraints.Step)
	if err != nil || step.coefficient.Sign() <= 0 {
		return NumericComparison{}, fmt.Errorf("%w: %v", ErrInvalidStep, err)
	}

	left, err := convertExact(ebus, conversion)
	if err != nil {
		return NumericComparison{}, err
	}
	right, err := exactFromDecimal(eebus)
	if err != nil {
		return NumericComparison{}, err
	}
	if compareExact(left, minimum) < 0 || compareExact(left, maximum) > 0 ||
		compareExact(right, minimum) < 0 || compareExact(right, maximum) > 0 {
		return NumericComparison{}, ErrOutOfRange
	}

	delta := subtractExact(left, right)
	delta.coefficient.Abs(delta.coefficient)
	converted, err := decimalFromExact(left)
	if err != nil {
		return NumericComparison{}, err
	}
	rightDecimal, err := decimalFromExact(right)
	if err != nil {
		return NumericComparison{}, err
	}
	deltaDecimal, err := decimalFromExact(delta)
	if err != nil {
		return NumericComparison{}, err
	}
	return NumericComparison{
		ConvertedEBus: converted,
		EEBus:         rightDecimal,
		Delta:         deltaDecimal,
		Match:         compareExact(delta, step) <= 0,
	}, nil
}

func exactFromDecimal(value Decimal) (exactDecimal, error) {
	if err := value.Validate(); err != nil {
		return exactDecimal{}, err
	}
	return exactDecimal{coefficient: big.NewInt(value.Number), scale: value.Scale}, nil
}

func convertExact(value Decimal, conversion Conversion) (exactDecimal, error) {
	input, err := exactFromDecimal(value)
	if err != nil {
		return exactDecimal{}, err
	}
	scale, err := exactFromDecimal(conversion.Scale)
	if err != nil {
		return exactDecimal{}, fmt.Errorf("%w: conversion scale: %v", ErrInvalidEvidence, err)
	}
	offset, err := exactFromDecimal(conversion.Offset)
	if err != nil {
		return exactDecimal{}, fmt.Errorf("%w: conversion offset: %v", ErrInvalidEvidence, err)
	}
	if conversion.SourceUnit == "" || conversion.TargetUnit == "" {
		return exactDecimal{}, fmt.Errorf("%w: conversion unit missing", ErrInvalidEvidence)
	}
	if conversion.Mode == ConversionIdentity {
		if conversion.SourceUnit != conversion.TargetUnit || compareExact(scale, exactDecimal{big.NewInt(1), 0}) != 0 || offset.coefficient.Sign() != 0 {
			return exactDecimal{}, fmt.Errorf("%w: invalid identity conversion", ErrInvalidEvidence)
		}
	} else if conversion.Mode != ConversionAffine {
		return exactDecimal{}, fmt.Errorf("%w: unknown conversion mode %q", ErrInvalidEvidence, conversion.Mode)
	}
	return addExact(multiplyExact(input, scale), offset), nil
}

func multiplyExact(left, right exactDecimal) exactDecimal {
	return exactDecimal{
		coefficient: new(big.Int).Mul(left.coefficient, right.coefficient),
		scale:       left.scale + right.scale,
	}
}

func addExact(left, right exactDecimal) exactDecimal {
	leftCoefficient, rightCoefficient, scale := alignExact(left, right)
	return exactDecimal{coefficient: new(big.Int).Add(leftCoefficient, rightCoefficient), scale: scale}
}

func subtractExact(left, right exactDecimal) exactDecimal {
	leftCoefficient, rightCoefficient, scale := alignExact(left, right)
	return exactDecimal{coefficient: new(big.Int).Sub(leftCoefficient, rightCoefficient), scale: scale}
}

func compareExact(left, right exactDecimal) int {
	leftCoefficient, rightCoefficient, _ := alignExact(left, right)
	return leftCoefficient.Cmp(rightCoefficient)
}

func alignExact(left, right exactDecimal) (*big.Int, *big.Int, int) {
	scale := left.scale
	if right.scale < scale {
		scale = right.scale
	}
	leftCoefficient := new(big.Int).Set(left.coefficient)
	rightCoefficient := new(big.Int).Set(right.coefficient)
	if difference := left.scale - scale; difference > 0 {
		leftCoefficient.Mul(leftCoefficient, powerOfTen(difference))
	}
	if difference := right.scale - scale; difference > 0 {
		rightCoefficient.Mul(rightCoefficient, powerOfTen(difference))
	}
	return leftCoefficient, rightCoefficient, scale
}

func decimalFromExact(value exactDecimal) (Decimal, error) {
	coefficient := new(big.Int).Set(value.coefficient)
	scale := value.scale
	if coefficient.Sign() == 0 {
		return Decimal{}, nil
	}

	ten := big.NewInt(10)
	remainder := new(big.Int)
	for scale < maximumDecimalScale {
		remainder.Mod(coefficient, ten)
		if remainder.Sign() != 0 {
			break
		}
		coefficient.Quo(coefficient, ten)
		scale++
	}
	if scale > maximumDecimalScale {
		coefficient.Mul(coefficient, powerOfTen(scale-maximumDecimalScale))
		scale = maximumDecimalScale
	}
	if scale < minimumDecimalScale || new(big.Int).Abs(new(big.Int).Set(coefficient)).Cmp(maximumSafeBigInt) > 0 {
		return Decimal{}, fmt.Errorf("%w: result is not representable", ErrInvalidDecimal)
	}
	return Decimal{Number: coefficient.Int64(), Scale: scale}, nil
}

func powerOfTen(exponent int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil)
}
