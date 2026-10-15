package main

import (
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func TestGrowattStorageDecimalCanonicalOutput(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  semreg.Decimal
	}{
		{"positive_ten", 10, semreg.Decimal{Coefficient: "1", Exponent10: 1}},
		{"positive_twenty", 20, semreg.Decimal{Coefficient: "2", Exponent10: 1}},
		{"positive_hundred", 100, semreg.Decimal{Coefficient: "1", Exponent10: 2}},
		{"negative_ten", -10, semreg.Decimal{Coefficient: "-1", Exponent10: 1}},
		{"negative_twenty", -20, semreg.Decimal{Coefficient: "-2", Exponent10: 1}},
		{"negative_hundred", -100, semreg.Decimal{Coefficient: "-1", Exponent10: 2}},
		{"canonical_positive_integer", 18, semreg.Decimal{Coefficient: "18", Exponent10: 0}},
		{"canonical_negative_integer", -18, semreg.Decimal{Coefficient: "-18", Exponent10: 0}},
		{"canonical_positive_fraction", 12.5, semreg.Decimal{Coefficient: "125", Exponent10: -1}},
		{"canonical_negative_fraction", -12.5, semreg.Decimal{Coefficient: "-125", Exponent10: -1}},
		{"zero", 0, semreg.Decimal{Coefficient: "0", Exponent10: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("normalized_equivalent", func(t *testing.T) {
				if err := tt.want.Validate(); err != nil {
					t.Fatalf("pinned Decimal API rejected normalized equivalent %+v: %v", tt.want, err)
				}
			})
			t.Run("helper", func(t *testing.T) {
				got := growattDecimal(tt.input)
				if err := got.Validate(); err != nil {
					t.Errorf("growattDecimal(%v) = %+v; want valid exact decimal: %v", tt.input, got, err)
				}
				if got != tt.want {
					t.Errorf("growattDecimal(%v) = %+v; want %+v", tt.input, got, tt.want)
				}
			})
		})
	}
}
