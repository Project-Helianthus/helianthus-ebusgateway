package main

import "testing"

func TestShouldStopDiscoveryScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		total int
		want  bool
	}{
		{name: "no devices", total: 0, want: false},
		{name: "some devices", total: 1, want: true},
		{name: "many devices", total: 7, want: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldStopDiscoveryScan(test.total); got != test.want {
				t.Fatalf("shouldStopDiscoveryScan(%d) = %v; want %v", test.total, got, test.want)
			}
		})
	}
}
