package main

import "testing"

func TestNormalizeDialHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain ipv4", in: "192.168.1.10", want: "192.168.1.10"},
		{name: "plain ipv6", in: "::1", want: "::1"},
		{name: "bracketed ipv6", in: "[::1]", want: "::1"},
		{name: "trim spaces", in: "  [2001:db8::1]  ", want: "2001:db8::1"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeDialHost(test.in); got != test.want {
				t.Fatalf("normalizeDialHost(%q) = %q; want %q", test.in, got, test.want)
			}
		})
	}
}
