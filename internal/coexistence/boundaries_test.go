package coexistence

import "testing"

func TestIdentityKeyV1ClassifiesViaDeviceKeysNarrowly(t *testing.T) {
	for _, test := range []struct {
		key  string
		want bool
	}{
		{key: "via_device", want: true},
		{key: "via_parent_device", want: true},
		{key: "via", want: false},
		{key: "via_prose", want: false},
		{key: "via_device_note", want: false},
	} {
		t.Run(test.key, func(t *testing.T) {
			if got := identityKeyV1(keyTokensV1(test.key)); got != test.want {
				t.Fatalf("identityKeyV1(%q) = %t; want %t", test.key, got, test.want)
			}
		})
	}
}
