package ebusgateway

import (
	"fmt"
	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	"strings"
	"testing"
	"time"
)

func cx18Config() GrowattPCSConfig {
	id, _ := canbus.NewInterfaceIdentity("synthetic0", 7)
	return GrowattPCSConfig{Enabled: true, ListenOnlyConfirmed: true, StaleAfter: time.Second, Native: canbusreg.CX01SelectionSpec{Interface: id, Flavor: "pcs-standard", ProjectCode: "QAAA", Release: "0011", Direction: "bms-to-pcs", Scope: "local", Bank: "ordinary", Endpoint: "synthetic", Epoch: 1}}
}
func TestCX18Config(t *testing.T) {
	if _, err := (GrowattPCSConfig{}).Selection(); err != nil {
		t.Fatal(err)
	}
	if _, err := (GrowattLINKConfig{}).Selections(); err != nil {
		t.Fatal(err)
	}
	c := cx18Config()
	if _, err := c.Selection(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*GrowattPCSConfig){
		"disabled":     func(c *GrowattPCSConfig) { c.Enabled = false },
		"confirmation": func(c *GrowattPCSConfig) { c.ListenOnlyConfirmed = false },
		"zero-age":     func(c *GrowattPCSConfig) { c.StaleAfter = 0 },
		"negative-age": func(c *GrowattPCSConfig) { c.StaleAfter = -1 },
		"interface":    func(c *GrowattPCSConfig) { c.Native.Interface = canbus.InterfaceIdentity{} },
		"flavor":       func(c *GrowattPCSConfig) { c.Native.Flavor = "inferred" },
		"project-case": func(c *GrowattPCSConfig) { c.Native.ProjectCode = "qaaa" },
		"release":      func(c *GrowattPCSConfig) { c.Native.Release = "9999" },
		"contract":     func(c *GrowattPCSConfig) { c.Native.Choice = "auto" },
		"direction":    func(c *GrowattPCSConfig) { c.Native.Direction = "pcs-to-bms" },
		"scope":        func(c *GrowattPCSConfig) { c.Native.Scope = "auto" },
		"bank":         func(c *GrowattPCSConfig) { c.Native.Bank = "parallel" },
		"endpoint":     func(c *GrowattPCSConfig) { c.Native.Endpoint = "" },
		"epoch":        func(c *GrowattPCSConfig) { c.Native.Epoch = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := cx18Config()
			mutate(&c)
			if _, err := c.Selection(); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	c.Native.ProjectCode, c.Native.Release, c.Native.Choice = "QAAa", "9011", ""
	if _, err := c.Selection(); err == nil {
		t.Fatal("project selected revision")
	}
	for _, choice := range []string{"legacy19", "release11"} {
		c.Native.Choice = choice
		if _, err := c.Selection(); err != nil {
			t.Fatal(err)
		}
	}
	c = cx18Config()
	c.Native.Flavor = "pcs-parallel"
	c.Native.Bank = "parallel"
	if _, err := c.Selection(); err != nil {
		t.Fatal(err)
	}
	c = cx18Config()
	c.Native.Flavor = "pcs-extended19"
	c.Native.Release = "0007"
	c.Native.RouteSet = true
	if _, err := c.Selection(); err != nil {
		t.Fatal(err)
	}
	c.Native.RouteSet = false
	if _, err := c.Selection(); err == nil {
		t.Fatal("missing route")
	}
}
func TestCX18SourceSelectors(t *testing.T) {
	good := `[{"source":1,"use_default":true,"contract":"release11"},{"source":254,"project_code":"QAAa","release":"9011","contract":"legacy19"}]`
	entries, err := ParseGrowattLINKSourceSelectors(good, "QAAA", "0011")
	if err != nil || len(entries) != 2 || entries[1].ProjectCode != "QAAa" {
		t.Fatal(entries, err)
	}
	for _, source := range []int{0, 1, 243, 244, 245, 246, 254, 255, 256} {
		t.Run(fmt.Sprint(source), func(t *testing.T) {
			_, err := ParseGrowattLINKSourceSelectors(fmt.Sprintf(`[{"source":%d,"use_default":true,"contract":"release11"}]`, source), "QAAA", "0011")
			valid := source >= 1 && source <= 254 && source != 244 && source != 245
			if (err == nil) != valid {
				t.Fatal(err)
			}
		})
	}
	invalid := []string{`null`, `[]`, `{}`, good + ` null`, `[{"source":1,"source":2}]`, `[{"source":1.0,"use_default":true,"contract":"release11"}]`, `[{"source":1,"use_default":false,"contract":"release11"}]`, `[{"source":1,"use_default":true,"project_code":"QAAA","contract":"release11"}]`, `[{"source":1,"use_default":true,"contract":"auto"}]`, `[{"source":1,"use_default":true,"contract":"release11","unknown":0}]`, `[{"source":1,"use_default":true,"contract":"release11"},{"source":1,"use_default":true,"contract":"legacy19"}]`, good[:len(good)-1]}
	for n, value := range invalid {
		t.Run(fmt.Sprint("invalid", n), func(t *testing.T) {
			if _, err := ParseGrowattLINKSourceSelectors(value, "QAAA", "0011"); err == nil {
				t.Fatal(value)
			}
		})
	}
	for _, count := range []int{32, 33} {
		var parts []string
		for n := 1; n <= count; n++ {
			parts = append(parts, fmt.Sprintf(`{"source":%d,"use_default":true,"contract":"release11"}`, n))
		}
		_, err := ParseGrowattLINKSourceSelectors("["+strings.Join(parts, ",")+"]", "QAAA", "0011")
		if (err == nil) != (count == 32) {
			t.Fatal(count, err)
		}
	}
	c := cx18Config()
	c.Native.Flavor = "link"
	c.Native.PCSFlavor = "pcs-standard"
	c.Native.Direction = "module-to-coordinator"
	c.Native.RouteSet = true
	c.Native.Destination = 244
	link := GrowattLINKConfig{Enabled: true, ListenOnlyConfirmed: true, StaleAfter: time.Second, Native: c.Native, SourceSelectors: good}
	sels, err := link.Selections()
	if err != nil || len(sels) != 2 || sels[0].CX01Spec().Source != 1 || sels[1].CX01Spec().Choice != "legacy19" {
		t.Fatal(sels, err)
	}
	link.Enabled = false
	if _, err := link.Selections(); err == nil {
		t.Fatal("active disabled LINK")
	}
	// Escaped code is decoded exactly once; mixed case remains significant.
	escaped := `[{"source":1,"project_code":"QA\u0041a","release":"9011","contract":"release11"}]`
	entries, err = ParseGrowattLINKSourceSelectors(escaped, "", "")
	if err != nil || entries[0].ProjectCode != "QAAa" {
		t.Fatal(entries, err)
	}
}
