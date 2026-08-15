package main

import (
	"flag"
	"os"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestIssue817NoEEBusSpecificAdminFlagsAreRegistered(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("issue817", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	fs.VisitAll(func(value *flag.Flag) {
		if strings.HasPrefix(value.Name, "eebus-admin-") {
			t.Errorf("eeBUS-specific admin flag remains registered: -%s", value.Name)
		}
	})
}

func TestIssue817AdminCompositionContainsNoCredentialOrSessionLoader(t *testing.T) {
	content, err := os.ReadFile("eebus_admin_config.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, forbidden := range []string{
		"bindEEBusAdminFlags",
		"loadEEBusAdminAuthConfig",
		"readEEBusAdminSecret",
		"validEEBusAdminUsername",
		"validEEBusAdminOrigin",
		"EEBusAdminConfig",
		"AuthConfig",
		"OwnerSecret",
		"HASecret",
		"OwnerOrigin",
		"SessionTTL",
		"credential",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("eebus_admin_config.go retains eeBUS-specific auth/config token %q", forbidden)
		}
	}
}
