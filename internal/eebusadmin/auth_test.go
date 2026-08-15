package eebusadmin

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestIssue817ServerConfigHasNoAuthenticationInput(t *testing.T) {
	typeOfConfig := reflect.TypeOf(Config{})
	for _, forbidden := range []string{"Auth", "OwnerUsername", "OwnerSecret", "HASecret", "OwnerOrigin", "SessionTTL"} {
		if _, ok := typeOfConfig.FieldByName(forbidden); ok {
			t.Errorf("eebusadmin.Config retains eeBUS-specific auth field %q", forbidden)
		}
	}
}

func TestIssue817AuthenticationImplementationIsRemoved(t *testing.T) {
	content, err := os.ReadFile("auth.go")
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, forbidden := range []string{
		"AuthConfig",
		"authentication",
		"authenticatedRequest",
		"ownerSession",
		"OwnerSecret",
		"HASecret",
		"Authorization",
		"Basic ",
		"Bearer ",
		"Cookie",
		"CSRF",
		"Origin",
		"Referer",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("auth.go retains eeBUS-specific auth/session token %q", forbidden)
		}
	}
}
