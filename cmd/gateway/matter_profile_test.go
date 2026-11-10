package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestMatterProfileFlagAndLifecycle(t *testing.T) {
	fs := flag.NewFlagSet("profile", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	wrap := bindMatterProfile(fs)
	if got := fs.Lookup("matter-library-enabled") != nil; got != matterProfileEnabled {
		t.Fatalf("Matter flag present = %v, want %v", got, matterProfileEnabled)
	}
	args := []string{}
	if matterProfileEnabled {
		args = append(args, "-matter-library-enabled")
	}
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := ebusgateway.DefaultConfig()
	sentinel := errors.New("lifecycle result")
	calls := 0
	err := wrap(func(got context.Context, config ebusgateway.Config) error {
		calls++
		if got != ctx || config.M2MGraphQL.Disabled() != cfg.M2MGraphQL.Disabled() {
			t.Fatal("ordinary lifecycle inputs changed")
		}
		return sentinel
	})(ctx, cfg)
	if calls != 1 || !errors.Is(err, sentinel) {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	if !matterProfileEnabled {
		if err := fs.Parse([]string{"-matter-library-enabled"}); err == nil {
			t.Fatal("excluded flag accepted")
		}
	}
}

func TestMatterProfilePortalAvailability(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	if got := portalConfigConstraints(cfg, nil).Matter; got {
		t.Fatal("disabled M2M advertises Matter")
	}
	// A synthetic listener setting is sufficient to enable the configuration;
	// this test opens no listener and loads no TLS material.
	cfg.M2MGraphQL.ListenAddr = "synthetic"
	if cfg.M2MGraphQL.Disabled() {
		t.Fatal("M2M fixture remained disabled")
	}
	if got := portalConfigConstraints(cfg, nil).Matter; got != matterProfileEnabled {
		t.Fatalf("Matter=%v want %v", got, matterProfileEnabled)
	}
	if cfg.M2MGraphQL.Disabled() {
		t.Fatal("profile changed ordinary M2M availability")
	}
}
