package mdns

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
)

func TestNormalizeAndValidate(t *testing.T) {
	t.Run("defaults and trims", func(t *testing.T) {
		txt := []string{"path=/graphql"}
		got, err := normalizeAndValidate(Service{
			Instance: " gateway ",
			Service:  " _test._tcp ",
			Domain:   "local",
			Port:     8080,
			Text:     txt,
		})
		if err != nil {
			t.Fatalf("normalizeAndValidate error = %v", err)
		}
		if got.Instance != "gateway" {
			t.Fatalf("Instance = %q; want %q", got.Instance, "gateway")
		}
		if got.Service != "_test._tcp" {
			t.Fatalf("Service = %q; want %q", got.Service, "_test._tcp")
		}
		if got.Domain != "local." {
			t.Fatalf("Domain = %q; want %q", got.Domain, "local.")
		}
		if got.Port != 8080 {
			t.Fatalf("Port = %d; want 8080", got.Port)
		}
		if len(got.Text) != 1 || got.Text[0] != "path=/graphql" {
			t.Fatalf("Text = %#v; want %q", got.Text, "path=/graphql")
		}
		got.Text[0] = "mutated"
		if txt[0] != "path=/graphql" {
			t.Fatalf("Text was not copied; original=%#v", txt)
		}
	})

	t.Run("domain default", func(t *testing.T) {
		got, err := normalizeAndValidate(Service{
			Instance: "gateway",
			Service:  "_test._tcp",
			Port:     8080,
		})
		if err != nil {
			t.Fatalf("normalizeAndValidate error = %v", err)
		}
		if got.Domain != defaultDomain {
			t.Fatalf("Domain = %q; want %q", got.Domain, defaultDomain)
		}
		if got.Text == nil || len(got.Text) != 0 {
			t.Fatalf("Text = %#v; want empty slice", got.Text)
		}
	})

	t.Run("invalid instance", func(t *testing.T) {
		_, err := normalizeAndValidate(Service{Service: "_test._tcp", Port: 8080})
		if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
			t.Fatalf("error = %v; want ErrInvalidPayload", err)
		}
	})

	t.Run("invalid service", func(t *testing.T) {
		_, err := normalizeAndValidate(Service{Instance: "gateway", Port: 8080})
		if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
			t.Fatalf("error = %v; want ErrInvalidPayload", err)
		}
	})

	t.Run("invalid port", func(t *testing.T) {
		_, err := normalizeAndValidate(Service{Instance: "gateway", Service: "_test._tcp", Port: 0})
		if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
			t.Fatalf("error = %v; want ErrInvalidPayload", err)
		}
	})
}

func TestAdvertiseWithRegistrar(t *testing.T) {
	t.Run("errors on nil registrar", func(t *testing.T) {
		_, err := advertiseWithRegistrar(context.Background(), Service{
			Instance: "gateway",
			Service:  "_test._tcp",
			Domain:   "local.",
			Port:     8080,
		}, nil)
		if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
			t.Fatalf("error = %v; want ErrInvalidPayload", err)
		}
	})

	t.Run("closes on context cancel and is idempotent", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		inner := &countingAdvertiser{done: make(chan struct{})}
		reg := &fakeRegistrar{advertiser: inner}

		adv, err := advertiseWithRegistrar(ctx, Service{
			Instance: "gateway",
			Service:  "_test._tcp",
			Domain:   "local.",
			Port:     8080,
		}, reg)
		if err != nil {
			t.Fatalf("advertiseWithRegistrar error = %v", err)
		}
		if !reg.called {
			t.Fatalf("registrar was not called")
		}

		cancel()

		select {
		case <-inner.done:
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("timeout waiting for Close after cancel")
		}

		if err := adv.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
		if err := adv.Close(); err != nil {
			t.Fatalf("Close (2nd) error = %v", err)
		}
		if got := inner.closes(); got != 1 {
			t.Fatalf("inner Close calls = %d; want 1", got)
		}
	})
}

type fakeRegistrar struct {
	called     bool
	last       Service
	advertiser Advertiser
	err        error
}

func (r *fakeRegistrar) Register(service Service) (Advertiser, error) {
	r.called = true
	r.last = service
	if r.err != nil {
		return nil, r.err
	}
	return r.advertiser, nil
}

type countingAdvertiser struct {
	count atomic.Int32
	done  chan struct{}
}

func (a *countingAdvertiser) Close() error {
	if a == nil {
		return nil
	}
	if a.count.Add(1) == 1 && a.done != nil {
		close(a.done)
	}
	return nil
}

func (a *countingAdvertiser) closes() int32 {
	if a == nil {
		return 0
	}
	return a.count.Load()
}
