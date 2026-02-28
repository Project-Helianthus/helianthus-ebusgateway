package mdns

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

const (
	// ServiceTypeGateway is the DNS-SD service type advertised for the GraphQL endpoint.
	ServiceTypeGateway = "_helianthus-graphql._tcp"

	defaultDomain = "local."
)

var ErrUnsupported = errors.New("mdns: unsupported")

type Service struct {
	Instance string
	Service  string
	Domain   string
	Port     int
	Text     []string
}

type Advertiser interface {
	Close() error
}

type registrar interface {
	Register(service Service) (Advertiser, error)
}

func Advertise(ctx context.Context, service Service) (Advertiser, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	service, err := normalizeAndValidate(service)
	if err != nil {
		return nil, err
	}

	return advertiseWithRegistrar(ctx, service, defaultRegistrar())
}

func advertiseWithRegistrar(ctx context.Context, service Service, reg registrar) (Advertiser, error) {
	if reg == nil {
		return nil, fmt.Errorf("mdns advertiser missing registrar: %w", ebuserrors.ErrInvalidPayload)
	}

	inner, err := reg.Register(service)
	if err != nil {
		return nil, err
	}

	advertiser := &managedAdvertiser{
		inner: inner,
	}

	if done := ctx.Done(); done != nil {
		// Goroutine exits when ctx.Done() is closed.
		go func() {
			<-done
			_ = advertiser.Close()
		}()
	}

	return advertiser, nil
}

type managedAdvertiser struct {
	inner Advertiser

	closeOnce sync.Once
	closeErr  error
}

func (a *managedAdvertiser) Close() error {
	if a == nil || a.inner == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		a.closeErr = a.inner.Close()
	})
	return a.closeErr
}

func normalizeAndValidate(service Service) (Service, error) {
	service.Instance = strings.TrimSpace(service.Instance)
	service.Service = strings.TrimSpace(service.Service)
	service.Domain = strings.TrimSpace(service.Domain)

	if service.Instance == "" {
		return Service{}, fmt.Errorf("mdns service missing instance: %w", ebuserrors.ErrInvalidPayload)
	}
	if service.Service == "" {
		return Service{}, fmt.Errorf("mdns service missing service type: %w", ebuserrors.ErrInvalidPayload)
	}
	if service.Domain == "" {
		service.Domain = defaultDomain
	}
	if !strings.HasSuffix(service.Domain, ".") {
		service.Domain += "."
	}
	if service.Port <= 0 || service.Port > 65535 {
		return Service{}, fmt.Errorf("mdns service invalid port %d: %w", service.Port, ebuserrors.ErrInvalidPayload)
	}
	if service.Text == nil {
		service.Text = []string{}
	} else {
		service.Text = append([]string(nil), service.Text...)
	}
	return service, nil
}
