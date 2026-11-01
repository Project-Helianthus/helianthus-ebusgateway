package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/libraryhost"
)

const (
	matterLibraryEnabledFlag = "matter-library-enabled"
	// matterLibraryStopBudget bounds the drain that precedes gateway release.
	matterLibraryStopBudget = 10 * time.Second

	matterLibraryReasonCompositionMissing libraryhost.Reason = "composition_missing"
	matterLibraryReasonCompositionFailed  libraryhost.Reason = "composition_failed"
)

var errMatterLibraryLifecycleMissing = errors.New("matter library gateway lifecycle is missing")

// matterLibraryOption is the explicit in-process feature option. Its zero value
// is disabled, and it stays apart from the gateway's own configuration.
type matterLibraryOption struct{ enabled bool }

// bindMatterLibraryFlags binds the option on an explicit flag set. No real
// factory exists yet, so enabling it alone resolves to unavailable.
func bindMatterLibraryFlags(fs *flag.FlagSet) *matterLibraryOption {
	option := &matterLibraryOption{}
	if fs == nil {
		return option
	}
	fs.BoolVar(&option.enabled, matterLibraryEnabledFlag, false, "enable the in-process Matter library host; without complete qualification evidence it stays unavailable")
	return option
}

// matterLibraryComposer builds the injected ports. It runs only when the option
// is enabled, so a disabled gateway constructs none of them.
type matterLibraryComposer func() (libraryhost.Config, error)

// matterLibraryOwner is the injected library owner of the ordinary lifecycle.
// Start reports whether the owner holds anything Stop must release.
type matterLibraryOwner interface {
	Start(context.Context) (bool, error)
	Stop(context.Context) error
}

// matterLibraryComposition composes the host from its option and composer.
type matterLibraryComposition struct {
	option  *matterLibraryOption
	compose matterLibraryComposer

	mu     sync.Mutex
	host   *libraryhost.Host
	reason libraryhost.Reason
}

func newMatterLibraryComposition(option *matterLibraryOption, compose matterLibraryComposer) *matterLibraryComposition {
	return &matterLibraryComposition{option: option, compose: compose}
}

// Start resolves the option before anything is constructed. Unavailable is
// retained as categorical state and is not a gateway failure.
func (c *matterLibraryComposition) Start(ctx context.Context) (bool, error) {
	if c == nil || c.option == nil || !c.option.enabled {
		return false, nil
	}
	host, err := c.resolveHost()
	if err != nil {
		return false, err
	}
	err = host.Start(ctx)
	state := host.Status().State
	return state == libraryhost.StateRunning || state == libraryhost.StateQuarantined, err
}

func (c *matterLibraryComposition) resolveHost() (*libraryhost.Host, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.host != nil {
		return c.host, nil
	}
	if c.compose == nil {
		c.reason = matterLibraryReasonCompositionMissing
		return nil, &libraryhost.UnavailableError{Reason: c.reason}
	}
	cfg, err := c.compose()
	if err != nil {
		c.reason = matterLibraryReasonCompositionFailed
		return nil, &libraryhost.UnavailableError{Reason: c.reason}
	}
	cfg.Enabled = true
	c.host = libraryhost.New(cfg)
	c.reason = ""
	return c.host, nil
}

// Stop is idempotent and does nothing when no host was composed.
func (c *matterLibraryComposition) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	host := c.host
	c.mu.Unlock()
	if host == nil {
		return nil
	}
	return host.Stop(ctx)
}

// Status reports the composed host, or why none was composed.
func (c *matterLibraryComposition) Status() libraryhost.Status {
	if c == nil || c.option == nil || !c.option.enabled {
		return libraryhost.Status{State: libraryhost.StateDisabled}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.host != nil {
		return c.host.Status()
	}
	if c.reason != "" {
		return libraryhost.Status{State: libraryhost.StateUnavailable, Reason: c.reason}
	}
	return libraryhost.Status{State: libraryhost.StateStopped}
}

// runGatewayLifecycleWithMatterLibrary runs the ordinary gateway lifecycle with
// an injected library owner. An inactive owner leaves the lifecycle unchanged.
func runGatewayLifecycleWithMatterLibrary(ctx context.Context, cfg ebusgateway.Config, owner matterLibraryOwner, run func(context.Context, ebusgateway.Config) error) error {
	return superviseMatterLibrary(ctx, cfg, owner, run, matterLibraryStopContext)
}

// matterLibraryStopContext outlives the cancelled parent but stays finite.
func matterLibraryStopContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), matterLibraryStopBudget)
}

func superviseMatterLibrary(ctx context.Context, cfg ebusgateway.Config, owner matterLibraryOwner, run func(context.Context, ebusgateway.Config) error, stopContext func(context.Context) (context.Context, context.CancelFunc)) error {
	if run == nil {
		return errMatterLibraryLifecycleMissing
	}
	if owner == nil {
		return run(ctx, cfg)
	}
	active, err := owner.Start(ctx)
	if err != nil {
		log.Printf("gateway: matter library: %v", err)
	}
	if !active {
		return run(ctx, cfg)
	}

	// The library is fenced and drained while the gateway still serves its
	// semantic source, so the gateway sees cancellation only after that stop.
	runCtx, releaseGateway := context.WithCancel(context.WithoutCancel(ctx))
	defer releaseGateway()
	runDone := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-runDone:
		}
		stopCtx, cancel := stopContext(ctx)
		defer cancel()
		err := owner.Stop(stopCtx)
		releaseGateway()
		stopped <- err
	}()
	result := run(runCtx, cfg)
	close(runDone)
	return errors.Join(result, <-stopped)
}
