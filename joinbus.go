package ebusgateway

// Package-doc continued: JoinBus adapter.
//
// This file implements the JoinBus interface from ebusgo/protocol, wired
// over the PassiveTransactionReconstructor's subscription channel. It is
// the source-of-truth bridge for startup-admission warmup on join-capable
// direct transports (ENH, ENS, UDP-plain, TCP-plain). ebusd-tcp does NOT
// instantiate this adapter per AD13.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

var ErrJoinBusInquiryUnsupported = errors.New("joinbus: inquiry not supported (InquiryEnabled=false)")

type joinBusAdapter struct {
	reconstructor  *PassiveTransactionReconstructor
	name           string
	priority       PassiveSubscriberPriority
	buffer         int
	inquiryEnabled bool
}

// NewJoinBusAdapter returns a protocol.JoinBus subscribed to the given
// reconstructor with priority=NonCritical and default buffer.
func NewJoinBusAdapter(reconstructor *PassiveTransactionReconstructor, name string, inquiryEnabled bool) (protocol.JoinBus, error) {
	if reconstructor == nil {
		return nil, fmt.Errorf("joinbus: reconstructor is nil")
	}
	if name == "" {
		name = "startup_admission_joinbus"
	}
	return &joinBusAdapter{
		reconstructor:  reconstructor,
		name:           name,
		priority:       PassiveSubscriberNonCritical,
		buffer:         0,
		inquiryEnabled: inquiryEnabled,
	}, nil
}

func (a *joinBusAdapter) Listen(ctx context.Context, onFrame func(protocol.Frame)) error {
	if ctx == nil {
		ctx = context.Background()
	}

	subscription, err := a.reconstructor.Subscribe(a.name, a.priority, a.buffer)
	if err != nil {
		return fmt.Errorf("joinbus: subscribe: %w", err)
	}
	defer subscription.Close()

	events := subscription.Events()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return nil
			}
			forwardJoinBusEvent(event, onFrame)
		}
	}
}

func forwardJoinBusEvent(event PassiveClassifiedEvent, onFrame func(protocol.Frame)) {
	if onFrame == nil {
		return
	}

	switch event.Kind {
	case PassiveClassifiedEventBroadcastFrame, PassiveClassifiedEventMasterFrame:
		onFrame(event.Request)
	case PassiveClassifiedEventTransaction:
		onFrame(event.Request)
		if event.HasResponse {
			onFrame(event.Response)
		}
	}
}

func (a *joinBusAdapter) InquiryExistence(ctx context.Context) error {
	return ErrJoinBusInquiryUnsupported
}

// DefaultStartupAdmissionJoinConfig returns the JoinConfig used by the
// startup-admission-discovery plan for join-capable direct transports.
// See plan AD01/AD02/AD09 and
// helianthus-docs-ebus/architecture/startup-admission-and-discovery.md §2.2.3.
func DefaultStartupAdmissionJoinConfig() protocol.JoinConfig {
	return protocol.JoinConfig{
		ListenWarmup:       5 * time.Second,
		InquiryEnabled:     false,
		PersistLastGood:    true,
		PersistLastGoodSet: true,
	}
}
