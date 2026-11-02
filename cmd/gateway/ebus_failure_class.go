package main

import (
	"context"
	"errors"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

// ebusFailureDetail describes one failed ebus.primary attempt for the recovery
// view: its closed class and its cause text. The text is the one the recovery
// log prints, the innermost cause with every address replaced.
func ebusFailureDetail(err error) drivermanager.FailureDetail {
	return drivermanager.FailureDetail{Class: ebusFailureClass(err), Cause: ebusRecoveryErrorText(err)}
}

// ebusFailureClass maps a failed attempt to the closed class of the recovery
// view, from the error category of the recovery log:
//
//   - a failed dial (refused, unreachable or any other dial error) is
//     DIAL_FAILED;
//   - a dial timeout, an INIT timeout and an exceeded deadline are TIMEOUT;
//   - a closed transport, and an adapter that reset the connection, are
//     TRANSPORT_CLOSED;
//   - everything else is OTHER.
func ebusFailureClass(err error) drivermanager.FailureClass {
	category := ebusRecoveryFailureCategory(err)
	switch {
	case category == "canceled" && errors.Is(err, context.DeadlineExceeded):
		return drivermanager.FailureTimeout
	case category == "start_failed" && errors.Is(err, ebuserrors.ErrAdapterReset):
		return drivermanager.FailureTransportClosed
	}
	return ebusCategoryFailureClass(category)
}

// ebusCategoryFailureClass maps one closed error category of the recovery log
// to its class.
func ebusCategoryFailureClass(category string) drivermanager.FailureClass {
	switch category {
	case "dial_refused", "dial_unreachable", "dial_failed":
		return drivermanager.FailureDialFailed
	case "dial_timeout", "handshake_timeout":
		return drivermanager.FailureTimeout
	case "transport_closed":
		return drivermanager.FailureTransportClosed
	}
	return drivermanager.FailureOther
}

// ebusLossDetail describes a lost connection for the recovery view by its
// closed trigger (the one the recovery log prints): the adapter connection and
// the transport are the established transport that was lost, a failed proxy
// listener or any other provider failure is not.
func ebusLossDetail(reason string) drivermanager.FailureDetail {
	class := drivermanager.FailureOther
	switch reason {
	case ebusLossAdapterConnection, ebusLossTransportClosed:
		class = drivermanager.FailureTransportClosed
	}
	return drivermanager.FailureDetail{Class: class, Cause: "connection lost (" + reason + ")"}
}
