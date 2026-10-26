package runtimecontrol

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// RecoveryView is the recovery schedule of one driver as the MCP runtime
// tools and the Portal runtime view publish it: the one-based number of the
// attempt scheduled for NextAttemptAt, when it is due (RFC 3339 UTC with the Z
// designator), the closed class of the failure that scheduled it and that
// failure's display text, bounded to MaxFailureCauseChars and free of addresses.
type RecoveryView struct {
	Attempt       int
	NextAttemptAt string
	FailureClass  drivermanager.FailureClass
	FailureCause  string
}

// Recovery reports the entry's recovery view, and whether it has one. A view
// exists only while the entry is in BACKOFF and the executor reported a
// schedule (an attempt number from one and a due time); every other state has
// none, whatever its reason code. A schedule whose failure the executor did not
// describe is published with the class OTHER and an empty cause.
func (s DriverStatus) Recovery() (RecoveryView, bool) {
	if s.ObservedState != drivermanager.ObservedBackoff || s.NextAttempt < 1 || s.NextAttemptUTC.IsZero() {
		return RecoveryView{}, false
	}
	due := s.NextAttemptUTC.UTC()
	if year := due.Year(); year < 1 || year > 9999 {
		return RecoveryView{}, false
	}
	class := s.LastFailureClass
	if !class.Valid() {
		class = drivermanager.FailureOther
	}
	return RecoveryView{
		Attempt:       s.NextAttempt,
		NextAttemptAt: due.Format(time.RFC3339Nano),
		FailureClass:  class,
		FailureCause:  sanitizeFailureCause(s.LastFailureCause),
	}, true
}
