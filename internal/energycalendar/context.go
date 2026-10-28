// Package energycalendar provides the private startup calendar dependency.
// It does not own energy admission, evidence timestamps, or freshness.
package energycalendar

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Generation is an opaque process-local identity, not a persistent identifier.
// A fresh context has a fresh identity even when its location name is unchanged.
type Generation struct{ token *byte }

// Context resolves its location once. Its fields cannot be replaced by callers.
// The injected clock must be safe for the caller's concurrency requirements.
type Context struct {
	location   *time.Location
	clock      func() time.Time
	generation Generation
}

// New constructs a startup context. A nil explicitLocation means omitted
// configuration: resolveLocal must return the process's resolvable IANA name.
// An empty explicit value is an error, never a request to use the default.
func New(explicitLocation *string, resolveLocal func() (string, error), clock func() time.Time) (*Context, error) {
	if clock == nil {
		return nil, errors.New("energy calendar requires a clock")
	}
	var name string
	if explicitLocation != nil {
		name = *explicitLocation
	} else {
		if resolveLocal == nil {
			return nil, errors.New("energy calendar requires a resolvable local IANA location")
		}
		var err error
		name, err = resolveLocal()
		if err != nil {
			return nil, fmt.Errorf("resolve energy calendar default: %w", err)
		}
	}
	// UTC is an explicit IANA choice. Local, abbreviations, POSIX expressions
	// and the Etc/GMT offset family are not resolvable calendar policies.
	if name == "" || strings.TrimSpace(name) != name ||
		(name != "UTC" && !strings.Contains(name, "/")) ||
		strings.HasPrefix(name, "Etc/GMT") {
		return nil, errors.New("energy calendar requires an IANA location, not an offset or abbreviation")
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("load energy calendar location: %w", err)
	}
	return &Context{location: location, clock: clock, generation: Generation{new(byte)}}, nil
}

// LocationName reports only the accepted startup location, never process state.
func (c *Context) LocationName() string { return c.location.String() }

// Generation binds derived references and caches to this startup context.
func (c *Context) Generation() Generation { return c.generation }

// Date is a civil date, not an instant or an evidence identity.
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

// Month is an absolute civil month without a day component.
type Month struct {
	Year  int
	Month time.Month
}

// Reference is an immutable value captured from one clock read. The original
// time.Time is retained separately from civil arithmetic, including any
// monotonic component supplied by the clock.
type Reference struct {
	instant    time.Time
	date       Date
	generation Generation
}

func (r Reference) Instant() time.Time     { return r.instant }
func (r Reference) Date() Date             { return r.date }
func (r Reference) Generation() Generation { return r.generation }
func (r Reference) Year() int              { return r.date.Year }
func (r Reference) PreviousYear() int      { return r.date.Year - 1 }
func (r Reference) Month() Month           { return Month{r.date.Year, r.date.Month} }
func (r Reference) PreviousMonth() Month {
	if r.date.Month == time.January {
		return Month{r.date.Year - 1, time.December}
	}
	return Month{r.date.Year, r.date.Month - 1}
}

// Capture derives every civil component from the same selected location and
// clock sample. It never reads or renews an energy observation.
func (c *Context) Capture() Reference {
	instant := c.clock()
	year, month, day := instant.In(c.location).Date()
	return Reference{instant, Date{year, month, day}, c.generation}
}

// Acquisition is a single-owner guard. Once invalidated, it stays aborted even
// if the clock returns to the captured date. It does not roll back accepted data.
type Acquisition struct {
	reference Reference
	aborted   bool
}

func (c *Context) BeginAcquisition() *Acquisition {
	return &Acquisition{reference: c.Capture()}
}

func (a *Acquisition) Reference() Reference { return a.reference }

// Check must run before accepting each reply, including after a failed read.
// A zero guard, a different context, or any civil-date change fails closed.
func (a *Acquisition) Check(c *Context) error {
	if a == nil {
		return errors.New("missing energy acquisition")
	}
	if a.aborted {
		return errors.New("energy acquisition already aborted")
	}
	if c == nil || c.location == nil || c.clock == nil || a.reference.generation.token == nil ||
		c.generation != a.reference.generation || c.Capture().date != a.reference.date {
		a.aborted = true
		return errors.New("energy acquisition calendar changed")
	}
	return nil
}
