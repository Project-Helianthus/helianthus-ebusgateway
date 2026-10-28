package energycalendar

import (
	"errors"
	"testing"
	"time"
)

func o16Instant(t *testing.T, value string) time.Time {
	t.Helper()
	instant, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return instant
}

func o16Context(t *testing.T, name string, clock func() time.Time) *Context {
	t.Helper()
	context, err := New(&name, nil, clock)
	if err != nil {
		t.Fatalf("calendar construction (zone rules required): %v", err)
	}
	return context
}

func TestO16SelectedCalendarReferences(t *testing.T) {
	originalLocal := time.Local
	time.Local = time.UTC
	t.Cleanup(func() { time.Local = originalLocal })
	// These literal civil expectations do not call the production derivation.
	for _, tc := range []struct {
		name, instant string
		date          Date
		previous      Month
	}{
		{"midnight", "2028-06-14T22:30:00Z", Date{2028, 6, 15}, Month{2028, 5}},
		{"month", "2028-04-30T22:30:00Z", Date{2028, 5, 1}, Month{2028, 4}},
		{"year", "2027-12-31T23:30:00Z", Date{2028, 1, 1}, Month{2027, 12}},
		{"leap month", "2028-03-30T23:30:00Z", Date{2028, 3, 31}, Month{2028, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instant := o16Instant(t, tc.instant)
			calls := 0
			context := o16Context(t, "Europe/Berlin", func() time.Time { calls++; return instant })
			ref := context.Capture()
			if calls != 1 || ref.Date() != tc.date || ref.Month() != (Month{tc.date.Year, tc.date.Month}) ||
				ref.PreviousMonth() != tc.previous || ref.Year() != tc.date.Year || ref.PreviousYear() != tc.date.Year-1 ||
				ref.Generation() != context.Generation() || ref.Instant() != instant {
				t.Fatalf("inconsistent reference: %+v; calls=%d", ref, calls)
			}
		})
	}
}

func TestO16AcquisitionDateFields(t *testing.T) {
	start := o16Instant(t, "2028-06-15T10:00:00Z")
	// Vary each civil-date identity field independently, with other fields valid
	// and unchanged; exercise both chronological directions for each field.
	for _, tc := range []struct{ field, changed string }{
		{"day forward", "2028-06-16T10:00:00Z"},
		{"day rollback", "2028-06-14T10:00:00Z"},
		{"month forward", "2028-07-15T10:00:00Z"},
		{"month rollback", "2028-05-15T10:00:00Z"},
		{"year forward", "2029-06-15T10:00:00Z"},
		{"year rollback", "2027-06-15T10:00:00Z"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			clock := start
			context := o16Context(t, "Europe/Berlin", func() time.Time { return clock })
			acquisition := context.BeginAcquisition()
			captured := acquisition.Reference()
			if err := acquisition.Check(context); err != nil {
				t.Fatalf("valid same-date control: %v", err)
			}
			clock = o16Instant(t, tc.changed)
			if acquisition.Check(context) == nil {
				t.Fatal("changed civil date admitted")
			}
			clock = start
			if acquisition.Check(context) == nil {
				t.Fatal("aborted acquisition resumed")
			}
			if acquisition.Reference() != captured {
				t.Fatal("guard changed original reference")
			}
			if err := context.BeginAcquisition().Check(context); err != nil {
				t.Fatalf("new acquisition should use its own reference: %v", err)
			}
		})
	}
}

func TestO16LocalBoundaryGuard(t *testing.T) {
	for _, tc := range []struct{ name, start, end string }{
		{"same month midnight", "2028-06-14T21:59:59Z", "2028-06-14T22:00:01Z"},
		{"month midnight", "2028-04-30T21:59:59Z", "2028-04-30T22:00:01Z"},
		{"year midnight", "2027-12-31T22:59:59Z", "2027-12-31T23:00:01Z"},
		{"rollback midnight", "2027-12-31T23:05:00Z", "2027-12-31T22:55:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := o16Instant(t, tc.start)
			context := o16Context(t, "Europe/Berlin", func() time.Time { return clock })
			acquisition := context.BeginAcquisition()
			if err := acquisition.Check(context); err != nil {
				t.Fatal(err)
			}
			clock = o16Instant(t, tc.end)
			if acquisition.Check(context) == nil {
				t.Fatal("local boundary admitted while UTC date stayed unchanged")
			}
		})
	}
}

func TestO16SameDateControls(t *testing.T) {
	for _, tc := range []struct {
		name, start, end string
		date             Date
	}{
		{"DST forward", "2028-03-26T00:30:00Z", "2028-03-26T01:30:00Z", Date{2028, 3, 26}},
		{"DST back", "2028-10-29T00:30:00Z", "2028-10-29T01:30:00Z", Date{2028, 10, 29}},
		{"same day wall rollback", "2028-06-15T10:00:00Z", "2028-06-15T09:00:00Z", Date{2028, 6, 15}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := o16Instant(t, tc.start)
			context := o16Context(t, "Europe/Berlin", func() time.Time { return clock })
			acquisition := context.BeginAcquisition()
			clock = o16Instant(t, tc.end)
			if err := acquisition.Check(context); err != nil {
				t.Fatalf("same date should survive offset/time changes: %v", err)
			}
			if acquisition.Reference().Date() != tc.date || context.Capture().Date() != tc.date {
				t.Fatal("same-date control changed civil identity")
			}
		})
	}
}

func TestO16InvalidLocations(t *testing.T) {
	clock := func() time.Time { return time.Time{} }
	for _, name := range []string{"", "Local", "CET", "EST", "+02:00", "UTC+02:00", "Etc/GMT-2", " Europe/Berlin", "Unknown/O16"} {
		t.Run(name, func(t *testing.T) {
			defaultCalls := 0
			context, err := New(&name, func() (string, error) { defaultCalls++; return "UTC", nil }, clock)
			if err == nil || context != nil || defaultCalls != 0 {
				t.Fatalf("invalid explicit location fell back: context=%v err=%v defaults=%d", context, err, defaultCalls)
			}
		})
	}
	for _, tc := range []struct {
		name     string
		resolver func() (string, error)
	}{
		{"absent resolver", nil},
		{"unresolvable default", func() (string, error) { return "", errors.New("unresolvable local zone") }},
		{"fixed default", func() (string, error) { return "+02:00", nil }},
		{"empty default", func() (string, error) { return "", nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if context, err := New(nil, tc.resolver, clock); err == nil || context != nil {
				t.Fatal("invalid default admitted")
			}
		})
	}
	name := "UTC"
	if context, err := New(&name, nil, nil); err == nil || context != nil {
		t.Fatal("missing clock admitted")
	}
	instant := o16Instant(t, "2028-06-14T22:30:00Z")
	context := o16Context(t, name, func() time.Time { return instant })
	if context.Capture().Date() != (Date{2028, 6, 14}) {
		t.Fatal("explicit UTC not honored")
	}
}

func TestO16DefaultResolvedOnceAndImmutable(t *testing.T) {
	instant := o16Instant(t, "2027-12-31T23:30:00Z")
	name := "Europe/Berlin"
	calls := 0
	context, err := New(nil, func() (string, error) { calls++; return name, nil }, func() time.Time { return instant })
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := New(&name, nil, func() time.Time { return instant })
	if err != nil {
		t.Fatal(err)
	}
	generation := context.Generation()
	originalLocal := time.Local
	t.Cleanup(func() { time.Local = originalLocal })
	t.Setenv("TZ", "UTC")
	time.Local = time.UTC
	name = "UTC"
	for _, accepted := range []*Context{context, explicit} {
		if accepted.LocationName() != "Europe/Berlin" || accepted.Capture().Date() != (Date{2028, 1, 1}) {
			t.Fatal("accepted calendar followed changed process settings or caller string")
		}
	}
	if calls != 1 || context.Generation() != generation {
		t.Fatal("default re-resolved or generation changed")
	}
}

func TestO16GenerationGuard(t *testing.T) {
	instant := o16Instant(t, "2028-06-15T10:00:00Z")
	clock := func() time.Time { return instant }
	context := o16Context(t, "Europe/Berlin", clock)
	// Only generation changes: identical location, rules, clock, date, instant.
	replacement := o16Context(t, "Europe/Berlin", clock)
	if context.Generation() == replacement.Generation() {
		t.Fatal("independent startup contexts share a generation")
	}
	acquisition := context.BeginAcquisition()
	if err := acquisition.Check(context); err != nil {
		t.Fatal(err)
	}
	if acquisition.Check(replacement) == nil || acquisition.Check(context) == nil {
		t.Fatal("old generation admitted or abort was not permanent")
	}
	if err := replacement.BeginAcquisition().Check(replacement); err != nil {
		t.Fatal(err)
	}
	var zero Acquisition
	var missing *Acquisition
	if zero.Check(context) == nil || missing.Check(context) == nil || context.BeginAcquisition().Check(nil) == nil {
		t.Fatal("invalid guard/context admitted")
	}
}

func TestO16OriginalEvidenceIndependent(t *testing.T) {
	// An evidence instant supplied by the caller is retained exactly when used
	// as a reference. Elapsed-age policy remains outside this package.
	evidence := o16Instant(t, "2028-04-30T21:50:00Z")
	clock := evidence
	context := o16Context(t, "Europe/Berlin", func() time.Time { return clock })
	ref := context.Capture()
	clock = o16Instant(t, "2028-04-30T21:59:59Z")
	acquisition := context.BeginAcquisition()
	clock = o16Instant(t, "2028-04-30T22:00:01Z")
	current := context.Capture()
	if acquisition.Check(context) == nil {
		t.Fatal("missing boundary abort")
	}
	if ref.Instant() != evidence || ref.Date() != (Date{2028, 4, 30}) ||
		current.Date() != (Date{2028, 5, 1}) || clock.Sub(ref.Instant()) != 10*time.Minute+time.Second {
		t.Fatal("calendar operation changed original reference instant or civil date")
	}
}
