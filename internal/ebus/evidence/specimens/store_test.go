package specimens

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func specimenInput() Input {
	return Input{Family: "synthetic-a", Source: 1, Target: 2, FrameType: "synthetic", RequestData: []byte{3}, ResponseData: []byte{4}, RequestLen: 7, ResponseLen: 3, Outcome: "Unknown", HasResponse: true, ObservedAt: time.Unix(1, 0).UTC()}
}

func TestSpecimenIdentityAxes(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Input)
	}{
		{"family", func(i *Input) { i.Family = "synthetic-b" }},
		{"source", func(i *Input) { i.Source++ }},
		{"target", func(i *Input) { i.Target++ }},
		{"frame_type", func(i *Input) { i.FrameType = "other" }},
		{"request_bytes", func(i *Input) { i.RequestData = []byte{5} }},
		{"outcome", func(i *Input) { i.Outcome = "other" }},
		{"response_presence", func(i *Input) { i.HasResponse = false }},
		{"response_bytes", func(i *Input) { i.ResponseData = []byte{6} }},
	}
	key := func(i Input) string {
		return dedupKey(i.Family, i.Source, i.Target, i.FrameType, i.RequestData, i.Outcome, i.HasResponse, i.ResponseData)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, changed := specimenInput(), specimenInput()
			tc.change(&changed)
			if key(first) == key(changed) {
				t.Fatal("single-axis change collapsed dedup key")
			}
			var store Store
			store.Insert(first)
			store.Insert(changed)
			_, count := store.Counts()
			if count != 2 {
				t.Fatalf("unique entries = %d, want 2", count)
			}
			for _, entry := range store.Snapshot("") {
				if entry.Count != 1 {
					t.Fatalf("unexpected duplicate: %+v", entry)
				}
			}
		})
	}
}

func TestSpecimenDuplicate(t *testing.T) {
	var store Store
	first := specimenInput()
	store.Insert(first)
	duplicate := specimenInput()
	duplicate.ObservedAt = first.ObservedAt.Add(time.Second)
	store.Insert(duplicate)
	got := store.Snapshot(first.Family)
	if len(got) != 1 || got[0].Count != 2 || !got[0].FirstSeenAt.Equal(first.ObservedAt) || !got[0].LastSeenAt.Equal(duplicate.ObservedAt) {
		t.Fatalf("duplicate metadata = %+v", got)
	}
	// Empty response data still distinguishes presence from absence.
	present, absent := specimenInput(), specimenInput()
	present.ResponseData, absent.ResponseData = nil, nil
	absent.HasResponse = false
	var empty Store
	empty.Insert(present)
	empty.Insert(absent)
	if _, n := empty.Counts(); n != 2 {
		t.Fatalf("empty-response presence collapsed: %d", n)
	}
	// Absent response bytes are not part of the identity or retained evidence.
	absent.ResponseData = []byte{9}
	empty.Insert(absent)
	got = empty.Snapshot(absent.Family)
	if len(got) != 2 || got[1].Count != 2 || got[1].ResponseLen != 0 || len(got[1].ResponseData) != 0 {
		t.Fatalf("absent response = %+v", got)
	}
}

func TestSpecimenCapacityAndOverwriteOrder(t *testing.T) {
	var store Store
	for i := 0; i < 32; i++ {
		in := specimenInput()
		in.RequestData = []byte{byte(i)}
		store.Insert(in)
	}
	duplicate := specimenInput()
	duplicate.RequestData = []byte{0}
	duplicate.ObservedAt = duplicate.ObservedAt.Add(time.Second)
	store.Insert(duplicate)
	next := specimenInput()
	next.RequestData = []byte{32}
	store.Insert(next)
	got := store.Snapshot(next.Family)
	if len(got) != 32 {
		t.Fatalf("retained = %d, want 32", len(got))
	}
	for i, entry := range got {
		if !reflect.DeepEqual(entry.RequestData, []byte{byte(i + 1)}) {
			t.Fatalf("slot %d = %v, want %d", i, entry.RequestData, i+1)
		}
	}
	for i := 1; i < 64; i++ {
		in := specimenInput()
		in.Family = fmt.Sprintf("synthetic-%d", i)
		store.Insert(in)
	}
	rejected := specimenInput()
	rejected.Family = "synthetic-rejected"
	store.Insert(rejected)
	if families, total := store.Counts(); families != 64 || total != 95 {
		t.Fatalf("counts = %d/%d, want 64/95", families, total)
	}
	if len(store.Snapshot(rejected.Family)) != 0 {
		t.Fatal("65th family admitted")
	}
	store.Insert(next)
	if entry := store.Snapshot(next.Family)[31]; entry.Count != 2 {
		t.Fatalf("existing family blocked at cap: %+v", entry)
	}
}

func TestSpecimenDetachedBytes(t *testing.T) {
	var store Store
	input := specimenInput()
	store.Insert(input)
	input.RequestData[0] = 8
	input.ResponseData[0] = 9
	got := store.Snapshot(input.Family)
	if len(got) != 1 || !reflect.DeepEqual(got[0].RequestData, []byte{3}) || !reflect.DeepEqual(got[0].ResponseData, []byte{4}) {
		t.Fatalf("input alias: %+v", got)
	}
	got[0].RequestData[0] = 10
	got[0].ResponseData[0] = 11
	got[0].Family = "changed"
	again := store.Snapshot(input.Family)
	if len(again) != 1 || again[0].Family != input.Family || !reflect.DeepEqual(again[0].RequestData, []byte{3}) || !reflect.DeepEqual(again[0].ResponseData, []byte{4}) {
		t.Fatalf("snapshot alias: %+v", again)
	}
}
