package specimens

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

const (
	MaxPerFamily = 32
	MaxFamilies  = 64
)

// Input carries native evidence already admitted and classified by the caller.
type Input struct {
	Family                    string
	Source, Target            byte
	FrameType                 string
	RequestData, ResponseData []byte
	RequestLen, ResponseLen   int
	Outcome                   string
	HasResponse               bool
	ObservedAt                time.Time
}

// Store retains bounded native evidence independently of shell observability.
// Its zero value is ready for use.
type Store struct {
	mu       sync.RWMutex
	families map[string]*familyBucket
}

type Entry struct {
	Family       string
	Source       byte
	Target       byte
	FrameType    string
	RequestData  []byte
	ResponseData []byte
	RequestLen   int
	ResponseLen  int
	Outcome      string
	FirstSeenAt  time.Time
	LastSeenAt   time.Time
	Count        uint64
	dedupKey     string
}

type familyBucket struct {
	entries [MaxPerFamily]Entry
	start   int
	length  int
}

func dedupKey(family string, source, target byte, frameType string, requestData []byte, outcome string, hasResponse bool, responseData []byte) string {
	key := family + fmt.Sprintf("/%02x/%02x/", source, target) + frameType + "/" + hex.EncodeToString(requestData) + "/" + outcome
	if hasResponse {
		key += "/" + hex.EncodeToString(responseData)
	}
	return key
}

func (store *Store) Insert(input Input) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.families == nil {
		store.families = make(map[string]*familyBucket)
	}
	family, frameType, outcome, now := input.Family, input.FrameType, input.Outcome, input.ObservedAt
	bucket, exists := store.families[family]
	if !exists {
		if len(store.families) >= MaxFamilies {
			return
		}
		bucket = &familyBucket{}
		store.families[family] = bucket
	}

	key := dedupKey(family, input.Source, input.Target, frameType, input.RequestData, outcome, input.HasResponse, input.ResponseData)

	// Search for existing entry with same dedup key.
	for i := 0; i < bucket.length; i++ {
		idx := (bucket.start + i) % MaxPerFamily
		if bucket.entries[idx].dedupKey == key {
			bucket.entries[idx].Count++
			bucket.entries[idx].LastSeenAt = now
			return
		}
	}

	// Build new entry.
	entry := Entry{
		Family:      family,
		Source:      input.Source,
		Target:      input.Target,
		FrameType:   frameType,
		RequestData: append([]byte(nil), input.RequestData...),
		RequestLen:  input.RequestLen,
		Outcome:     outcome,
		FirstSeenAt: now,
		LastSeenAt:  now,
		Count:       1,
		dedupKey:    key,
	}
	if input.HasResponse {
		entry.ResponseData = append([]byte(nil), input.ResponseData...)
		entry.ResponseLen = input.ResponseLen
	}

	// Ring buffer push.
	if bucket.length < MaxPerFamily {
		idx := (bucket.start + bucket.length) % MaxPerFamily
		bucket.entries[idx] = entry
		bucket.length++
	} else {
		bucket.entries[bucket.start] = entry
		bucket.start = (bucket.start + 1) % MaxPerFamily
	}
}

// Counts returns retained families and unique specimens, not observation totals.
func (store *Store) Counts() (families, total int) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	for _, bucket := range store.families {
		total += bucket.length
	}
	return len(store.families), total
}

// Snapshot returns detached entries in per-family insertion order.
// Ordering across families is unspecified; public projections own their sorting.
func (store *Store) Snapshot(family string) []Entry {
	store.mu.RLock()
	defer store.mu.RUnlock()
	var items []Entry
	for fam, bucket := range store.families {
		if family != "" && fam != family {
			continue
		}
		for i := 0; i < bucket.length; i++ {
			entry := bucket.entries[(bucket.start+i)%MaxPerFamily]
			entry.RequestData = append([]byte(nil), entry.RequestData...)
			entry.ResponseData = append([]byte(nil), entry.ResponseData...)
			items = append(items, entry)
		}
	}
	return items
}
