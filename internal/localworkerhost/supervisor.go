// Package localworkerhost supervises the same-executable eBUS local worker from
// the gateway parent and validates the child's startup plan.
//
// The host interfaces here are frozen ports: launch, descriptor allocation and
// time are injected. This package opens no device, owns no transport or mux
// implementation and has no arming path; a child it starts can only report
// passive, TX-inactive readiness and leases. A missing backend leaves the local
// worker unavailable.
package localworkerhost

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ChildFlag selects the worker branch. It is valid only as argv[1].
	ChildFlag = "--internal-ebus-local-worker"
	// ABIVersion is the host/worker ABI this build speaks.
	ABIVersion uint32 = 1
	// FirstDescriptor is the child descriptor number of the first inherited
	// resource; later resources follow in layout order.
	FirstDescriptor = 3
	// ReportBytes is the fixed size of one child report on the control
	// descriptor; it is also the declared size of the control role.
	ReportBytes = 152
	// MaxMappingBytes bounds the fixed shared mapping.
	MaxMappingBytes = 1 << 24
	// MaxPending bounds commands held for the current generation.
	MaxPending = 64

	reportMagic = "LWHR"
)

var (
	ErrConfig        = errors.New("localworkerhost: invalid supervisor configuration")
	ErrRecursion     = errors.New("localworkerhost: a worker child cannot supervise a worker")
	ErrDuplicate     = errors.New("localworkerhost: a worker child is already live in this process")
	ErrRunning       = errors.New("localworkerhost: the worker is already started")
	ErrUnreaped      = errors.New("localworkerhost: the worker child was not reaped within its stop budget")
	ErrArgv          = errors.New("localworkerhost: malformed worker argv")
	ErrBuild         = errors.New("localworkerhost: build or executable identity mismatch")
	ErrABI           = errors.New("localworkerhost: ABI version mismatch")
	ErrDescriptor    = errors.New("localworkerhost: descriptor role, number or size mismatch")
	ErrRuntimePolicy = errors.New("localworkerhost: child runtime policy mismatch")
	ErrGeneration    = errors.New("localworkerhost: generation mismatch")
	ErrStale         = errors.New("localworkerhost: stale generation dropped")
	ErrExhausted     = errors.New("localworkerhost: generation counter exhausted")
	ErrReport        = errors.New("localworkerhost: malformed or unexpected report")
	ErrNotPassive    = errors.New("localworkerhost: worker is not passive with TX inactive")
	ErrLease         = errors.New("localworkerhost: lease sequence out of order")
	ErrLeaseExpired  = errors.New("localworkerhost: lease expired")
	ErrReadyTimeout  = errors.New("localworkerhost: readiness was not acknowledged in time")
	ErrNotReady      = errors.New("localworkerhost: worker is not ready")
	ErrPlan          = errors.New("localworkerhost: plan identity is invalid or duplicated")
	ErrFull          = errors.New("localworkerhost: pending command bound reached")
	ErrExited        = errors.New("localworkerhost: worker child exited")
	ErrStopped       = errors.New("localworkerhost: worker stopped by the supervisor")
)

// Digest is a SHA-256 identity digest.
type Digest [sha256.Size]byte

// DigestOf digests one identity description.
func DigestOf(data []byte) Digest { return Digest(sha256.Sum256(data)) }

func (d Digest) zero() bool { return d == Digest{} }

// Identity binds a worker to the exact parent build, executable image and ABI.
type Identity struct {
	Build      Digest
	Executable Digest
	ABI        uint32
}

func (id Identity) valid() bool {
	return !id.Build.zero() && !id.Executable.zero() && id.ABI == ABIVersion
}

// Role is the fixed role of one inherited descriptor.
type Role uint8

const (
	RoleControl Role = 1
	RoleMapping Role = 2
)

// Descriptor declares one inherited resource and its fixed size.
type Descriptor struct {
	Role Role
	Size uint64
}

// validateLayout accepts exactly the fixed control-then-mapping layout.
func validateLayout(layout []Descriptor) error {
	if len(layout) != 2 {
		return ErrDescriptor
	}
	control, mapping := layout[0], layout[1]
	if control.Role != RoleControl || control.Size != ReportBytes {
		return ErrDescriptor
	}
	if mapping.Role != RoleMapping || mapping.Size == 0 || mapping.Size%8 != 0 || mapping.Size > MaxMappingBytes {
		return ErrDescriptor
	}
	return nil
}

func layoutEqual(left, right []Descriptor) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// Generation is the identity fence of one worker incarnation. Every start
// advances every field, so nothing published for a predecessor is admitted.
type Generation struct {
	Worker       uint64
	Stream       uint64
	Registration uint64
	Token        uint64
}

func (g Generation) valid() bool {
	return g.Worker != 0 && g.Stream != 0 && g.Registration != 0 && g.Token != 0
}

func (g Generation) next() (Generation, error) {
	if g.Worker == math.MaxUint64 || g.Stream == math.MaxUint64 || g.Registration == math.MaxUint64 || g.Token == math.MaxUint64 {
		return Generation{}, ErrExhausted
	}
	return Generation{Worker: g.Worker + 1, Stream: g.Stream + 1, Registration: g.Registration + 1, Token: g.Token + 1}, nil
}

// RuntimePolicy is the child's own Go runtime environment. It is set
// explicitly and never copied from the gateway.
type RuntimePolicy struct {
	GC          string
	MemoryLimit string
}

func (p RuntimePolicy) valid() bool {
	return p.GC != "" && p.MemoryLimit != "" && !strings.ContainsAny(p.GC+p.MemoryLimit, "= \t\n")
}

// childRuntimeKeys are parent runtime settings the child never inherits.
var childRuntimeKeys = []string{"GOGC", "GOMEMLIMIT", "GODEBUG", "GOMAXPROCS"}

// ChildEnv derives the child environment: parent runtime settings are dropped
// and the explicit child policy is appended.
func ChildEnv(base []string, policy RuntimePolicy) []string {
	env := make([]string, 0, len(base)+2)
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		inherited := true
		for _, runtimeKey := range childRuntimeKeys {
			if key == runtimeKey {
				inherited = false
				break
			}
		}
		if inherited {
			env = append(env, entry)
		}
	}
	return append(env, "GOGC="+policy.GC, "GOMEMLIMIT="+policy.MemoryLimit)
}

// ChildPlan is everything the parent hands the child through its argv.
type ChildPlan struct {
	Identity      Identity
	Generation    Generation
	Descriptors   []Descriptor
	Runtime       RuntimePolicy
	LeaseInterval time.Duration
}

var childArgvKeys = []string{"build", "executable", "abi", "worker", "stream", "registration", "token", "control", "mapping", "gc", "memlimit", "lease-ms"}

// Argv renders the explicit argument vector: the executable, ChildFlag and the
// fixed key=value plan fields in fixed order. No gateway flag is carried.
func (p ChildPlan) Argv(executable string) []string {
	values := p.argvValues()
	argv := make([]string, 0, len(values)+2)
	argv = append(argv, executable, ChildFlag)
	for i, key := range childArgvKeys {
		argv = append(argv, key+"="+values[i])
	}
	return argv
}

func (p ChildPlan) argvValues() []string {
	var control, mapping uint64
	if len(p.Descriptors) == 2 {
		control, mapping = p.Descriptors[0].Size, p.Descriptors[1].Size
	}
	return []string{
		hex.EncodeToString(p.Identity.Build[:]),
		hex.EncodeToString(p.Identity.Executable[:]),
		strconv.FormatUint(uint64(p.Identity.ABI), 10),
		strconv.FormatUint(p.Generation.Worker, 10),
		strconv.FormatUint(p.Generation.Stream, 10),
		strconv.FormatUint(p.Generation.Registration, 10),
		strconv.FormatUint(p.Generation.Token, 10),
		strconv.FormatUint(control, 10),
		strconv.FormatUint(mapping, 10),
		p.Runtime.GC,
		p.Runtime.MemoryLimit,
		strconv.FormatInt(p.LeaseInterval.Milliseconds(), 10),
	}
}

// SelectsChild reports whether argv selects the worker branch.
func SelectsChild(argv []string) bool { return len(argv) > 1 && argv[1] == ChildFlag }

// ContainsChildFlag reports whether ChildFlag appears anywhere in argv.
func ContainsChildFlag(argv []string) bool {
	for _, arg := range argv {
		if arg == ChildFlag {
			return true
		}
	}
	return false
}

// ParseChildArgv parses argv after the executable name. It accepts exactly
// ChildFlag followed by the fixed plan fields in order, and nothing else.
func ParseChildArgv(args []string) (ChildPlan, error) {
	if len(args) != len(childArgvKeys)+1 || args[0] != ChildFlag {
		return ChildPlan{}, ErrArgv
	}
	values := make([]string, len(childArgvKeys))
	for i, key := range childArgvKeys {
		value, ok := strings.CutPrefix(args[i+1], key+"=")
		if !ok || value == "" {
			return ChildPlan{}, fmt.Errorf("%w: field %s", ErrArgv, key)
		}
		values[i] = value
	}
	var plan ChildPlan
	var err error
	if plan.Identity.Build, err = parseDigest(values[0]); err != nil {
		return ChildPlan{}, err
	}
	if plan.Identity.Executable, err = parseDigest(values[1]); err != nil {
		return ChildPlan{}, err
	}
	numbers := make([]uint64, 7)
	for i := range numbers {
		if numbers[i], err = strconv.ParseUint(values[i+2], 10, 64); err != nil {
			return ChildPlan{}, fmt.Errorf("%w: field %s", ErrArgv, childArgvKeys[i+2])
		}
	}
	if numbers[0] > math.MaxUint32 {
		return ChildPlan{}, ErrArgv
	}
	plan.Identity.ABI = uint32(numbers[0])
	plan.Generation = Generation{Worker: numbers[1], Stream: numbers[2], Registration: numbers[3], Token: numbers[4]}
	plan.Descriptors = []Descriptor{{Role: RoleControl, Size: numbers[5]}, {Role: RoleMapping, Size: numbers[6]}}
	plan.Runtime = RuntimePolicy{GC: values[9], MemoryLimit: values[10]}
	lease, err := strconv.ParseInt(values[11], 10, 64)
	if err != nil || lease <= 0 || lease > int64(time.Hour/time.Millisecond) {
		return ChildPlan{}, fmt.Errorf("%w: field lease-ms", ErrArgv)
	}
	plan.LeaseInterval = time.Duration(lease) * time.Millisecond
	return plan, nil
}

func parseDigest(value string) (Digest, error) {
	var digest Digest
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(digest) {
		return Digest{}, ErrArgv
	}
	copy(digest[:], raw)
	return digest, nil
}

// DescriptorKind is what the child observes behind an inherited descriptor.
type DescriptorKind uint8

const (
	KindUnknown DescriptorKind = iota
	KindStream
	KindMapping
)

// Observed is the child's own inspection of one inherited descriptor.
type Observed struct {
	FD   int
	Kind DescriptorKind
	Size uint64
}

// ValidateChild checks the parsed plan against the child's own identity, the
// inherited descriptors and its runtime environment, before any readiness.
func ValidateChild(plan ChildPlan, self Identity, observed []Observed, getenv func(string) string) error {
	if plan.Identity.ABI != ABIVersion || self.ABI != ABIVersion {
		return ErrABI
	}
	if !self.valid() || plan.Identity != self {
		return ErrBuild
	}
	if !plan.Generation.valid() {
		return ErrGeneration
	}
	if err := validateLayout(plan.Descriptors); err != nil {
		return err
	}
	if len(observed) != len(plan.Descriptors) {
		return ErrDescriptor
	}
	for i, want := range plan.Descriptors {
		got := observed[i]
		if got.FD != FirstDescriptor+i {
			return ErrDescriptor
		}
		switch want.Role {
		case RoleControl:
			if got.Kind != KindStream {
				return ErrDescriptor
			}
		case RoleMapping:
			if got.Kind != KindMapping || got.Size != want.Size {
				return ErrDescriptor
			}
		default:
			return ErrDescriptor
		}
	}
	if !plan.Runtime.valid() || getenv == nil || getenv("GOGC") != plan.Runtime.GC || getenv("GOMEMLIMIT") != plan.Runtime.MemoryLimit {
		return ErrRuntimePolicy
	}
	if plan.LeaseInterval <= 0 {
		return ErrArgv
	}
	return nil
}

// ReportKind is the kind of one child report.
type ReportKind uint8

const (
	ReportReady ReportKind = 1
	ReportLease ReportKind = 2
)

// WorkerState is the worker's self-reported wire state. This build has only
// the passive state; any other value fences the generation.
type WorkerState uint8

const WorkerPassive WorkerState = 1

// Report is one fixed-size child acknowledgement on the control descriptor.
type Report struct {
	Kind        ReportKind
	Identity    Identity
	Generation  Generation
	Descriptors []Descriptor
	State       WorkerState
	TXActive    bool
	Sequence    uint64
}

// EncodeReport serializes a report as pointer-free little-endian bytes.
func EncodeReport(report Report) ([ReportBytes]byte, error) {
	var out [ReportBytes]byte
	if len(report.Descriptors) > 2 {
		return out, ErrReport
	}
	copy(out[0:4], reportMagic)
	out[4] = byte(report.Kind)
	out[5] = byte(report.State)
	if report.TXActive {
		out[6] = 1
	}
	out[7] = byte(len(report.Descriptors))
	binary.LittleEndian.PutUint32(out[8:], report.Identity.ABI)
	binary.LittleEndian.PutUint64(out[16:], report.Generation.Worker)
	binary.LittleEndian.PutUint64(out[24:], report.Generation.Stream)
	binary.LittleEndian.PutUint64(out[32:], report.Generation.Registration)
	binary.LittleEndian.PutUint64(out[40:], report.Generation.Token)
	binary.LittleEndian.PutUint64(out[48:], report.Sequence)
	copy(out[56:88], report.Identity.Build[:])
	copy(out[88:120], report.Identity.Executable[:])
	for i, descriptor := range report.Descriptors {
		base := 120 + 16*i
		out[base] = byte(descriptor.Role)
		binary.LittleEndian.PutUint64(out[base+8:], descriptor.Size)
	}
	return out, nil
}

// DecodeReport parses one report and refuses unknown or non-zero reserved bytes.
func DecodeReport(raw []byte) (Report, error) {
	if len(raw) != ReportBytes || string(raw[0:4]) != reportMagic || raw[6] > 1 || raw[7] > 2 {
		return Report{}, ErrReport
	}
	for _, b := range raw[12:16] {
		if b != 0 {
			return Report{}, ErrReport
		}
	}
	report := Report{
		Kind:     ReportKind(raw[4]),
		State:    WorkerState(raw[5]),
		TXActive: raw[6] == 1,
		Identity: Identity{ABI: binary.LittleEndian.Uint32(raw[8:])},
		Generation: Generation{
			Worker:       binary.LittleEndian.Uint64(raw[16:]),
			Stream:       binary.LittleEndian.Uint64(raw[24:]),
			Registration: binary.LittleEndian.Uint64(raw[32:]),
			Token:        binary.LittleEndian.Uint64(raw[40:]),
		},
		Sequence: binary.LittleEndian.Uint64(raw[48:]),
	}
	copy(report.Identity.Build[:], raw[56:88])
	copy(report.Identity.Executable[:], raw[88:120])
	for i := 0; i < 2; i++ {
		base := 120 + 16*i
		for _, b := range raw[base+1 : base+8] {
			if b != 0 {
				return Report{}, ErrReport
			}
		}
		descriptor := Descriptor{Role: Role(raw[base]), Size: binary.LittleEndian.Uint64(raw[base+8:])}
		if i >= int(raw[7]) {
			if descriptor != (Descriptor{}) {
				return Report{}, ErrReport
			}
			continue
		}
		report.Descriptors = append(report.Descriptors, descriptor)
	}
	return report, nil
}

// Resource is one parent-owned inherited resource.
type Resource interface {
	Role() Role
	Size() uint64
	Close() error
}

// Allocator prepares the fixed descriptors for one generation.
type Allocator interface {
	Allocate(Descriptor) (Resource, error)
}

// LaunchSpec is one explicit same-executable launch: no shell, no inherited
// gateway argv and an explicit environment.
type LaunchSpec struct {
	Path       string
	Args       []string
	Env        []string
	Files      []Resource
	Generation Generation
}

// Process is one launched child.
type Process interface {
	Wait() error
	Stop() error
	Kill() error
}

// Launcher starts a child from a LaunchSpec.
type Launcher interface {
	Launch(LaunchSpec) (Process, error)
}

// Clock is the injected monotonic time source.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

// Config is the fixed supervisor configuration.
type Config struct {
	Self          Identity
	Executable    string
	ProcessArgs   []string
	BaseEnv       []string
	Descriptors   []Descriptor
	Runtime       RuntimePolicy
	LeaseInterval time.Duration
	LeaseTTL      time.Duration
	ReadyTimeout  time.Duration
	StopGrace     time.Duration
	KillGrace     time.Duration
	Allocator     Allocator
	Launcher      Launcher
	Clock         Clock
}

// State is the supervisor's lifecycle state.
type State string

const (
	StateIdle     State = "idle"
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateFenced   State = "fenced"
	StateStopping State = "stopping"
	StateStopped  State = "stopped"
	StateUnreaped State = "unreaped"
)

// PlanRef names one plan published for exactly one generation.
type PlanRef struct {
	Worker       uint64
	Stream       uint64
	Registration uint64
	Plan         uint64
}

// Command is one pending output request for exactly one generation.
type Command struct {
	Worker   uint64
	Stream   uint64
	Token    uint64
	Sequence uint64
}

// Status is a detached supervisor view.
type Status struct {
	State      State
	Generation Generation
	Reason     error
	Plans      int
	Pending    int
	Cancelled  uint64
	Launches   uint64
}

// liveChild admits at most one worker child per process.
var liveChild atomic.Bool

type child struct {
	gen   Generation
	proc  Process
	files []Resource
	done  chan struct{}
}

// Supervisor owns one worker child at a time.
type Supervisor struct {
	cfg Config

	mu          sync.Mutex
	state       State
	gen         Generation
	reason      error
	child       *child
	readyBy     time.Time
	leaseExpiry time.Time
	leaseSeq    uint64
	plans       map[PlanRef]struct{}
	pending     []Command
	cancelled   uint64
	launches    uint64
}

// New validates the configuration. A process that is itself a worker child
// cannot construct a supervisor.
func New(cfg Config) (*Supervisor, error) {
	if ContainsChildFlag(cfg.ProcessArgs) {
		return nil, ErrRecursion
	}
	if !cfg.Self.valid() || !filepath.IsAbs(cfg.Executable) || validateLayout(cfg.Descriptors) != nil || !cfg.Runtime.valid() {
		return nil, ErrConfig
	}
	if cfg.LeaseInterval <= 0 || cfg.LeaseTTL <= cfg.LeaseInterval || cfg.ReadyTimeout <= 0 || cfg.StopGrace <= 0 || cfg.KillGrace <= 0 {
		return nil, ErrConfig
	}
	if cfg.Allocator == nil || cfg.Launcher == nil || cfg.Clock == nil {
		return nil, ErrConfig
	}
	cfg.ProcessArgs = append([]string(nil), cfg.ProcessArgs...)
	cfg.BaseEnv = append([]string(nil), cfg.BaseEnv...)
	cfg.Descriptors = append([]Descriptor(nil), cfg.Descriptors...)
	return &Supervisor{cfg: cfg, state: StateIdle, plans: map[PlanRef]struct{}{}}, nil
}

// Start allocates fresh descriptors and launches a new generation. A failed
// launch still consumes its generation.
func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateIdle && s.state != StateStopped {
		if s.state == StateUnreaped {
			return ErrUnreaped
		}
		return ErrRunning
	}
	gen, err := s.gen.next()
	if err != nil {
		return err
	}
	if !liveChild.CompareAndSwap(false, true) {
		return ErrDuplicate
	}
	s.gen = gen
	files, err := s.allocate()
	if err != nil {
		liveChild.Store(false)
		return err
	}
	plan := ChildPlan{Identity: s.cfg.Self, Generation: gen, Descriptors: s.cfg.Descriptors, Runtime: s.cfg.Runtime, LeaseInterval: s.cfg.LeaseInterval}
	spec := LaunchSpec{
		Path:       s.cfg.Executable,
		Args:       plan.Argv(s.cfg.Executable),
		Env:        ChildEnv(s.cfg.BaseEnv, s.cfg.Runtime),
		Files:      append([]Resource(nil), files...),
		Generation: gen,
	}
	s.launches++
	proc, err := s.cfg.Launcher.Launch(spec)
	if err != nil {
		closeResources(files)
		liveChild.Store(false)
		s.state = StateStopped
		s.reason = err
		return err
	}
	c := &child{gen: gen, proc: proc, files: files, done: make(chan struct{})}
	s.child = c
	s.state = StateStarting
	s.reason = nil
	s.readyBy = s.cfg.Clock.Now().Add(s.cfg.ReadyTimeout)
	s.leaseExpiry = time.Time{}
	s.leaseSeq = 0
	go s.watch(c)
	return nil
}

func (s *Supervisor) allocate() ([]Resource, error) {
	files := make([]Resource, 0, len(s.cfg.Descriptors))
	for _, want := range s.cfg.Descriptors {
		resource, err := s.cfg.Allocator.Allocate(want)
		if err != nil {
			closeResources(files)
			return nil, err
		}
		if resource == nil || resource.Role() != want.Role || resource.Size() != want.Size {
			if resource != nil {
				_ = resource.Close()
			}
			closeResources(files)
			return nil, ErrDescriptor
		}
		files = append(files, resource)
	}
	return files, nil
}

func closeResources(files []Resource) {
	for _, file := range files {
		_ = file.Close()
	}
}

// watch reaps the child and finalizes its generation. Resources are released
// only after the child has exited, so no live worker can still reach them.
func (s *Supervisor) watch(c *child) {
	_ = c.proc.Wait()
	closeResources(c.files)
	s.mu.Lock()
	if s.child == c {
		if s.state != StateStopping && s.state != StateUnreaped {
			s.fenceLocked(ErrExited)
		}
		s.state = StateStopped
		s.child = nil
	}
	liveChild.Store(false)
	s.mu.Unlock()
	close(c.done)
}

// fenceLocked closes admission for the current generation and drops its plans
// and pending commands; they are never replayed into a successor.
func (s *Supervisor) fenceLocked(reason error) {
	if s.state == StateStarting || s.state == StateReady {
		s.state = StateFenced
	}
	if s.reason == nil {
		s.reason = reason
	}
	s.cancelled += uint64(len(s.pending))
	s.pending = nil
	s.plans = map[PlanRef]struct{}{}
}

// Observe applies one child report. Reports from a predecessor generation are
// dropped; any other mismatch fences the current generation.
func (s *Supervisor) Observe(report Report) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.child == nil || report.Generation.Worker < s.gen.Worker {
		return ErrStale
	}
	if s.state != StateStarting && s.state != StateReady {
		return ErrNotReady
	}
	if report.Generation != s.gen {
		s.fenceLocked(ErrGeneration)
		return ErrGeneration
	}
	if report.Identity.ABI != s.cfg.Self.ABI {
		s.fenceLocked(ErrABI)
		return ErrABI
	}
	if report.Identity != s.cfg.Self {
		s.fenceLocked(ErrBuild)
		return ErrBuild
	}
	if report.State != WorkerPassive || report.TXActive {
		s.fenceLocked(ErrNotPassive)
		return ErrNotPassive
	}
	now := s.cfg.Clock.Now()
	switch report.Kind {
	case ReportReady:
		if s.state != StateStarting || report.Sequence != 0 {
			s.fenceLocked(ErrReport)
			return ErrReport
		}
		if !layoutEqual(report.Descriptors, s.cfg.Descriptors) {
			s.fenceLocked(ErrDescriptor)
			return ErrDescriptor
		}
		if !now.Before(s.readyBy) {
			s.fenceLocked(ErrReadyTimeout)
			return ErrReadyTimeout
		}
		s.state = StateReady
		s.leaseExpiry = now.Add(s.cfg.LeaseTTL)
	case ReportLease:
		if s.state != StateReady || len(report.Descriptors) != 0 {
			s.fenceLocked(ErrReport)
			return ErrReport
		}
		if report.Sequence != s.leaseSeq+1 {
			s.fenceLocked(ErrLease)
			return ErrLease
		}
		if !now.Before(s.leaseExpiry) {
			s.fenceLocked(ErrLeaseExpired)
			return ErrLeaseExpired
		}
		s.leaseSeq = report.Sequence
		s.leaseExpiry = now.Add(s.cfg.LeaseTTL)
	default:
		s.fenceLocked(ErrReport)
		return ErrReport
	}
	return nil
}

// Tick fences a generation whose readiness or lease deadline has passed.
func (s *Supervisor) Tick() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Clock.Now()
	switch {
	case s.state == StateStarting && !now.Before(s.readyBy):
		s.fenceLocked(ErrReadyTimeout)
		return ErrReadyTimeout
	case s.state == StateReady && !now.Before(s.leaseExpiry):
		s.fenceLocked(ErrLeaseExpired)
		return ErrLeaseExpired
	}
	return nil
}

// PublishPlan admits a plan only for the exact current ready generation.
func (s *Supervisor) PublishPlan(ref PlanRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return ErrNotReady
	}
	if ref.Worker != s.gen.Worker || ref.Stream != s.gen.Stream || ref.Registration != s.gen.Registration {
		return ErrStale
	}
	if _, exists := s.plans[ref]; exists || ref.Plan == 0 {
		return ErrPlan
	}
	s.plans[ref] = struct{}{}
	return nil
}

// Plans lists the plans admitted for the current generation.
func (s *Supervisor) Plans() []PlanRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := make([]PlanRef, 0, len(s.plans))
	for ref := range s.plans {
		refs = append(refs, ref)
	}
	return refs
}

// Enqueue holds a command only for the exact current ready generation and
// owner token.
func (s *Supervisor) Enqueue(command Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return ErrNotReady
	}
	if command.Worker != s.gen.Worker || command.Stream != s.gen.Stream || command.Token != s.gen.Token {
		return ErrStale
	}
	if len(s.pending) >= MaxPending {
		return ErrFull
	}
	s.pending = append(s.pending, command)
	return nil
}

// Drain takes the pending commands of the current generation.
func (s *Supervisor) Drain() []Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := s.pending
	s.pending = nil
	return pending
}

// Status returns a detached view.
func (s *Supervisor) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Status{State: s.state, Generation: s.gen, Reason: s.reason, Plans: len(s.plans), Pending: len(s.pending), Cancelled: s.cancelled, Launches: s.launches}
}

// Stop fences the generation, asks the child to stop, escalates to a kill after
// the stop grace and reports ErrUnreaped if the child outlives the kill grace.
// An unreaped child keeps its resources and blocks any new start.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	c := s.child
	if c == nil {
		s.mu.Unlock()
		return nil
	}
	s.fenceLocked(ErrStopped)
	if s.state != StateUnreaped {
		s.state = StateStopping
	}
	s.mu.Unlock()

	select {
	case <-c.done:
		return nil
	default:
	}
	_ = c.proc.Stop()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
	case <-s.cfg.Clock.After(s.cfg.StopGrace):
	}
	_ = c.proc.Kill()
	select {
	case <-c.done:
		return nil
	case <-s.cfg.Clock.After(s.cfg.KillGrace):
	}
	s.mu.Lock()
	if s.child == c {
		s.state = StateUnreaped
		s.reason = ErrUnreaped
	}
	s.mu.Unlock()
	return ErrUnreaped
}

// Restart stops and reaps the current child, then starts a new generation that
// carries none of the predecessor's plans or commands.
func (s *Supervisor) Restart(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	return s.Start()
}
