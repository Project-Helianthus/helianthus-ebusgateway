package main

import (
	"crypto/sha256"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime/debug"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/localworkerhost"
)

const (
	ebusLocalWorkerExitStopped = 0
	ebusLocalWorkerExitFailed  = 1
	ebusLocalWorkerExitRefused = 2
)

var errEBusLocalWorkerUnavailable = errors.New("ebus local worker: no qualified backend is linked; the local worker stays unavailable")

// ebusLocalWorkerChildPorts are everything the worker branch may touch. There
// is no device, listener, transmit or launch port.
type ebusLocalWorkerChildPorts interface {
	Identity() (localworkerhost.Identity, error)
	Inspect(fd int) (localworkerhost.Observed, error)
	Getenv(key string) string
	Control(fd int) (io.ReadWriteCloser, error)
	After(time.Duration) <-chan time.Time
}

// dispatchEBusLocalWorker selects the worker branch from argv alone. It runs
// before any gateway configuration, flag binding, listener or job exists.
func dispatchEBusLocalWorker(argv []string, ports func() ebusLocalWorkerChildPorts) (int, bool) {
	if !localworkerhost.SelectsChild(argv) {
		return 0, false
	}
	return runEBusLocalWorkerChild(argv[1:], ports()), true
}

// runEBusLocalWorkerChild validates the plan, identity, descriptors and
// runtime policy, reports passive TX-inactive readiness and renews its lease
// until the parent closes the control descriptor. No backend is linked, so
// the child never leaves the passive state.
func runEBusLocalWorkerChild(args []string, ports ebusLocalWorkerChildPorts) int {
	plan, err := localworkerhost.ParseChildArgv(args)
	if err != nil {
		log.Printf("ebus local worker: refused: %v", err)
		return ebusLocalWorkerExitRefused
	}
	self, err := ports.Identity()
	if err != nil {
		log.Printf("ebus local worker: refused: %v", err)
		return ebusLocalWorkerExitRefused
	}
	observed := make([]localworkerhost.Observed, 0, len(plan.Descriptors))
	for i := range plan.Descriptors {
		descriptor, err := ports.Inspect(localworkerhost.FirstDescriptor + i)
		if err != nil {
			log.Printf("ebus local worker: refused: %v", err)
			return ebusLocalWorkerExitRefused
		}
		observed = append(observed, descriptor)
	}
	if err := localworkerhost.ValidateChild(plan, self, observed, ports.Getenv); err != nil {
		log.Printf("ebus local worker: refused: %v", err)
		return ebusLocalWorkerExitRefused
	}
	control, err := ports.Control(localworkerhost.FirstDescriptor)
	if err != nil {
		log.Printf("ebus local worker: refused: %v", err)
		return ebusLocalWorkerExitRefused
	}
	return serveEBusLocalWorkerPassive(control, plan, self, ports)
}

func serveEBusLocalWorkerPassive(control io.ReadWriteCloser, plan localworkerhost.ChildPlan, self localworkerhost.Identity, ports ebusLocalWorkerChildPorts) int {
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_, _ = io.Copy(io.Discard, control)
	}()
	code := ebusLocalWorkerExitStopped
	report := localworkerhost.Report{
		Kind:        localworkerhost.ReportReady,
		Identity:    self,
		Generation:  plan.Generation,
		Descriptors: plan.Descriptors,
		State:       localworkerhost.WorkerPassive,
	}
	for {
		if err := writeEBusLocalWorkerReport(control, report); err != nil {
			code = ebusLocalWorkerExitFailed
			break
		}
		stopped := false
		select {
		case <-closed:
			stopped = true
		case <-ports.After(plan.LeaseInterval):
		}
		if stopped {
			break
		}
		report = localworkerhost.Report{
			Kind:       localworkerhost.ReportLease,
			Identity:   self,
			Generation: plan.Generation,
			State:      localworkerhost.WorkerPassive,
			Sequence:   report.Sequence + 1,
		}
	}
	_ = control.Close()
	<-closed
	return code
}

func writeEBusLocalWorkerReport(w io.Writer, report localworkerhost.Report) error {
	raw, err := localworkerhost.EncodeReport(report)
	if err != nil {
		return err
	}
	_, err = w.Write(raw[:])
	return err
}

// osEBusLocalWorkerChildPorts inspects only inherited descriptors and the
// child's own executable image; it opens no device.
type osEBusLocalWorkerChildPorts struct{}

func newEBusLocalWorkerChildPorts() ebusLocalWorkerChildPorts { return osEBusLocalWorkerChildPorts{} }

func (osEBusLocalWorkerChildPorts) Identity() (localworkerhost.Identity, error) {
	return ebusLocalWorkerSelfIdentity()
}

func (osEBusLocalWorkerChildPorts) Inspect(fd int) (localworkerhost.Observed, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return localworkerhost.Observed{}, err
	}
	observed := localworkerhost.Observed{FD: fd}
	switch uint32(stat.Mode) & unix.S_IFMT {
	case unix.S_IFIFO, unix.S_IFSOCK:
		observed.Kind = localworkerhost.KindStream
	case unix.S_IFREG:
		observed.Kind = localworkerhost.KindMapping
		if stat.Size > 0 {
			observed.Size = uint64(stat.Size)
		}
	}
	return observed, nil
}

func (osEBusLocalWorkerChildPorts) Getenv(key string) string { return os.Getenv(key) }

func (osEBusLocalWorkerChildPorts) Control(fd int) (io.ReadWriteCloser, error) {
	file := os.NewFile(uintptr(fd), "ebus-local-worker-control")
	if file == nil {
		return nil, localworkerhost.ErrDescriptor
	}
	return file, nil
}

func (osEBusLocalWorkerChildPorts) After(d time.Duration) <-chan time.Time { return time.After(d) }

// ebusLocalWorkerSelfIdentity digests the embedded build description and the
// executable image, so a replaced executable cannot pass as its parent.
func ebusLocalWorkerSelfIdentity() (localworkerhost.Identity, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return localworkerhost.Identity{}, localworkerhost.ErrBuild
	}
	path, err := os.Executable()
	if err != nil {
		return localworkerhost.Identity{}, err
	}
	image, err := os.Open(path)
	if err != nil {
		return localworkerhost.Identity{}, err
	}
	defer func() { _ = image.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, image); err != nil {
		return localworkerhost.Identity{}, err
	}
	var executable localworkerhost.Digest
	copy(executable[:], hash.Sum(nil))
	return localworkerhost.Identity{
		Build:      localworkerhost.DigestOf([]byte(info.String())),
		Executable: executable,
		ABI:        localworkerhost.ABIVersion,
	}, nil
}

// ebusLocalWorkerBackend is the frozen seam a qualified backend fills: the
// fixed descriptor layout, its allocator and the child runtime policy.
type ebusLocalWorkerBackend interface {
	Allocator() localworkerhost.Allocator
	Layout() []localworkerhost.Descriptor
	Runtime() localworkerhost.RuntimePolicy
}

// ebusLocalWorkerParent is the parent-side process context of a supervisor.
type ebusLocalWorkerParent struct {
	Self        localworkerhost.Identity
	Executable  string
	ProcessArgs []string
	Env         []string
	Launcher    localworkerhost.Launcher
	Clock       localworkerhost.Clock
}

const (
	ebusLocalWorkerLeaseInterval = 250 * time.Millisecond
	ebusLocalWorkerLeaseTTL      = time.Second
	ebusLocalWorkerReadyTimeout  = 5 * time.Second
	ebusLocalWorkerStopGrace     = 2 * time.Second
	ebusLocalWorkerKillGrace     = 2 * time.Second
)

// composeEBusLocalWorker builds the supervisor only for a linked backend.
// Without one the local worker stays unavailable and nothing is launched.
func composeEBusLocalWorker(backend ebusLocalWorkerBackend, parent ebusLocalWorkerParent) (*localworkerhost.Supervisor, error) {
	if backend == nil {
		return nil, errEBusLocalWorkerUnavailable
	}
	return localworkerhost.New(localworkerhost.Config{
		Self:          parent.Self,
		Executable:    parent.Executable,
		ProcessArgs:   parent.ProcessArgs,
		BaseEnv:       parent.Env,
		Descriptors:   backend.Layout(),
		Runtime:       backend.Runtime(),
		LeaseInterval: ebusLocalWorkerLeaseInterval,
		LeaseTTL:      ebusLocalWorkerLeaseTTL,
		ReadyTimeout:  ebusLocalWorkerReadyTimeout,
		StopGrace:     ebusLocalWorkerStopGrace,
		KillGrace:     ebusLocalWorkerKillGrace,
		Allocator:     backend.Allocator(),
		Launcher:      parent.Launcher,
		Clock:         parent.Clock,
	})
}

// ebusLocalWorkerFile is a resource that can be inherited by a child.
type ebusLocalWorkerFile interface {
	File() *os.File
}

// execEBusLocalWorkerLauncher re-executes the same binary through os/exec with
// an explicit path, argv, environment and inherited descriptors; no shell.
type execEBusLocalWorkerLauncher struct{}

func (execEBusLocalWorkerLauncher) command(spec localworkerhost.LaunchSpec) (*exec.Cmd, error) {
	if !localworkerhost.SelectsChild(spec.Args) || len(spec.Args) == 0 || spec.Args[0] != spec.Path {
		return nil, localworkerhost.ErrArgv
	}
	files := make([]*os.File, 0, len(spec.Files))
	for _, resource := range spec.Files {
		inheritable, ok := resource.(ebusLocalWorkerFile)
		if !ok || inheritable.File() == nil {
			return nil, localworkerhost.ErrDescriptor
		}
		files = append(files, inheritable.File())
	}
	return &exec.Cmd{
		Path:       spec.Path,
		Args:       append([]string(nil), spec.Args...),
		Env:        append([]string(nil), spec.Env...),
		ExtraFiles: files,
		Stderr:     os.Stderr,
	}, nil
}

func (l execEBusLocalWorkerLauncher) Launch(spec localworkerhost.LaunchSpec) (localworkerhost.Process, error) {
	cmd, err := l.command(spec)
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return execEBusLocalWorkerProcess{cmd: cmd}, nil
}

type execEBusLocalWorkerProcess struct{ cmd *exec.Cmd }

func (p execEBusLocalWorkerProcess) Wait() error { return p.cmd.Wait() }
func (p execEBusLocalWorkerProcess) Stop() error { return p.cmd.Process.Signal(syscall.SIGTERM) }
func (p execEBusLocalWorkerProcess) Kill() error { return p.cmd.Process.Kill() }

type systemEBusLocalWorkerClock struct{}

func (systemEBusLocalWorkerClock) Now() time.Time                         { return time.Now() }
func (systemEBusLocalWorkerClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
