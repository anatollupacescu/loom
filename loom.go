package loom

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Status represents the lifecycle state of a service.
type Status int32

const (
	Idle Status = iota
	Starting
	Running
	Stopping
	Stopped
	Failed
	DepFailed
)

func (s Status) String() string {
	switch s {
	case Idle:
		return "idle"
	case Starting:
		return "starting"
	case Running:
		return "running"
	case Stopping:
		return "stopping"
	case Stopped:
		return "stopped"
	case Failed:
		return "failed"
	case DepFailed:
		return "dependency failed"
	default:
		return "unknown"
	}
}

// Service manages the lifecycle of a service and its dependencies.
type Service struct {
	startOnce sync.Once
	startErr  error
	stopOnce  sync.Once
	stopErr   error

	name  string
	state atomic.Int32

	startFn func(ctx context.Context) error
	stopFn  func(ctx context.Context) error // nil means no-op
	timeout time.Duration                   // 0 means no timeout; used as grace period for both start and stop

	deps []*Service
}

// Option configures a service.
type Option func(*Service)

// WithStopFn sets the shutdown function for the service.
func WithStopFn(fn func(ctx context.Context) error) Option {
	return func(a *Service) { a.stopFn = fn }
}

// WithTimeout sets the start/stop grace period for the service.
func WithTimeout(d time.Duration) Option {
	return func(a *Service) { a.timeout = d }
}

// WithDeps declares services that must be fully started before this one.
func WithDeps(deps ...*Service) Option {
	return func(a *Service) { a.deps = append(a.deps, deps...) }
}

// New creates a named service with the given start function and options.
func New(name string, startFn func(ctx context.Context) error, opts ...Option) *Service {
	a := &Service{
		name:    name,
		startFn: startFn,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Status returns the service's current lifecycle status. Safe for concurrent use.
func (a *Service) Status() Status {
	return Status(a.state.Load())
}

func (a *Service) setStatus(s Status) {
	a.state.Store(int32(s))
}

// Validate checks the dependency graph for cycles. Call before Start.
func (a *Service) Validate() error {
	return checkCycle(a, make(map[*Service]bool), make(map[*Service]bool), nil)
}

func checkCycle(a *Service, inStack, visited map[*Service]bool, path []*Service) error {
	if inStack[a] {
		names := make([]string, len(path))
		for i, p := range path {
			names[i] = p.name
		}
		return fmt.Errorf("cycle detected: %s", strings.Join(append(names, a.name), " -> "))
	}
	if visited[a] {
		return nil
	}
	inStack[a] = true
	path = append(path, a)
	for _, dep := range a.deps {
		if err := checkCycle(dep, inStack, visited, path); err != nil {
			return err
		}
	}
	inStack[a] = false
	visited[a] = true
	return nil
}

// Start begins the service and all its dependencies concurrently.
// Idempotent: subsequent calls return the stored result immediately.
func (a *Service) Start(ctx context.Context) error {
	a.startOnce.Do(func() {
		a.startErr = a.start(ctx)
	})
	return a.startErr
}

func (a *Service) start(ctx context.Context) error {
	// Start all dependencies concurrently, collecting every error.
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, dep := range a.deps {
		wg.Go(func() {
			if err := dep.Start(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("dep %q: %w", dep.name, err))
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		a.setStatus(DepFailed)
		return err
	}

	a.setStatus(Starting)

	startCtx := ctx
	var cancel context.CancelFunc
	if a.timeout > 0 {
		startCtx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}

	if err := a.startFn(startCtx); err != nil {
		a.setStatus(Failed)
		return err
	}

	a.setStatus(Running)
	log.Printf("%s: started", a.name)
	return nil
}

// Stop shuts down the service, then its dependencies concurrently.
// Idempotent. Best-effort: dep errors are logged but never block traversal.
func (a *Service) Stop(ctx context.Context) error {
	a.stopOnce.Do(func() {
		a.stopErr = a.stop(ctx)
	})
	return a.stopErr
}

func (a *Service) stop(ctx context.Context) error {
	var selfErr error

	// Only run stopFn if the service actually reached Running.
	if a.state.CompareAndSwap(int32(Running), int32(Stopping)) {
		if a.stopFn != nil {
			stopCtx := ctx
			var cancel context.CancelFunc
			if a.timeout > 0 {
				stopCtx, cancel = context.WithTimeout(ctx, a.timeout)
				defer cancel()
			}
			if err := a.stopFn(stopCtx); err != nil {
				log.Printf("%s: stop error: %v", a.name, err)
				a.setStatus(Failed)
				selfErr = err
			} else {
				a.setStatus(Stopped)
				log.Printf("%s: stopped", a.name)
			}
		} else {
			a.setStatus(Stopped)
			log.Printf("%s: stopped", a.name)
		}
	}

	// Always fan out to deps regardless of own status or stop result.
	var wg sync.WaitGroup
	for _, dep := range a.deps {
		wg.Go(func() {
			if err := dep.Stop(ctx); err != nil {
				log.Printf("%s: dep %q stop error: %v", a.name, dep.name, err)
			}
		})
	}
	wg.Wait()

	return selfErr
}

// Reset clears start/stop state so the service can be restarted.
// Must not be called concurrently with Start or Stop.
func (a *Service) Reset() {
	a.startOnce = sync.Once{}
	a.stopOnce = sync.Once{}
	a.startErr = nil
	a.stopErr = nil
	a.setStatus(Idle)
}
