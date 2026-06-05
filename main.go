package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	idle = iota
	running
	stopping
	stopped
	failed
	dep_failed
)

type status int

func (s status) String() string {
	switch s {
	case idle:
		return "idle"
	case running:
		return "running"
	case stopping:
		return "stopping"
	case stopped:
		return "stopped"
	case failed:
		return "failed"
	case dep_failed:
		return "dependency failed"
	default:
		return "unknown"
	}
}

type Actor struct {
	sync.Mutex

	startOnce sync.Once
	startErr  error
	stopOnce  sync.Once
	stopErr   error

	name   string
	status status

	startFn func(ctx context.Context) error
	stopFn  func(ctx context.Context) error // nil means no-op
	timeout time.Duration                   // 0 means no timeout; used as grace period for both start and stop

	deps []*Actor
}

func (a *Actor) Validate() error {
	return checkCycle(a, make(map[*Actor]bool), make(map[*Actor]bool), nil)
}

func checkCycle(a *Actor, inStack map[*Actor]bool, visited map[*Actor]bool, path []*Actor) error {
	if inStack[a] {
		names := make([]string, len(path))
		for i, p := range path {
			names[i] = p.name
		}
		names = append(names, a.name)
		return fmt.Errorf("cycle detected: %s", strings.Join(names, " -> "))
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

// Start begins the actor and all its dependencies.
// The context is used for the overall startup chain; each actor derives a
// child context from it using its own timeout. Note: only the first caller's
// context is used — subsequent calls return the stored result immediately.
func (a *Actor) Start(ctx context.Context) error {
	a.startOnce.Do(func() {
		a.startErr = a.start(ctx)
	})
	return a.startErr
}

func (a *Actor) start(ctx context.Context) error {
	// Start all dependencies concurrently
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	for _, dep := range a.deps {
		wg.Go(func() {
			if err := dep.Start(ctx); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if firstErr != nil {
		a.status = dep_failed
		return firstErr
	}

	// Derive a child context for this actor's own startFn
	startCtx := ctx
	var cancel context.CancelFunc
	if a.timeout > 0 {
		startCtx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}

	if err := a.startFn(startCtx); err != nil {
		a.status = failed
		return err
	}

	a.status = running
	log.Printf("%s: started", a.name)
	return nil
}

// Stop shuts down the actor and all its dependencies in reverse order.
// The provided context is the top-level shutdown context (e.g. from an OS
// signal); each actor derives a child context from it using its own timeout.
// Shutdown is best-effort: a failed or timed-out stopFn is logged but does
// not block the rest of the graph from stopping.
func (a *Actor) Stop(ctx context.Context) error {
	a.stopOnce.Do(func() {
		a.stopErr = a.stop(ctx)
	})
	return a.stopErr
}

func (a *Actor) stop(ctx context.Context) error {
	// Stop self only if running — but always traverse deps regardless
	if a.status == running {
		a.status = stopping

		if a.stopFn != nil {
			stopCtx := ctx
			var cancel context.CancelFunc
			if a.timeout > 0 {
				stopCtx, cancel = context.WithTimeout(ctx, a.timeout)
				defer cancel()
			}

			if err := a.stopFn(stopCtx); err != nil {
				// best-effort: log, record, but continue to stop deps
				log.Printf("%s: stop error: %v", a.name, err)
				a.status = failed
				a.stopErr = err
			} else {
				a.status = stopped
				log.Printf("%s: stopped", a.name)
			}
		} else {
			a.status = stopped
			log.Printf("%s: stopped", a.name)
		}
	}

	// Fan out to deps in parallel — always, regardless of own status or stop result
	var wg sync.WaitGroup
	for _, dep := range a.deps {
		wg.Go(func() {
			if err := dep.Stop(ctx); err != nil {
				log.Printf("%s: dep (%s) stop error: %v", a.name, dep.name, err)
			}
		})
	}
	wg.Wait()

	return a.stopErr
}

func main() {
	net := &Actor{
		timeout: 60 * time.Millisecond,
		name:    "network",
		startFn: func(ctx context.Context) error {
			return nil
		},
		stopFn: func(ctx context.Context) error {
			return nil
		},
	}

	db := &Actor{
		timeout: 100 * time.Millisecond,
		name:    "Database",
		startFn: func(ctx context.Context) error {
			return errors.New("port busy")
		},
		stopFn: func(ctx context.Context) error {
			return nil
		},
		deps: []*Actor{net},
	}

	cache := &Actor{
		timeout: 100 * time.Millisecond,
		name:    "Cache",
		startFn: func(ctx context.Context) error {
			return nil
		},
		stopFn: func(ctx context.Context) error {
			return nil
		},
		deps: []*Actor{net},
	}

	ui := &Actor{
		timeout: 200 * time.Millisecond,
		name:    "UI",
		startFn: func(ctx context.Context) error {
			return nil
		},
		stopFn: func(ctx context.Context) error {
			return nil
		},
		deps: []*Actor{db, cache},
	}

	if err := ui.Validate(); err != nil {
		log.Printf("validation error: %v", err)
		return
	}

	ctx := context.Background()

	if err := ui.Start(ctx); err != nil {
		log.Printf("failed to start root: %v", err)
	}
	log.Printf("ui status: %v", ui.status)
	log.Printf("net status: %v", net.status)
	log.Printf("db status: %v", db.status)
	log.Printf("cache status: %v", cache.status)

	log.Println("--- shutting down ---")

	if err := ui.Stop(ctx); err != nil {
		log.Printf("failed to stop root: %v", err)
	}
	log.Printf("ui status: %v", ui.status)
	log.Printf("net status: %v", net.status)
	log.Printf("db status: %v", db.status)
	log.Printf("cache status: %v", cache.status)
}
