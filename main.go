package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	idle = iota
	starting
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
	case starting:
		return "starting"
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
	once     sync.Once
	startErr error

	name   string
	status status

	startFn func() error
	timeout time.Duration // 0 means no timeout

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
	visited[a] = true // fully explored, safe to skip on future visits
	return nil
}

func (a *Actor) Start() error {
	a.once.Do(func() {
		a.startErr = a.start()
	})
	return a.startErr
}

func (a *Actor) start() error {
	// Start all dependencies concurrently
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	for _, dep := range a.deps {
		wg.Go(func() {
			if err := dep.Start(); err != nil {
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

	// Start self
	if a.timeout == 0 {
		err := a.startFn()
		if err != nil {
			a.status = failed
			return err
		}
		a.status = running
		log.Printf("%s: started", a.name)
		return nil
	}

	c := make(chan error, 1)
	go func() {
		c <- a.startFn()
	}()

	select {
	case err := <-c:
		if err != nil {
			a.status = failed
			return err
		}
	case <-time.After(a.timeout):
		a.status = failed
		return errors.New("timeout")
	}

	a.status = running
	log.Printf("%s: started", a.name)
	return nil
}

func main() {
	net := &Actor{
		timeout: 60 * time.Millisecond,
		name:    "network",
		startFn: func() error {
			return nil //errors.New("network error")
		},
	}

	db := &Actor{
		timeout: 100 * time.Millisecond,
		name:    "Database",
		startFn: func() error {
			return errors.New("port busy")
		},
		deps: []*Actor{net},
	}

	cache := &Actor{
		timeout: 100 * time.Millisecond,
		name:    "Cache",
		startFn: func() error {
			return nil
		},
		deps: []*Actor{net},
	}

	ui := &Actor{
		timeout: 200 * time.Millisecond,
		name:    "UI",
		startFn: func() error {
			return nil
		},
		deps: []*Actor{db, cache},
	}

	if err := ui.Validate(); err != nil {
		log.Printf("validation error: %v", err)
		return
	}

	if err := ui.Start(); err != nil {
		log.Printf("failed to start root: %v", err)
	}

	log.Printf("ui status: %v", ui.status)
	log.Printf("net status: %v", net.status)
	log.Printf("db status: %v", db.status)
	log.Printf("cache status: %v", cache.status)
}
