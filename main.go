package main

import (
	"errors"
	"log"
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
	timeout int

	deps []*Actor
}

type result struct {
	err error
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
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, dep := range a.deps {
		wg.Go(func() {
			if err := dep.Start(); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if len(errs) > 0 {
		a.Lock()
		a.status = dep_failed
		a.Unlock()
		return errs[0]
	}

	// Start self
	var err error
	if a.timeout == 0 {
		err = a.startFn()
		if err != nil {
			a.Lock()
			a.status = failed
			a.Unlock()
			return err
		}
		a.Lock()
		a.status = running
		a.Unlock()
		log.Printf("%s: started", a.name)
		return nil
	}

	c := make(chan result, 1)
	go func() {
		c <- result{a.startFn()}
	}()

	select {
	case res := <-c:
		if res.err != nil {
			a.Lock()
			a.status = failed
			a.Unlock()
			return res.err
		}
	case <-time.After(time.Duration(a.timeout) * time.Millisecond):
		a.Lock()
		a.status = failed
		a.Unlock()
		return errors.New("timeout")
	}

	a.Lock()
	a.status = running
	a.Unlock()
	log.Printf("%s: started", a.name)
	return nil
}

func main() {
	net := &Actor{
		timeout: 60,
		name:    "network",
		startFn: func() error {
			return errors.New("network error")
		},
	}

	db := &Actor{
		timeout: 100,
		name:    "database",
		startFn: func() error {
			return errors.New("port busy")
		},
		deps: []*Actor{net},
	}

	cache := &Actor{
		timeout: 100,
		name:    "cache",
		startFn: func() error {
			return nil
		},
		deps: []*Actor{net},
	}

	ui := &Actor{
		timeout: 200,
		name:    "UI",
		startFn: func() error {
			return nil
		},
		deps: []*Actor{db, cache},
	}

	if err := ui.Start(); err != nil {
		log.Printf("failed to start root: %v", err)
	}
	log.Printf("ui status: %v", ui.status)
	log.Printf("net status: %v", net.status)
	log.Printf("db status: %v", db.status)
	log.Printf("cache status: %v", cache.status)
}
