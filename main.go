package main

import (
	"log"
	"sync"
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

type Actor struct {
	sync.Mutex
	name    string
	status  int
	startFn func() error
	deps    []*Actor
}

func (a *Actor) Start(timeout int) error {
	var err error
	for _, dep := range a.deps {
		if err != nil {
			log.Printf("%s: skippoing dependency actor (%s): %s", a.name, dep.name, err)
			break
		}
		func() {
			dep.Lock()
			defer dep.Unlock()
			if dep.status == idle {
				log.Printf("%s: starting dependency actor (%s)", a.name, dep.name)
				if err = dep.Start(timeout); err != nil {
					a.status = dep_failed
					log.Printf("%s: failed to start dependency actor (%s): %v", a.name, dep.name, err)
					return
				}
			} else {
				log.Printf("%s: dependency actor already in status (%s): %d", a.name, dep.name, dep.status)
			}
		}()
	}
	if a.status == dep_failed {
		return err
	}
	if err := a.startFn(); err != nil {
		a.status = failed
		return err
	}
	a.status = running
	return nil
}

func main() {

	net := &Actor{
		name: "network",
		startFn: func() error {
			return nil //errors.New("network error")
		},
	}

	db := &Actor{
		name: "Database",
		startFn: func() error {
			log.Println("DB is running")
			return nil
		},
		deps: []*Actor{net},
	}

	ui := &Actor{
		name: "UI",
		startFn: func() error {
			log.Println("UI is running")
			return nil
		},
		deps: []*Actor{db, net},
	}

	_ = ui.Start(100)
	log.Printf("ui status: %v", ui.status)
}
