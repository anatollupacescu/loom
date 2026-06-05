package loom_test

import (
	"context"
	"errors"
	"log"
	"time"

	. "github.com/anatollupacescu/loom"
)

func Example() {

	net := New("network",
		func(ctx context.Context) error { return nil },
		WithTimeout(60*time.Millisecond),
		WithStopFn(func(ctx context.Context) error { return nil }),
	)

	db := New("database",
		func(ctx context.Context) error { return errors.New("port busy") },
		WithTimeout(100*time.Millisecond),
		WithStopFn(func(ctx context.Context) error { return nil }),
		WithDeps(net),
	)

	cache := New("cache",
		func(ctx context.Context) error { return nil },
		WithTimeout(100*time.Millisecond),
		WithStopFn(func(ctx context.Context) error { return nil }),
		WithDeps(net),
	)

	ui := New("ui",
		func(ctx context.Context) error { return nil },
		WithTimeout(200*time.Millisecond),
		WithStopFn(func(ctx context.Context) error { return nil }),
		WithDeps(db, cache),
	)

	if err := ui.Validate(); err != nil {
		log.Fatalf("validation error: %v", err)
	}

	ctx := context.Background()

	if err := ui.Start(ctx); err != nil {
		log.Printf("start error: %v", err)
	}
	log.Printf("ui:    %v", ui.Status())
	log.Printf("net:   %v", net.Status())
	log.Printf("db:    %v", db.Status())
	log.Printf("cache: %v", cache.Status())

	log.Println("--- shutting down ---")

	if err := ui.Stop(ctx); err != nil {
		log.Printf("stop error: %v", err)
	}
	log.Printf("ui:    %v", ui.Status())
	log.Printf("net:   %v", net.Status())
	log.Printf("db:    %v", db.Status())
	log.Printf("cache: %v", cache.Status())

	//Output:
}
