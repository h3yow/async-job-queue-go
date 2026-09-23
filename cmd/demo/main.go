// Command demo drives the persistent job queue end to end against a local
// JSON file so you can observe delivery, retry, dead-letter and restart
// recovery semantics by hand.
//
// Usage:
//
//	go run ./cmd/demo            # enqueue, consume, retry, dead-letter, then exit
//	go run ./cmd/demo -restart   # reopen the same file and recover unfinished jobs
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	jq "github.com/h3yow/async-job-queue-go"
)

const storeFile = "./.demo/queue.json"

func main() {
	restart := flag.Bool("restart", false, "reopen the store file and recover instead of seeding")
	flag.Parse()

	logger := log.New(os.Stdout, "demo: ", log.LstdFlags|log.Lmicroseconds)

	store, err := jq.OpenStore(storeFile)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}

	var mu sync.Mutex
	attempts := map[string]int{}
	cfg := jq.Config{
		Workers:      3,
		LeaseTTL:     2 * time.Second,
		MaxAttempts:  3,
		PollInterval: 20 * time.Millisecond,
		Backoff:      jq.ExponentialBackoff{BaseDelay: 50 * time.Millisecond, MaxDelay: 500 * time.Millisecond},
		ConsumerID:   consumerID(*restart),
		Logger:       logger,
		Handler: func(ctx context.Context, d *jq.Delivery) error {
			mu.Lock()
			attempts[d.Job.ID]++
			n := attempts[d.Job.ID]
			mu.Unlock()
			_ = d.UpdateProgress(33 * n)
			switch d.Job.ID {
			case "demo-ok":
				return nil
			case "demo-retry":
				if n < 2 {
					return jq.RetryError("simulated transient failure")
				}
				return nil
			case "demo-terminal":
				return jq.TerminalError("simulated unrecoverable payload error")
			default:
				return nil
			}
		},
	}
	queue := jq.New(store, cfg)

	if !*restart {
		for _, id := range []string{"demo-ok", "demo-retry", "demo-terminal"} {
			if err := queue.Enqueue(id, map[string]any{"seeded": !*restart}); err != nil {
				log.Fatalf("enqueue %s: %v", id, err)
			}
		}
		logger.Printf("seeded 3 jobs into %s", storeFile)
	} else {
		logger.Printf("reopened %s; stale leases recovered automatically", storeFile)
	}

	queue.Start()
	time.Sleep(1500 * time.Millisecond)

	if err := queue.Shutdown(context.Background()); err != nil {
		log.Fatalf("shutdown: %v", err)
	}

	jobs, err := store.All()
	if err == nil {
		_ = jobs
	}
	// Store is closed after Shutdown; reopen read-only-ish for a final report.
	report, err := jq.OpenStore(storeFile)
	if err != nil {
		log.Fatalf("reopen for report: %v", err)
	}
	all, _ := report.All()
	fmt.Println("\n=== final durable state ===")
	for _, j := range all {
		fmt.Printf("id=%-16s status=%-9s attempts=%d progress=%d reason=%q\n",
			j.ID, j.Status, j.Attempts, j.Progress, j.DeadReason)
	}
	_ = report.Close()
}

func consumerID(restart bool) string {
	if restart {
		return "demo-restart"
	}
	return "demo-bootstrap"
}
