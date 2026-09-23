package jobqueue

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Config configures a Queue.
type Config struct {
	// Workers is the number of concurrent consumers.
	Workers int
	// LeaseTTL is the visibility window: a delivery not acknowledged before
	// it expires becomes redeliverable to another consumer.
	LeaseTTL time.Duration
	// MaxAttempts is the maximum number of deliveries (1 = no retry).
	MaxAttempts int
	// Backoff selects the retry delay policy.
	Backoff Backoff
	// PollInterval is how often idle consumers look for new work.
	PollInterval time.Duration
	// ConsumerID identifies this process in lease records and logs.
	ConsumerID string
	// Handler processes each delivery.
	Handler Handler
	// Now overrides the clock (tests). nil uses wall clock.
	Now func() time.Time
	// Logger receives structured delivery decisions (nil -> stderr logger).
	Logger *log.Logger
}

// EnqueuePayload is the user payload of a job.
type EnqueuePayload = map[string]any

// Queue runs multiple consumers over a persistent Store and provides the
// user-facing enqueue/inspection API.
type Queue struct {
	store Store
	cfg   Config
	now   func() time.Time
	logf  func(format string, args ...any)

	mu       sync.Mutex
	started  bool
	closed   bool
	inflight map[string]string // jobID -> leaseID currently being handled

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New constructs a Queue with defaults applied. Call Start to run consumers.
func New(store Store, cfg Config) *Queue {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExponentialBackoff{BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 20 * time.Millisecond
	}
	if cfg.ConsumerID == "" {
		host, _ := os.Hostname()
		cfg.ConsumerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "jobqueue: ", log.LstdFlags|log.Lmicroseconds)
	}
	return &Queue{
		store:    store,
		cfg:      cfg,
		now:      now,
		logf:     logger.Printf,
		inflight: map[string]string{},
	}
}

// Start launches the consumer goroutines. It must be called at most once.
func (q *Queue) Start() {
	q.mu.Lock()
	if q.started {
		q.mu.Unlock()
		return
	}
	q.started = true
	q.ctx, q.cancel = context.WithCancel(context.Background())
	ctx := q.ctx
	q.mu.Unlock()

	for i := 0; i < cfgWorkers(q); i++ {
		q.wg.Add(1)
		go q.worker(i, ctx)
	}
}

func cfgWorkers(q *Queue) int { return q.cfg.Workers }

func (q *Queue) worker(n int, ctx context.Context) {
	defer q.wg.Done()
	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		processed, err := q.RunOne(ctx)
		if err != nil && err != ErrNotFound && err != context.Canceled && err != ErrClosed {
			q.logf("worker=%d consumer=%s poll error: %v", n, q.cfg.ConsumerID, err)
		}
		_ = processed
	}
}

// RunOne claims and processes at most one job synchronously. It returns true
// when a job was claimed (even if that delivery failed and was retried).
func (q *Queue) RunOne(ctx context.Context) (bool, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false, ErrClosed
	}
	q.mu.Unlock()

	d, reclaimed, err := q.store.Claim(q.cfg.ConsumerID, q.cfg.LeaseTTL, q.now())
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	q.track(d.Job.ID, d.LeaseID)
	defer q.untrack(d.Job.ID, d.LeaseID)

	// Safety valve: a job whose deliveries only ever time out (worker keeps
	// crashing before it can nack) must not loop forever.
	if reclaimed && d.Attempt > d.Job.MaxAttempts {
		reason := fmt.Sprintf(
			"delivery attempts exhausted: lease expired without ack %d times (max=%d)",
			d.Attempt, d.Job.MaxAttempts)
		if derr := q.store.Dead(d.Job.ID, d.LeaseID, reason, q.now()); derr != nil {
			q.logf("job=%s attempts=%d/%d basis=lease-timeout-exhausted dead-letter failed: %v",
				d.Job.ID, d.Attempt, d.Job.MaxAttempts, derr)
		} else {
			q.logf("job=%s attempts=%d/%d basis=lease-timeout-exhausted decision=dead-letter reason=%q",
				d.Job.ID, d.Attempt, d.Job.MaxAttempts, reason)
		}
		return true, nil
	}

	if reclaimed {
		q.logf("job=%s attempts=%d/%d basis=lease-expired decision=redeliver owner=%s",
			d.Job.ID, d.Attempt, d.Job.MaxAttempts, q.cfg.ConsumerID)
	} else {
		q.logf("job=%s attempts=%d/%d basis=claim decision=deliver owner=%s",
			d.Job.ID, d.Attempt, d.Job.MaxAttempts, q.cfg.ConsumerID)
	}

	if q.cfg.Handler == nil {
		// No handler: release immediately so the job stays redeliverable.
		if rerr := q.store.Release(d.Job.ID, d.LeaseID, q.now()); rerr != nil {
			q.logf("job=%s attempts=%d release without handler failed: %v", d.Job.ID, d.Attempt, rerr)
		}
		return true, nil
	}

	delivery := *d
	delivery.q = q
	herr := q.cfg.Handler(ctx, &delivery)
	if herr != nil && q.shuttingDown() {
		// Shutdown in progress: an unfinished delivery is released
		// immediately (no backoff) so it stays redeliverable after restart.
		if rerr := q.store.Release(d.Job.ID, d.LeaseID, q.now()); rerr != nil {
			if rerr == ErrLeaseLost {
				q.logf("job=%s attempts=%d/%d basis=shutdown decision=ignored reason=lease-lost",
					d.Job.ID, d.Attempt, d.Job.MaxAttempts)
			} else {
				q.logf("job=%s attempts=%d/%d basis=shutdown release failed: %v",
					d.Job.ID, d.Attempt, d.Job.MaxAttempts, rerr)
			}
		} else {
			q.logf("job=%s attempts=%d/%d basis=shutdown decision=release-to-pending",
				d.Job.ID, d.Attempt, d.Job.MaxAttempts)
		}
		return true, nil
	}
	q.resolve(&delivery, herr)
	return true, nil
}

// resolve applies ack / retry / dead-letter based on the handler result, and
// logs every decision with job id, delivery count and the judgment basis.
func (q *Queue) resolve(d *Delivery, herr error) {
	now := q.now()
	if herr == nil {
		if err := q.store.Complete(d.Job.ID, d.LeaseID, now); err != nil {
			// Lease expired and the job is already being redelivered. The
			// ack is discarded; the new owner decides the outcome.
			q.logf("job=%s attempts=%d/%d basis=ack decision=ignored reason=lease-lost (%v)",
				d.Job.ID, d.Attempt, d.Job.MaxAttempts, err)
			return
		}
		q.logf("job=%s attempts=%d/%d basis=handler-success decision=ack",
			d.Job.ID, d.Attempt, d.Job.MaxAttempts)
		return
	}

	decision, reason := classifyError(herr)
	if decision == DecisionTerminal {
		msg := fmt.Sprintf("terminal failure: %s", reason)
		if err := q.store.Dead(d.Job.ID, d.LeaseID, msg, now); err != nil {
			q.logf("job=%s attempts=%d/%d basis=terminal-error decision=dead-letter failed: %v",
				d.Job.ID, d.Attempt, d.Job.MaxAttempts, err)
			return
		}
		q.logf("job=%s attempts=%d/%d basis=terminal-error decision=dead-letter reason=%q",
			d.Job.ID, d.Attempt, d.Job.MaxAttempts, msg)
		return
	}

	// Retryable failure.
	if d.Attempt >= d.Job.MaxAttempts {
		msg := fmt.Sprintf("retries exhausted after %d/%d deliveries: %s",
			d.Attempt, d.Job.MaxAttempts, reason)
		if err := q.store.Dead(d.Job.ID, d.LeaseID, msg, now); err != nil {
			q.logf("job=%s attempts=%d/%d basis=retryable-error decision=dead-letter failed: %v",
				d.Job.ID, d.Attempt, d.Job.MaxAttempts, err)
			return
		}
		q.logf("job=%s attempts=%d/%d basis=retry-budget-exhausted decision=dead-letter reason=%q",
			d.Job.ID, d.Attempt, d.Job.MaxAttempts, msg)
		return
	}

	delay := q.cfg.Backoff.Delay(d.Attempt)
	avail := now.Add(delay)
	if err := q.store.Retry(d.Job.ID, d.LeaseID, reason, avail, now); err != nil {
		q.logf("job=%s attempts=%d/%d basis=retryable-error decision=retry failed: %v",
			d.Job.ID, d.Attempt, d.Job.MaxAttempts, err)
		return
	}
	q.logf("job=%s attempts=%d/%d basis=retryable-error decision=retry backoff=%s reason=%q",
		d.Job.ID, d.Attempt, d.Job.MaxAttempts, delay, reason)
}

// shuttingDown reports whether Start was called and consumers were canceled.
func (q *Queue) shuttingDown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ctx != nil && q.ctx.Err() != nil
}

func (q *Queue) track(jobID, leaseID string) {
	q.mu.Lock()
	q.inflight[jobID] = leaseID
	q.mu.Unlock()
}

func (q *Queue) untrack(jobID, leaseID string) {
	q.mu.Lock()
	if cur, ok := q.inflight[jobID]; ok && cur == leaseID {
		delete(q.inflight, jobID)
	}
	q.mu.Unlock()
}

// UpdateProgress persists progress for an active delivery. It returns
// ErrLeaseLost if the visibility window already expired.
func (q *Queue) UpdateProgress(d *Delivery, progress int) error {
	return q.store.UpdateProgress(d.Job.ID, d.LeaseID, progress, q.now())
}

// Enqueue persists a new pending job.
func (q *Queue) Enqueue(id string, payload EnqueuePayload) error {
	j := &Job{
		ID:          id,
		Payload:     payload,
		Status:      StatusPending,
		MaxAttempts: q.cfg.MaxAttempts,
	}
	return q.store.Enqueue(j)
}

// EnqueueJobs persists pre-built jobs (used in tests/demos).
func (q *Queue) EnqueueJobs(jobs ...*Job) error { return q.store.Enqueue(jobs...) }

// Get returns a persisted job snapshot.
func (q *Queue) Get(id string) (*Job, error) { return q.store.Get(id) }

// All returns all persisted job snapshots.
func (q *Queue) All() ([]*Job, error) { return q.store.All() }

// RequeueDead moves a dead-lettered job back to pending (operator action) and
// resets its attempt budget.
func (q *Queue) RequeueDead(id string) error {
	return q.store.RequeueDead(id, q.now())
}

// Shutdown stops consumers gracefully. In-flight leased jobs that have not
// reached a terminal outcome are released back to pending so they stay
// redeliverable after restart, then the store is closed.
func (q *Queue) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	q.closed = true
	started := q.started
	var cancel context.CancelFunc
	if started {
		cancel = q.cancel
	}
	q.mu.Unlock()

	if started {
		cancel()
		waitCh := make(chan struct{})
		go func() { q.wg.Wait(); close(waitCh) }()
		select {
		case <-waitCh:
		case <-ctx.Done():
			q.logf("graceful shutdown deadline exceeded; forcibly releasing in-flight leases")
		}
	}

	// Whatever remains in-flight (handler ignored cancellation) must not be
	// left stuck behind a stale lease: release it for redelivery.
	q.mu.Lock()
	leftovers := make([]struct{ id, lease string }, 0, len(q.inflight))
	for id, lease := range q.inflight {
		leftovers = append(leftovers, struct{ id, lease string }{id, lease})
	}
	q.mu.Unlock()
	for _, l := range leftovers {
		if err := q.store.Release(l.id, l.lease, q.now()); err != nil && err != ErrLeaseLost {
			q.logf("job=%s shutdown release error: %v", l.id, err)
		} else {
			q.logf("job=%s basis=shutdown decision=release-to-pending", l.id)
		}
	}
	return q.store.Close()
}
