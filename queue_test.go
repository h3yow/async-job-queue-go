package jobqueue

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testLogger() *log.Logger {
	return log.New(os.Stdout, "TEST jobqueue: ", log.LstdFlags|log.Lmicroseconds)
}

func newTestQueue(t *testing.T, store Store, cfg Config) *Queue {
	t.Helper()
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 5 * time.Second
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Millisecond
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExponentialBackoff{BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}
	}
	cfg.ConsumerID = "test-" + t.Name()
	cfg.Logger = testLogger()
	return New(store, cfg)
}

func tempStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queue.json")
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// 已确认完成的任务必须持久化，重启后不丢、不再投递。
func TestAckPersistsAcrossRestart(t *testing.T) {
	path := tempStorePath(t)
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	q := newTestQueue(t, store, Config{Handler: func(ctx context.Context, d *Delivery) error {
		if err := d.UpdateProgress(100); err != nil {
			t.Errorf("progress: %v", err)
		}
		return nil
	}})
	if err := q.Enqueue("job-ack", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	ok, err := q.RunOne(context.Background())
	if err != nil || !ok {
		t.Fatalf("RunOne ok=%v err=%v", ok, err)
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Simulate a process restart by reopening the same file.
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	q2 := newTestQueue(t, store2, Config{})
	j, err := q2.Get("job-ack")
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if j.Status != StatusSucceeded || j.Progress != 100 {
		t.Fatalf("after restart status=%s progress=%d, want succeeded/100", j.Status, j.Progress)
	}
	if ok, err := q2.RunOne(context.Background()); err != nil || ok {
		t.Fatalf("succeeded job redelivered after restart: ok=%v err=%v", ok, err)
	}
	q2.Shutdown(context.Background())
}

// 可重试失败先退避重试，最终成功，attempts 反映真实投递次数。
func TestRetryBackoffThenSucceed(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var mu sync.Mutex
	fails := 0
	q := newTestQueue(t, store, Config{
		MaxAttempts: 4,
		Backoff:     ExponentialBackoff{BaseDelay: 2 * time.Millisecond, MaxDelay: 10 * time.Millisecond},
		Handler: func(ctx context.Context, d *Delivery) error {
			t.Logf("job=%s delivery attempt=%d progress-before=%d", d.Job.ID, d.Attempt, d.Job.Progress)
			_ = d.UpdateProgress(50)
			mu.Lock()
			defer mu.Unlock()
			if fails < 2 {
				fails++
				return RetryError("transient downstream timeout")
			}
			return nil
		},
	})
	if err := q.Enqueue("job-retry", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	q.Start()
	waitFor(t, 2*time.Second, func() bool {
		j, _ := q.Get("job-retry")
		return j != nil && j.Status == StatusSucceeded
	})
	j, _ := q.Get("job-retry")
	if j.Attempts != 3 {
		t.Fatalf("attempts=%d, want 3", j.Attempts)
	}
	if j.Progress != 100 {
		t.Fatalf("progress=%d, want 100", j.Progress)
	}
	q.Shutdown(context.Background())
}

func TestExponentialBackoff(t *testing.T) {
	b := ExponentialBackoff{BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 100 * time.Millisecond}, // capped
		{0, 10 * time.Millisecond},
	}
	for _, c := range cases {
		if got := b.Delay(c.attempt); got != c.want {
			t.Errorf("Delay(%d)=%s, want %s", c.attempt, got, c.want)
		}
	}
}

// 重试次数耗尽后必须进入死信，保留原因，且不会再被普通消费者取出。
func TestRetryExhaustedEntersDeadLetter(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var calls int
	var mu sync.Mutex
	q := newTestQueue(t, store, Config{
		MaxAttempts: 2,
		Backoff:     ExponentialBackoff{BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Handler: func(ctx context.Context, d *Delivery) error {
			mu.Lock()
			calls++
			mu.Unlock()
			t.Logf("job=%s attempt=%d failing with retryable error", d.Job.ID, d.Attempt)
			return RetryError("http 500 from dependency")
		},
	})
	if err := q.Enqueue("job-dlq-retry", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	q.Start()
	waitFor(t, 2*time.Second, func() bool {
		j, _ := q.Get("job-dlq-retry")
		return j != nil && j.Status == StatusDead
	})
	time.Sleep(50 * time.Millisecond) // prove it is never redelivered again
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 2 {
		t.Fatalf("handler calls=%d, want exactly 2", gotCalls)
	}
	j, _ := q.Get("job-dlq-retry")
	if j.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", j.Attempts)
	}
	if j.DeadReason == "" || !contains(j.DeadReason, "http 500 from dependency") {
		t.Fatalf("dead reason not traceable: %q", j.DeadReason)
	}
	if !contains(j.DeadReason, "retries exhausted") {
		t.Fatalf("dead reason missing exhaustion marker: %q", j.DeadReason)
	}
	// Direct claim must skip the dead job as well.
	if _, _, err := store.Claim("probe", time.Second, time.Now().UTC()); err != ErrNotFound {
		t.Fatalf("dead job claimed: err=%v", err)
	}
	q.Shutdown(context.Background())
}

// 不可恢复错误（terminal）第一次投递即入死信，不消耗重试预算。
func TestTerminalFailureDeadLettersImmediately(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var calls int
	var mu sync.Mutex
	q := newTestQueue(t, store, Config{
		MaxAttempts: 5,
		Handler: func(ctx context.Context, d *Delivery) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return TerminalError("invalid payload: missing field user_id")
		},
	})
	if err := q.Enqueue("job-dlq-terminal", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	q.Start()
	waitFor(t, 2*time.Second, func() bool {
		j, _ := q.Get("job-dlq-terminal")
		return j != nil && j.Status == StatusDead
	})
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("handler calls=%d, want 1 (terminal must not retry)", gotCalls)
	}
	j, _ := q.Get("job-dlq-terminal")
	if j.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", j.Attempts)
	}
	if !contains(j.DeadReason, "terminal failure") || !contains(j.DeadReason, "missing field user_id") {
		t.Fatalf("dead reason not traceable: %q", j.DeadReason)
	}
	q.Shutdown(context.Background())
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// 租约（可见性窗口）超时后任务必须可被重新投递；晚到的 ack 必须失效，
// 最终任务只被一个消费者确认一次 —— 即“至少一次投递，最终状态唯一”。
func TestLeaseExpiryRedeliveryAndStaleAckIgnored(t *testing.T) {
	path := tempStorePath(t)
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var mu sync.Mutex
	var deliveryOwners []string
	var successes int

	// Consumer A holds the lease past its visibility window, then tries to
	// ack late. Consumer B gets the redelivery and completes the job.
	leaseTTL := 60 * time.Millisecond
	q := newTestQueue(t, store, Config{
		Workers:     1,
		LeaseTTL:    leaseTTL,
		MaxAttempts: 5,
		Handler: func(ctx context.Context, d *Delivery) error {
			mu.Lock()
			owner := d.Job.LeaseOwner
			attempt := d.Attempt
			deliveryOwners = append(deliveryOwners, owner)
			mu.Unlock()
			t.Logf("job=%s attempt=%d owner=%s handling", d.Job.ID, attempt, owner)
			if attempt == 1 {
				// Simulate a stalled worker: sleep beyond the visibility window.
				time.Sleep(5 * leaseTTL / 2)
				if ctx.Err() != nil {
					t.Logf("job=%s attempt=%d handler canceled before late ack", d.Job.ID, attempt)
					return RetryError("canceled during shutdown-like stall")
				}
				t.Logf("job=%s attempt=%d sending stale ack after lease expiry", d.Job.ID, attempt)
				return nil // late ack must be ignored
			}
			t.Logf("job=%s attempt=%d owner=%s completing", d.Job.ID, attempt, owner)
			mu.Lock()
			successes++
			mu.Unlock()
			return nil
		},
	})
	// Drive synchronously to keep the timing deterministic: first RunOne
	// stalls; second RunOne (after TTL) claims the expired lease.
	if err := q.Enqueue("job-dup", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	firstDone := make(chan struct{})
	go func() { _, _ = q.RunOne(context.Background()); close(firstDone) }()
	waitFor(t, time.Second, func() bool {
		j, _ := q.Get("job-dup")
		return j != nil && j.Status == StatusLeased && time.Now().UTC().After(j.LeaseExpiresAt)
	})
	ok2, err2 := q.RunOne(context.Background())
	if err2 != nil || !ok2 {
		t.Fatalf("redelivery claim ok=%v err=%v", ok2, err2)
	}
	<-firstDone
	waitFor(t, time.Second, func() bool {
		j, _ := q.Get("job-dup")
		return j != nil && j.Status == StatusSucceeded
	})
	j, _ := q.Get("job-dup")
	if j.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", j.Attempts)
	}
	mu.Lock()
	if successes != 1 {
		t.Fatalf("successful completions=%d, want exactly 1", successes)
	}
	mu.Unlock()
	q.Shutdown(context.Background())
}

// 多消费者并发：多个任务应分布到多个 worker 并行处理，而在可见性窗口内
// 同一个任务绝不能同时被两个消费者处理。
func TestConcurrentConsumersExclusiveDelivery(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const nJobs = 20
	var mu sync.Mutex
	active := 0
	maxActive := 0
	successByJob := map[string]int{}
	maxConcurrent := make(chan struct{}, 1)

	q := newTestQueue(t, store, Config{
		Workers:     4,
		LeaseTTL:    5 * time.Second,
		MaxAttempts: 3,
		Handler: func(ctx context.Context, d *Delivery) error {
			mu.Lock()
			if _, busy := successByJob[d.Job.ID]; busy && successByJob[d.Job.ID] > 0 {
				mu.Unlock()
				t.Fatalf("job=%s delivered again after success (attempt=%d)", d.Job.ID, d.Attempt)
			}
			active++
			if active > maxActive {
				maxActive = active
			}
			select {
			case maxConcurrent <- struct{}{}:
			default:
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond) // hold the lease long enough to race
			mu.Lock()
			active--
			successByJob[d.Job.ID]++
			mu.Unlock()
			return nil
		},
	})
	for i := 0; i < nJobs; i++ {
		id := "job-conc-" + itoa(i)
		if err := q.Enqueue(id, nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	q.Start()
	waitFor(t, 3*time.Second, func() bool {
		jobs, _ := q.All()
		n := 0
		for _, j := range jobs {
			if j.Status == StatusSucceeded {
				n++
			}
		}
		return n == nJobs
	})
	q.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(successByJob) != nJobs {
		t.Fatalf("succeeded jobs=%d, want %d", len(successByJob), nJobs)
	}
	for id, n := range successByJob {
		if n != 1 {
			t.Fatalf("job=%s handled %d times, want exactly 1", id, n)
		}
	}
	if maxActive < 2 {
		t.Fatalf("max concurrent handlers=%d, want >= 2 (consumers did not run in parallel)", maxActive)
	}
	select {
	case <-maxConcurrent:
	default:
		t.Fatal("never observed a concurrent handler")
	}
	t.Logf("conclusion: %d jobs each handled exactly once; max concurrent handlers=%d", nJobs, maxActive)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// 模拟进程崩溃：不调用 Shutdown，直接丢弃队列并重新打开同一个存储文件。
// - 已 ack 的任务保持 succeeded，不再投递；
// - 崩溃时仍 leased 的任务必须恢复为 pending 并被新进程成功处理；
// - 死信保持死信。
func TestCrashRecoveryRedeliversUnfinished(t *testing.T) {
	path := tempStorePath(t)

	// --- "process 1": crashes mid-processing ---
	store1, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	q1 := newTestQueue(t, store1, Config{ConsumerID: "process-1", MaxAttempts: 3,
		Handler: func(ctx context.Context, d *Delivery) error { return nil }})
	// Enqueue in a controlled order matching claim order (sorted by
	// creation time, then id): done -> mid -> pending -> dead.
	jobs := []*Job{
		{ID: "job-1-done", Status: StatusPending, MaxAttempts: 3},
		{ID: "job-2-mid", Status: StatusPending, MaxAttempts: 3},
		{ID: "job-3-pending", Status: StatusPending, MaxAttempts: 3},
		{ID: "job-4-dead", Status: StatusPending, MaxAttempts: 1},
	}
	for _, jb := range jobs {
		if err := q1.EnqueueJobs(jb); err != nil {
			t.Fatalf("enqueue %s: %v", jb.ID, err)
		}
		time.Sleep(2 * time.Millisecond) // distinct CreatedAt => deterministic claim order
	}
	// 1) Ack job-1-done fully.
	if _, err := q1.RunOne(context.Background()); err != nil {
		t.Fatalf("run done: %v", err)
	}
	// 2) Lease job-2-mid and never ack (simulating a crash inside the handler),
	// after recording durable progress.
	d, _, err := store1.Claim("process-1", time.Minute, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim mid: %v", err)
	}
	if d.Job.ID != "job-2-mid" {
		t.Fatalf("unexpected claim order: %s", d.Job.ID)
	}
	if err := store1.UpdateProgress(d.Job.ID, d.LeaseID, 42, time.Now().UTC()); err != nil {
		t.Fatalf("progress: %v", err)
	}
	// 3) Put job-4-dead into the dead set directly (dead-letter semantics are
	// covered by dedicated tests); job-3-pending is left pending.
	if err := store1.Dead("job-4-dead", "", "preexisting dead letter", time.Now().UTC()); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	// "Crash": drop the process without Shutdown, leaving the lease on disk.
	_ = store1.Close()

	// --- "process 2": reopen and recover ---
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	q2 := newTestQueue(t, store2, Config{ConsumerID: "process-2", MaxAttempts: 3,
		Handler: func(ctx context.Context, d *Delivery) error {
			t.Logf("job=%s recovered delivery attempt=%d prior-progress=%d", d.Job.ID, d.Attempt, d.Job.Progress)
			return nil
		}})
	j, err := q2.Get("job-2-mid")
	if err != nil {
		t.Fatalf("get mid: %v", err)
	}
	if j.Status != StatusPending {
		t.Fatalf("crashed leased job status=%s, want pending after recovery", j.Status)
	}
	q2.Start()
	waitFor(t, 2*time.Second, func() bool {
		jobs, _ := q2.All()
		okPending, okMid := false, false
		for _, jj := range jobs {
			if jj.ID == "job-3-pending" && jj.Status == StatusSucceeded {
				okPending = true
			}
			if jj.ID == "job-2-mid" && jj.Status == StatusSucceeded {
				okMid = true
			}
		}
		return okPending && okMid
	})
	j, _ = q2.Get("job-1-done")
	if j.Status != StatusSucceeded {
		t.Fatalf("acked job status after restart=%s, want succeeded", j.Status)
	}
	j, _ = q2.Get("job-2-mid")
	if j.Attempts != 2 { // 1 crashed attempt + 1 recovery attempt
		t.Fatalf("job-2-mid attempts=%d, want 2 (crashed delivery must count)", j.Attempts)
	}
	if !contains(j.LastError, "stale lease recovered") {
		t.Fatalf("recovery reason not recorded: %q", j.LastError)
	}
	j, _ = q2.Get("job-4-dead")
	if j.Status != StatusDead {
		t.Fatalf("dead job status after restart=%s, want dead", j.Status)
	}
	q2.Shutdown(context.Background())
}

// 优雅关闭：处理中（handler 尊重 ctx）的任务应回到 pending 并保持可投递。
func TestGracefulShutdownReleasesInflight(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	entered := make(chan struct{})
	q := newTestQueue(t, store, Config{
		Workers:     1,
		LeaseTTL:    time.Minute,
		MaxAttempts: 3,
		Handler: func(ctx context.Context, d *Delivery) error {
			close(entered)
			<-ctx.Done() // honor shutdown; do not finish
			t.Logf("job=%s attempt=%d observed shutdown, leaving unfinished", d.Job.ID, d.Attempt)
			return ctx.Err()
		},
	})
	if err := q.Enqueue("job-shutdown", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	q.Start()
	<-entered
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := q.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// Reopening the file must reveal a redeliverable pending job.
	path := store.path
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	j2, err := store2.Get("job-shutdown")
	if err != nil {
		t.Fatalf("get after shutdown: %v", err)
	}
	if j2.Status != StatusPending {
		t.Fatalf("in-flight job after shutdown status=%s, want pending", j2.Status)
	}
	if !j2.AvailableAt.IsZero() {
		t.Fatalf("released job should be immediately available, got %v", j2.AvailableAt)
	}
	// A new consumer must be able to complete it.
	q2 := newTestQueue(t, store2, Config{ConsumerID: "post-shutdown",
		Handler: func(ctx context.Context, d *Delivery) error { return nil }})
	if ok, err := q2.RunOne(context.Background()); err != nil || !ok {
		t.Fatalf("redelivery after shutdown ok=%v err=%v", ok, err)
	}
	j3, _ := store2.Get("job-shutdown")
	if j3.Status != StatusSucceeded {
		t.Fatalf("status after redelivery=%s, want succeeded", j3.Status)
	}
	q2.Shutdown(context.Background())
}

// 保护阀：任务每次都在 ack/nack 之前“崩溃”（只能靠租约超时重投）时，
// 投递次数超过 MaxAttempts 后也必须进入死信，杜绝无限静默重投。
func TestLeaseTimeoutExhaustionDeadLetters(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	q := newTestQueue(t, store, Config{ConsumerID: "crasher", Workers: 0,
		LeaseTTL: 20 * time.Millisecond, MaxAttempts: 2})
	if err := q.Enqueue("job-crash-loop", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Two expired deliveries (claims with no ack). The third claim — made
	// through the queue, where the max-delivery rule lives — must dead-letter.
	for i := 1; i <= 2; i++ {
		d, reclaimed, err := store.Claim("crasher", 20*time.Millisecond, time.Now().UTC())
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if i == 1 && reclaimed {
			t.Fatalf("first claim must not be a reclamation")
		}
		if i > 1 && !reclaimed {
			t.Fatalf("claim %d must report reclaimed=true", i)
		}
		_ = d
		// Handler crashes before ack/nack; the visibility window passes.
		time.Sleep(25 * time.Millisecond)
	}
	ok, err := q.RunOne(context.Background())
	if err != nil || !ok {
		t.Fatalf("RunOne safety valve ok=%v err=%v", ok, err)
	}
	j, _ := q.Get("job-crash-loop")
	if j.Status != StatusDead {
		t.Fatalf("status=%s, want dead", j.Status)
	}
	if j.Attempts != 3 {
		t.Fatalf("attempts=%d, want 3 (2 stale + 1 safety-valve claim)", j.Attempts)
	}
	if !contains(j.DeadReason, "lease expired without ack") {
		t.Fatalf("dead reason=%q missing lease-timeout basis", j.DeadReason)
	}
	q.Shutdown(context.Background())
}

// 运维操作 RequeueDead：死信可被显式重放回 pending 并由修复后的消费者
// 重新处理成功；普通 Claim 路径永远不会自动取出死信。
func TestRequeueDeadIsExplicitOnly(t *testing.T) {
	store, err := OpenStore(tempStorePath(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	q := newTestQueue(t, store, Config{ConsumerID: "before-fix", MaxAttempts: 1,
		Handler: func(ctx context.Context, d *Delivery) error {
			return RetryError("buggy first version")
		}})
	if err := q.Enqueue("job-requeue", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if ok, err := q.RunOne(context.Background()); err != nil || !ok {
		t.Fatalf("run1 ok=%v err=%v", ok, err)
	}
	j, _ := q.Get("job-requeue")
	if j.Status != StatusDead {
		t.Fatalf("status=%s, want dead", j.Status)
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// "Restart" with the bug fixed; operator explicitly replays the dead job.
	store2, err := OpenStore(store.path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	q2 := newTestQueue(t, store2, Config{ConsumerID: "after-fix", MaxAttempts: 1,
		Handler: func(ctx context.Context, d *Delivery) error {
			t.Logf("job=%s replayed delivery attempts=%d prior-reason=%q", d.Job.ID, d.Attempt, d.Job.LastError)
			return nil
		}})
	if err := q2.RequeueDead("job-requeue"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	j, _ = q2.Get("job-requeue")
	if j.Status != StatusPending || j.Attempts != 0 || j.DeadReason != "" {
		t.Fatalf("after requeue status=%s attempts=%d dead=%q", j.Status, j.Attempts, j.DeadReason)
	}
	if ok, err := q2.RunOne(context.Background()); err != nil || !ok {
		t.Fatalf("run2 ok=%v err=%v", ok, err)
	}
	j, _ = q2.Get("job-requeue")
	if j.Status != StatusSucceeded {
		t.Fatalf("status=%s, want succeeded after replay", j.Status)
	}
	q2.Shutdown(context.Background())
}
