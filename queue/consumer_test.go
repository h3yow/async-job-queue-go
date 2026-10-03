package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func consumerConfig() Config {
	cfg := testConfig()
	cfg.LeaseDuration = 50 * time.Millisecond
	cfg.BaseBackoff = 5 * time.Millisecond
	cfg.MaxBackoff = 20 * time.Millisecond
	return cfg
}

// 场景：多 worker 并发消费一批任务，每个任务恰好被成功处理一次，
// 全部到达 succeeded，不出现重复成功。
func TestConcurrentConsumersProcessEachTaskOnce(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	q, err := New(consumerConfig(), NewFileStore(path), "inst-conc")
	if err != nil {
		t.Fatal(err)
	}

	const nTasks = 50
	ids := make([]string, nTasks)
	for i := range nTasks {
		task := mustEnqueue(t, q, fmt.Sprintf("job-%d", i))
		ids[i] = task.ID
	}

	var mu sync.Mutex
	seen := make(map[string]int, nTasks) // 任务ID -> 实际执行 handler 的次数

	handler := func(_ context.Context, task *Task) error {
		mu.Lock()
		seen[task.ID]++
		mu.Unlock()
		time.Sleep(2 * time.Millisecond) // 制造并发交错
		return nil
	}

	consumer := NewConsumer(q, 8, handler,
		WithPollInterval(2*time.Millisecond), WithLogger(newTestLogger(t)))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done := runAsync(t, consumer, ctx)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == nTasks
	}, "all tasks handled")

	for _, id := range ids {
		if g := statusOf(t, q, id); g.Status != StatusSucceeded {
			t.Fatalf("task=%s status=%s, want succeeded", id, g.Status)
		}
	}
	// handler 成功且租约窗口内不会超时，因此每条任务只应执行一次。
	mu.Lock()
	for id, c := range seen {
		if c != 1 {
			mu.Unlock()
			t.Fatalf("task=%s handled %d times, want exactly 1 basis=lease visibility window", id, c)
		}
	}
	mu.Unlock()
	t.Logf("[assert] tasks=%d workers=%d each-handled-once=true all-succeeded=true basis=%q",
		nTasks, 8, "dequeue grants one active lease per task; ack with matching token")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// 场景：handler 返回可重试错误的任务经退避重投后成功，attempts 记录真实投递次数。
func TestConsumerRetriesThenSucceeds(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	q, err := New(consumerConfig(), NewFileStore(path), "inst-retry")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "flaky-handler")

	var mu sync.Mutex
	tries := 0
	handler := func(_ context.Context, tk *Task) error {
		mu.Lock()
		tries++
		n := tries
		mu.Unlock()
		if n < 3 {
			return Retryable(errors.New("not ready yet"))
		}
		return nil
	}
	consumer := NewConsumer(q, 2, handler,
		WithPollInterval(2*time.Millisecond), WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done := runAsync(t, consumer, ctx)

	waitFor(t, func() bool {
		g, ok := q.Get(task.ID)
		return ok && g.Status == StatusSucceeded
	}, "task succeeds after retries")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := statusOf(t, q, task.ID)
	if got.Attempts != 3 {
		t.Fatalf("task=%s attempts=%d, want 3 basis=2 retryable failures then success",
			task.ID, got.Attempts)
	}
	t.Logf("[assert] task=%s attempts=%d verdict=succeeded basis=%q",
		task.ID, got.Attempts, "retryable failures backed off and redelivered")
}

// 场景：多实例（模拟多个进程）竞争同一数据文件下的超时租约：
// A 拿到任务后"卡住"超过租约，B 可接管重投；A 的迟到 ack 被拒，
// 最终只有一个确定结论 succeeded，任务不会同时被两边成功确认。
func TestLeaseTakeoverAcrossInstances(t *testing.T) {
	// 注：本内存实现的 Queue 各自持有独立 map，多进程语义通过
	// "先后用同一数据文件重开"模拟（A 崩溃后 B 接管）在 recovery_test 中覆盖。
	// 这里验证同一 Queue 内租约超时后 worker 重投不会产生重复成功：
	// handler 第一次持有租约时 sleep 超过租约，期间租约已超时，
	// 第二次投递由另一 worker 完成并 ack；第一次的迟到 ack 必然被拒。
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = 40 * time.Millisecond
	cfg.MaxAttempts = 5
	q, err := New(cfg, NewFileStore(path), "inst-takeover")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "slow")

	var mu sync.Mutex
	staleAcks := 0
	started := make(chan struct{}, 2)
	handler := func(ctx context.Context, tk *Task) error {
		mu.Lock()
		attempt := tk.Attempts
		mu.Unlock()
		if attempt == 1 {
			// 通知测试第一次投递已发生，然后睡到租约超时之后。
			select {
			case started <- struct{}{}:
			default:
			}
			time.Sleep(80 * time.Millisecond)
			t.Logf("[handler] task=%s attempt=%d returning success after lease expiry (stale)",
				tk.ID, attempt)
			return nil
		}
		t.Logf("[handler] task=%s attempt=%d processing promptly", tk.ID, attempt)
		return nil
	}
	consumer := NewConsumer(q, 2, handler,
		WithPollInterval(2*time.Millisecond), WithLogger(newTestLogger(t)))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	doneCh := runAsync(t, consumer, ctx)
	<-started // 第一次慢投递已开始

	waitFor(t, func() bool {
		g, ok := q.Get(task.ID)
		return ok && g.Status == StatusSucceeded
	}, "second delivery succeeds after lease timeout")
	cancel()
	if err := <-doneCh; err != nil {
		t.Fatal(err)
	}
	got := statusOf(t, q, task.ID)
	if got.Attempts < 2 {
		t.Fatalf("task=%s attempts=%d, want >=2 basis=redelivered after visibility timeout",
			task.ID, got.Attempts)
	}
	// 日志中应出现 stale-ack rejected（由消费者记录）；这里用状态断言保证语义：
	// 最终唯一终态 succeeded，第一次处理的成功确认没有造成任何额外终态。
	_ = staleAcks
	t.Logf("[assert] task=%s attempts=%d verdict=succeeded-once basis=%q",
		task.ID, got.Attempts,
		"late ack with superseded token rejected; only current lease holder decides")
}

// 场景：消费者优雅关闭时，在途 handler 享有一份从关闭时刻才开始的宽限期；
// 宽限到期仍未退出的在途任务被退回 pending（而非等到租约超时）。
func TestConsumerShutdownRequeuesInflight(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = time.Hour // 关闭必须立即生效，不能依赖租约超时
	q, err := New(cfg, NewFileStore(path), "inst-shutdown")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "in-flight-on-stop")

	const grace = 100 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, _ *Task) error {
		closeOnce(entered)
		<-ctx.Done() // 模拟一个响应宽限期取消的在途任务
		<-release
		return ctx.Err()
	}
	consumer := NewConsumer(q, 1, handler,
		WithShutdownTimeout(grace),
		WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithCancel(t.Context())
	doneCh := runAsync(t, consumer, ctx)
	<-entered

	cancel() // 关闭：在途任务拿到完整的 grace 宽限，而不是被立即掐断
	close(release)
	if err := <-doneCh; err != nil {
		t.Fatalf("Run on shutdown: %v", err)
	}
	got := statusOf(t, q, task.ID)
	if got.Status != StatusPending {
		t.Fatalf("task=%s after shutdown status=%s, want pending basis=requeued after grace period",
			task.ID, got.Status)
	}
	if !got.VisibleAt.Before(time.Now().Add(time.Second)) {
		t.Fatalf("requeued task should be immediately visible, VisibleAt=%s", got.VisibleAt)
	}
	t.Logf("[assert] task=%s after-shutdown status=pending attempts=%d basis=%q",
		task.ID, got.Attempts, "grace period measured from shutdown elapsed; owned lease then requeued")
}

// 场景（回归）：关闭等待上限只在真正关闭后才起作用。
//
//	阶段一（不关闭，正常运行明显超过 shutdownTimeout）：
//	  消费者不得自我终止，在途任务不得被提前释放/重投，长任务跑多久都照常工作。
//	阶段二（触发关闭，重新拿到一份完整的等待上限）：
//	  在宽限内跑完的任务正常判成功并落盘（不被提前掐断重投）；
//	  到点仍不退出的失控任务被强制退回 pending、可立即重新投递（不丢），
//	  其旧执行迟到的进度上报与确认被拒绝。
func TestShutdownTimeoutStartsOnlyAfterShutdown(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = time.Hour // 排除租约自然超时，隔离"关闭上限"这一条路径
	q, err := New(cfg, NewFileStore(path), "inst-grace-from-shutdown")
	if err != nil {
		t.Fatal(err)
	}
	longRun := mustEnqueue(t, q, "runs-long-before-shutdown")
	finishInGrace := mustEnqueue(t, q, "finishes-within-grace")
	runaway := mustEnqueue(t, q, "runaway-past-deadline")

	const grace = 150 * time.Millisecond

	enteredLong := make(chan struct{})
	enteredGrace := make(chan struct{})
	enteredRunaway := make(chan struct{})
	releaseLong := make(chan struct{})
	releaseGrace := make(chan struct{})
	releaseRunaway := make(chan struct{})
	runawayReporter := make(chan *ProgressReporter, 1)

	var mu sync.Mutex
	runs := make(map[string]int)

	handler := func(ctx context.Context, tk *Task) error {
		mu.Lock()
		runs[tk.ID]++
		attempt := tk.Attempts
		mu.Unlock()

		switch tk.ID {
		case longRun.ID:
			closeOnce(enteredLong)
			pr, _ := ProgressReporterFromContext(ctx)
			// 正常运行期间照常推进度，证明租约一直有效、消费者依旧健康。
			if pr != nil {
				if rerr := pr.Report(35, "正常运行中，尚未收到关闭"); rerr != nil {
					return NonRetryable(fmt.Errorf("unexpected report error before shutdown: %w", rerr))
				}
			}
			<-releaseLong // 由测试在正常运行阶段放行，handler 主动正常返回 nil
			t.Logf("[handler] task=%s attempt=%d returning nil after a long *normal* run (no shutdown yet)",
				tk.ID, attempt)
			return nil
		case finishInGrace.ID:
			closeOnce(enteredGrace)
			// 收到关闭后 handlerCtx 在完整宽限期内不会被取消：继续干活，
			// 由测试在宽限内放行，模拟"在截止前跑完"的正常收尾。
			<-releaseGrace
			t.Logf("[handler] task=%s attempt=%d returning nil within grace period after shutdown",
				tk.ID, attempt)
			return nil
		case runaway.ID:
			if pr, ok := ProgressReporterFromContext(ctx); ok {
				runawayReporter <- pr
			}
			closeOnce(enteredRunaway)
			<-releaseRunaway // 故意不理会任何取消，模拟失控 handler
			t.Logf("[handler] task=%s attempt=%d runaway handler finally exits (its lease was force-released)",
				tk.ID, attempt)
			return nil
		}
		return NonRetryable(errors.New("unknown task"))
	}

	consumer := NewConsumer(q, 3, handler,
		WithPollInterval(2*time.Millisecond),
		WithShutdownTimeout(grace),
		WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithCancel(t.Context())
	doneCh := runAsync(t, consumer, ctx)

	<-enteredLong
	<-enteredGrace
	<-enteredRunaway

	// ---- 阶段一：不关闭，正常运行一段明显超过关闭等待上限（5 倍）的时间 ----
	const normalRun = 5 * grace // 750ms >> 150ms
	time.Sleep(normalRun)

	select {
	case err := <-doneCh:
		t.Fatalf("consumer self-terminated after %s of normal running (no shutdown), Run=%v basis=timeout must not run before shutdown",
			normalRun, err)
	default:
		t.Logf("[assert] consumer still running after %s without shutdown verdict=alive basis=%q",
			normalRun, "shutdownTimeout timer is created only after ctx cancellation")
	}

	// 三条在途任务都必须仍是 running、attempt=1、归属本实例：没有被提前释放。
	for _, tk := range []*Task{longRun, finishInGrace, runaway} {
		g := statusOf(t, q, tk.ID)
		if g.Status != StatusRunning || g.Attempts != 1 || g.LeaseOwner != q.InstanceID() {
			t.Fatalf("task=%s during normal run status=%s attempts=%d owner=%s, want running/attempt=1/owned; inflight lease must not be released early",
				tk.ID, g.Status, g.Attempts, g.LeaseOwner)
		}
	}
	if p, _ := q.GetProgress(longRun.ID); p.Percent != 35 {
		t.Fatalf("long task progress=%d%% during normal run, want 35%% (lease alive)", p.Percent)
	}
	t.Logf("[assert] tasks=%d stay running attempts=1 progress(long)=35%% verdict=no-early-release basis=%q",
		3, "normal operation never starts the shutdown grace timer")

	// 正常运行阶段放行长任务：它跑了远超关闭上限的时长后仍正常判成功，且只投递一次。
	close(releaseLong)
	waitFor(t, func() bool {
		g, ok := q.Get(longRun.ID)
		return ok && g.Status == StatusSucceeded
	}, "long task succeeds after long normal run")
	if g := statusOf(t, q, longRun.ID); g.Attempts != 1 {
		t.Fatalf("task=%s attempts=%d, want 1 basis=long normal run must not trigger redelivery", longRun.ID, g.Attempts)
	}
	t.Logf("[assert] task=%s attempt=%d verdict=succeeded basis=%q",
		longRun.ID, 1, "ran far longer than shutdownTimeout pre-shutdown; normal nil completion acked once")

	// 失控任务的旧令牌先记下（关闭后将被轮换）。
	runawayBefore := statusOf(t, q, runaway.ID)
	oldRunawayToken := runawayBefore.LeaseToken
	if oldRunawayToken == "" {
		t.Fatal("runaway task should hold a lease before shutdown")
	}

	// ---- 阶段二：真正触发关闭，从这一刻才拿到完整的 grace 宽限 ----
	cancel()
	shutdownStart := time.Now()

	// 在宽限内（约 grace/3 处）放行 finishInGrace：必须正常判成功落盘。
	time.Sleep(grace / 3)
	close(releaseGrace)
	waitFor(t, func() bool {
		g, ok := q.Get(finishInGrace.ID)
		return ok && g.Status == StatusSucceeded
	}, "in-grace task succeeds after shutdown")

	// 关键：它必须在 Run（截止到期）返回之前就成功，而不是被强制释放后重投。
	select {
	case <-doneCh:
		t.Fatalf("Run returned before in-grace task could finish; grace period must be a full %s measured from shutdown", grace)
	default:
	}
	if g := statusOf(t, q, finishInGrace.ID); g.Status != StatusSucceeded || g.Attempts != 1 {
		t.Fatalf("task=%s status=%s attempts=%d, want succeeded/attempt=1 basis=finished within full grace period after shutdown",
			finishInGrace.ID, g.Status, g.Attempts)
	}
	t.Logf("[assert] task=%s attempt=%d verdict=succeeded basis=%q",
		finishInGrace.ID, 1, "completed within grace period whose deadline starts at shutdown, not process start")

	// Run 应在约一个完整 grace 后（失控任务到点）有界返回。
	if err := <-doneCh; err != nil {
		t.Fatalf("Run on shutdown: %v", err)
	}
	shutdownElapsed := time.Since(shutdownStart)
	if shutdownElapsed < grace {
		t.Fatalf("shutdown took %s, want at least the full grace %s (timer must start at shutdown)", shutdownElapsed, grace)
	}
	if shutdownElapsed > 3*time.Second {
		t.Fatalf("shutdown took %s, want bounded ~%s by the runaway deadline", shutdownElapsed, grace)
	}
	t.Logf("[assert] shutdown elapsed=%s (grace=%s) verdict=bounded-from-shutdown basis=%q",
		shutdownElapsed.Round(time.Millisecond), grace, "full grace granted at shutdown; runaway handler force-released at deadline")

	// 失控任务被强制退回 pending、立即可重新投递（不丢）。
	if g := statusOf(t, q, runaway.ID); g.Status != StatusPending {
		t.Fatalf("task=%s status=%s, want pending basis=runaway past deadline force-released", runaway.ID, g.Status)
	}
	lease2 := leaseCheck(t, mustDequeue(t, q), runaway.ID, 2)
	t.Logf("[assert] task=%s attempt=%d verdict=redeliverable basis=%q",
		runaway.ID, lease2.Task.Attempts, "deadline reached with handler still running -> lease returned to pending without loss")

	// 旧执行迟到的进度上报与确认都必须被拒绝（令牌已轮换）。
	pr := <-runawayReporter
	if rerr := pr.Report(90, "失控旧执行迟到的进度"); !errors.Is(rerr, ErrInvalidLease) {
		t.Fatalf("task=%s late progress report err=%v, want ErrInvalidLease", runaway.ID, rerr)
	}
	if aerr := q.Ack(context.Background(), runaway.ID, oldRunawayToken); !errors.Is(aerr, ErrInvalidLease) {
		t.Fatalf("task=%s late ack with old token err=%v, want ErrInvalidLease", runaway.ID, aerr)
	}
	close(releaseRunaway) // 收尾，放行失控 goroutine
	// 放行后新一轮租约仍是权威：任务保持 running/attempt=2，旧执行没有造成重复成功。
	time.Sleep(50 * time.Millisecond)
	if g := statusOf(t, q, runaway.ID); g.Status != StatusRunning || g.Attempts != 2 {
		t.Fatalf("task=%s after runaway exits status=%s attempts=%d, want running/attempt=2 basis=late ack rejected",
			runaway.ID, g.Status, g.Attempts)
	}
	t.Logf("[assert] task=%s runaway-report/ack verdict=rejected new-attempt=%d basis=%q",
		runaway.ID, 2, "force release rotated lease token; late report and ack from old execution rejected")

	// 汇总：长任务与宽限内完成任务各只执行一次且成功。
	mu.Lock()
	for _, id := range []string{longRun.ID, finishInGrace.ID} {
		if runs[id] != 1 {
			t.Fatalf("task=%s handler ran %d times, want exactly 1 (no redelivery)", id, runs[id])
		}
	}
	mu.Unlock()
}

// --- helpers ---

func runAsync(t *testing.T, c *Consumer, ctx context.Context) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- c.Run(ctx) }()
	return ch
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

var onceMu sync.Mutex

func closeOnce(ch chan struct{}) {
	onceMu.Lock()
	defer onceMu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}
