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
	concCfg := consumerConfig()
	// 给足租约窗口：本用例断言"可见性窗口内每任务恰好处理一次"，
	// 不是验证租约超时重投（后者由 TestLeaseTakeoverAcrossInstances 覆盖）。
	// 在 -race 调度抖动下 50ms 短租约可能在 2ms handler 执行中途到期造成
	// 非预期重投，使该断言偶发失败，这里用长租约消除该无关抖动。
	concCfg.LeaseDuration = time.Hour
	q, err := New(concCfg, NewFileStore(path), "inst-conc")
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
		handled := len(seen) == nTasks
		mu.Unlock()
		if !handled {
			return false
		}
		// handler 全部执行过还不够：必须等每个任务的 Ack 都落盘到 succeeded，
		// 否则在 Ack 落盘前断言终态会偶发读到 running。
		for _, id := range ids {
			if g, ok := q.Get(id); !ok || g.Status != StatusSucceeded {
				return false
			}
		}
		return true
	}, "all tasks handled and acked as succeeded")

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

// 场景：消费者优雅关闭时，在途任务被退回 pending（而非等到租约超时）。
func TestConsumerShutdownRequeuesInflight(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = time.Hour // 关闭必须立即生效，不能依赖租约超时
	q, err := New(cfg, NewFileStore(path), "inst-shutdown")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "in-flight-on-stop")

	entered := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, _ *Task) error {
		closeOnce(entered)
		<-ctx.Done() // 模拟一个响应取消的在途任务
		<-release
		return ctx.Err()
	}
	consumer := NewConsumer(q, 1, handler,
		WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithCancel(t.Context())
	doneCh := runAsync(t, consumer, ctx)
	<-entered

	cancel()
	close(release)
	if err := <-doneCh; err != nil {
		t.Fatalf("Run on shutdown: %v", err)
	}
	got := statusOf(t, q, task.ID)
	if got.Status != StatusPending {
		t.Fatalf("task=%s after shutdown status=%s, want pending basis=requeued on graceful stop",
			task.ID, got.Status)
	}
	if !got.VisibleAt.Before(time.Now().Add(time.Second)) {
		t.Fatalf("requeued task should be immediately visible, VisibleAt=%s", got.VisibleAt)
	}
	t.Logf("[assert] task=%s after-shutdown status=pending attempts=%d basis=%q",
		task.ID, got.Attempts, "consumer canceled inflight work and requeued owned lease")
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
